#!/usr/bin/perl -w

# Steps to follow on loading
# 1. Parse model
# 2. Match suitable files to load from the file set (i.e., the input directory)
# 3. Decide whether the matched files can constitute a sound subset
# 4. Group by domains
# 4.a. Correlate by inheritance, validate line by line and translate into JSON/BSON
# 4.b. Extract keys for second pass validations (i.e., relations)
# 5. Second pass validations
# 6. Load into MongoDB

use strict;

use Carp;
use Config::IniFiles;
use File::Basename;
use File::Path;
use File::Spec;

use FindBin;
use lib "$FindBin::Bin/lib";
use BP::Model;
use BP::Loader::CorrelatableConcept;
use BP::Loader::Mapper;
# These are included so they self-register on BP::Loader::Mapper
use BP::Loader::Mapper::Autoload::Relational;
use BP::Loader::Mapper::Autoload::MongoDB;
use BP::Loader::Mapper::Autoload::Elasticsearch;
use BP::Loader::Mapper::Autoload::DocumentationGenerator;

use Time::HiRes;

sub tiempo($$) {
	my($label,$prev)=@_;
	my $next = time();
	
	print STDERR 'DEBUG: ',$label,' ',$next-$prev,"\n";
	
	return $next;
}

if(scalar(@ARGV)>=2) {
	my $iniFile = shift(@ARGV);
	my $workingDir = shift(@ARGV);
	
	# First, let's read the configuration
	my $ini = Config::IniFiles->new(-file => $iniFile, -default => $BP::Loader::Mapper::DEFAULTSECTION);
	
	# And create the working directory
	File::Path::make_path($workingDir);
	
	# Let's parse the model
	my $modelFile = $ini->val($BP::Loader::Mapper::SECTION,'model');
	# Setting up the right path on relative cases
	$modelFile = File::Spec->catfile(File::Basename::dirname($iniFile),$modelFile)  unless(File::Spec->file_name_is_absolute($modelFile));
	
	print "Parsing model $modelFile...\n";
	my $model = undef;
	eval {
		$model = BP::Model->new($modelFile);
	};
	
	if($@) {
		Carp::croak('ERROR: Model parsing and validation failed. Reason: '.$@);
	}
	print "\tDONE!\n";
	
	# Setting up the file prefix
	my (undef,undef,undef,$day,$month,$year) = localtime();
	# Doing numerical adjustments
	$year += 1900;
	$month ++;
	my $thisdate = sprintf("%d%.2d%.2d",$year,$month,$day);
	
	my $relOutfilePrefix=join('-',$model->projectName,'data_model',$model->versionString,$thisdate);
	$ini->newval($BP::Loader::Mapper::SECTION,BP::Loader::Mapper::FILE_PREFIX_KEY,$relOutfilePrefix);
	
	my %storageModels = ();
	
	# Is there any file whose data has to be mapped?
	if(scalar(@ARGV)>0) {
		# Setting up the loader storage model(s)
		Carp::croak('ERROR: undefined destination storage model')  unless($ini->exists($BP::Loader::Mapper::SECTION,'loaders'));
		my $loadModelNames = $ini->val($BP::Loader::Mapper::SECTION,'loaders');
		
		my @loadModels = ();
		foreach my $loadModelName (split(/,/,$loadModelNames)) {
			unless(exists($storageModels{$loadModelName})) {
				$storageModels{$loadModelName} = BP::Loader::Mapper->newInstance($loadModelName,$model,$ini);
				push(@loadModels,$loadModelName);
			}
		}
		
		# Now, do we need to push the metadata there?
		if(!$ini->exists($BP::Loader::Mapper::SECTION,'metadata-loaders') || $ini->val($BP::Loader::Mapper::SECTION,'metadata-loaders') eq 'true') {
			foreach my $mapper (@storageModels{@loadModels}) {
				$mapper->storeNativeModel();
			}
		}
		
		# Let's get the associated concepts
		my %conceptMatch = ();
		my @conceptMatchArray = ();
		foreach my $filename (@ARGV) {
			my $p_matches = $model->matchConceptsFromFilename($filename);
			if(defined($p_matches)) {
				if(scalar(@{$p_matches}) > 1) {
					Carp::croak('ERROR: Filename '.$filename.' matched more than one filename pattern. Please reconsider change the filename patterns\n')
				} else {
					# Saving the correspondences for later
					my $match = $p_matches->[0];
					my $conceptKey = $match->[0] + 0;
					unless(exists($conceptMatch{$conceptKey})) {
						# The concept and the filenames which matched
						my $conceptMatchValue = [$match->[0],[]];
						$conceptMatch{$conceptKey} = $conceptMatchValue;
						push(@conceptMatchArray,$conceptMatchValue);
					}
					push(@{$conceptMatch{$conceptKey}->[1]}, [$filename,$match]);
				}
			} else {
				print STDERR "WARNING: Unable to identify corresponding concept for $filename. Discarding...\n";
			}
		}
		
		if(scalar(keys(%conceptMatch)) == 0) {
			print STDERR "ERROR: No input filename matched against declared concepts. Exiting...\n";
			exit 1;
		}
		
		foreach my $mapper (@storageModels{@loadModels}) {
			# Now, let's create the possibly correlatable concepts
			# and check which of them can be correlated
			my @mainCorrelatableConcepts = ();
			my @otherCorrelatedConcepts = ();
			my %correlatableConcepts = ();
			my %chainedConcepts = ();
			foreach my $set (@conceptMatchArray) {
				my($concept,$matchedFileset) = @{$set};
				
				my $conceptKey = $concept+0;
				$correlatableConcepts{$conceptKey} = BP::Loader::CorrelatableConcept->($concept,map { $_->[0] } @{$matchedFileset});
				
				# Save for later processing
				if($mapper->nestedCorrelatedConcepts() && ! $concept->goesToCollection()) {
					my $idConcept = $concept->idConcept;
					
					if(defined($idConcept)) {
						my $idkey = $idConcept+0;
						$chainedConcepts{$idkey} = []  unless(exists($chainedConcepts{$idkey}));
						push(@{$chainedConcepts{$idkey}},$correlatableConcepts{$conceptKey});
					} else {
						Carp::croak('FATAL ERROR: Concept '.$concept->id.' does not go to a collection and it does not have an identifying concept\n');
					}
					push(@otherCorrelatedConcepts,$correlatableConcepts{$conceptKey});
				} else {
					push(@mainCorrelatableConcepts,$correlatableConcepts{$conceptKey});
				}
			}
			
			my @freeSlavesCorrelatableConcepts = ();
			
			if($mapper->nestedCorrelatedConcepts()) {
				# Let's visit the possible correlations
				my %disabledCorrelatedConcepts = ();
				foreach my $idkey (keys(%chainedConcepts)) {
					if(exists($correlatableConcepts{$idkey})) {
						# First, register them as correlatable
						my $correlatableConcept = $correlatableConcepts{$idkey};
						foreach my $correlatedConcept (@{$chainedConcepts{$idkey}}) {
							$correlatableConcept->addCorrelatedConcept($correlatedConcept);
							
							# Then, save their keys, to be disabled when they are processed
							$disabledCorrelatedConcepts{$correlatedConcept+0} = undef;
						}
					}
				}
				
				foreach my $correlatedConcept (@otherCorrelatedConcepts) {
					# Skip the ones which are already chained
					next  if(exists($disabledCorrelatedConcepts{$correlatedConcept+0}));
					push(@freeSlavesCorrelatableConcepts,$correlatedConcept);
				}
			} else {
				@freeSlavesCorrelatableConcepts = @otherCorrelatedConcepts;
			}
			
			# Now, let's load!
			$mapper->mapData(\@mainCorrelatableConcepts,\@freeSlavesCorrelatableConcepts);
		}
	} elsif($ini->exists($BP::Loader::Mapper::SECTION,'metadata-models')) {
		# Setting up other storage models, and generate the native models
	
		my $storageModelNames = $ini->val($BP::Loader::Mapper::SECTION,'metadata-models');
		foreach my $storageModelName (split(/,/,$storageModelNames)) {
			$storageModels{$storageModelName} = BP::Loader::Mapper->newInstance($storageModelName,$model,$ini)  unless(exists($storageModels{$storageModelName}));
			
			print "Generating native model for $storageModelName...\n";
			my $p_list = $storageModels{$storageModelName}->generateNativeModel($workingDir);
			foreach my $p_path (@{$p_list}) {
				print "\t* ",$p_path->[0]," (",($p_path->[1]?"required":"optional"),")\n";
			}
			print "\tDONE!\n";
		}
	}
} else{
	print STDERR <<EOF ;
ERROR: This program ($0) takes as input a INI file with the configuration pointing to the model and a working directory
	* With no additional parameters, it generates the data model files.
	* With one or more files, it stores them in the destination database.\n";
EOF
}


