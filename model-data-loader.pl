#!/usr/bin/perl -W

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
use DCC::Model;
use DCC::Loader::CorrelatableConcept;
use DCC::Loader::Storage;
# These are included so they self-register on DCC::Loader::Storage
use DCC::Loader::Storage::Relational;
use DCC::Loader::Storage::MongoDB;

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
	my $ini = Config::IniFiles->new(-file => $iniFile);
	
	# And create the working directory
	File::Path::make_path($workingDir);
	
	# Let's parse the model
	my $modelFile = $ini->val('main','model');
	# Setting up the right path on relative cases
	$modelFile = File::Spec->catfile(File::Basename::dirname($iniFile),$modelFile)  unless(File::Spec->file_name_is_absolute($modelFile));
	
	my $model = undef;
	eval {
		$model = DCC::Model->new($modelFile);
	};
	
	if($@) {
		Carp::croak('ERROR: Model parsing and validation failed. Reason: '.$@);
	}
	
	# Setting up the loader storage model
	Carp::croak('ERROR: undefined destination storage model')  unless($ini->exists('storage','load'));
	my $loadModel = $ini->val('storage','load');
	
	my $loader = DCC::Loader::Storage->new($loadModel,$model,$ini);
	
	my %storageModels = (
		$loadModel => $loader
	);
	
	# Setting up other storage models, and generate the native models
	if($ini->exists('storage','model')) {
		my $storageModelNames = $ini->val('storage','model');
		foreach my $storageModelName (split(/,/,$storageModelNames)) {
			$storageModels{$storageModelName} = DCC::Loader::Storage->new($storageModelName,$model,$ini)  unless(exists($storageModels{$storageModelName}));
			
			print "Generating native model for $storageModelName... ";
			$storageModels{$storageModelName}->generateNativeModel($workingDir);
			print "DONE!\n";
		}
	}
	
	# Is there any file to load to the database?
	if(scalar(@ARGV)>0) {
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
		
		# Now, let's create the possibly correlatable concepts
		# and check which of them can be correlated
		my @mainCorrelatableConcepts = ();
		my @otherCorrelatedConcepts = ();
		my %correlatableConcepts = ();
		my %chainedConcepts = ();
		foreach my $set (@conceptMatchArray) {
			my($concept,$matchedFileset) = @{$set};
			
			my $conceptKey = $concept+0;
			$correlatableConcepts{$conceptKey} = DCC::Loader::CorrelatableConcept->($concept,map { $_->[0] } @{$matchedFileset});
			
			# Save for later processing
			if($loader->isHierarchical() && ! $concept->goesToCollection()) {
				my $idConcept = $concept->idConcept;
				
				if(defined($idConcept)) {
					my $idkey = $idConcept+0;
					$chainedConcepts{$idkey} = []  unless(exists($chainedConcepts{$idkey}));
					push(@{$chainedConcepts{$idkey}},$correlatableConcepts{$conceptKey});
				} else {
					Carp::croak('FATAL ERROR: Concept '.$concept->_jsonId.' does not go to a collection and it does not have an identifying concept\n');
				}
				push(@otherCorrelatedConcepts,$correlatableConcepts{$conceptKey});
			} else {
				push(@mainCorrelatableConcepts,$correlatableConcepts{$conceptKey});
			}
		}
		
		my @freeSlavesCorrelatableConcepts = ();
		
		if($loader->isHierarchical()) {
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
		$loader->loadConcepts(\@mainCorrelatableConcepts,\@freeSlavesCorrelatableConcepts);
	}
} else{
	print STDERR "ERROR: This program takes as input a INI file with the configuration pointing to the model, a working directory and, optionally, one or more files to store in the destination database.\n";
}


