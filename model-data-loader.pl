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

use FindBin;
use lib "$FindBin::Bin/lib";
use DCC::Model;
use DCC::Loader::CorrelatableConcept;

use File::Temp;
use Sys::CPU;
#use Sys::MemInfo;
use Time::HiRes;

use MongoDB;

sub sortCompressed($\%);
sub mapValues($$$);
sub keyMatches($$);

my $MONGOHOST = '127.0.0.1';
my $MONGOPORT = 27017;
my $MONGODB = 'mongotest';

my $BMAX=20000;

my %P_DESC = (
	'fields' => {
		'analysis_id' => ['s'],
		'analyzed_sample_id' => ['s'],
		'methylated_fragment_id' => ['s'],
		'chromosome' => ['s'],
		'chromosome_start' => ['i'],
		'chromosome_end' => ['i'],
		'chromosome_strand' => ['i'],
		'beta_value' => ['d'],
		'quality_score' => ['d'],
		'probability' => ['d'],
		'validation_status' => ['s'],
		'validation_platform' => ['s'],
		'note' => ['s'],
	},
	'keys' => ['analysis_id','analyzed_sample_id','methylated_fragment_id']
);

my %S_DESC = (
	'fields' => {
		'analysis_id' => ['s'],
		'analyzed_sample_id' => ['s'],
		'methylated_fragment_id' => ['s'],
		'gene_affected' => ['s'],
		'gene_build_version' => ['i'],
		'note' => ['s'],
	},
	'keys' => ['analysis_id','analyzed_sample_id','methylated_fragment_id']
);

sub sortCompressed($\%) {
	my($infile,$p_desc)=@_;
	
	my $tmpout = File::Temp->new();
	my $tmpoutfilename = $tmpout->filename();
	
	my @coldesc=();
	my @keypos=();
	
	# Let's fetch the header
	# so we can map the reading positions
	my $H;
	if(open($H,"'$GREP' -v '^#' '$infile' | head -n 1 |")) {
		# Reading only the header
		my $header = <$H>;
		
		close($H);
		
		# And now, let's process the header!
		chomp($header);
		my $pos = 0;
		my %k_hash = map { $_ => undef } @{$p_desc->{'keys'}};
		foreach my $col (split(/\t/,$header)) {
			my $type = (exists($p_desc->{'fields'}{$col}))?$p_desc->{'fields'}{$col}[0]:'s';
			push(@coldesc,[$col,$type]);
			
			$k_hash{$col}=$pos  if(exists($k_hash{$col}));
			
			$pos++;
		}
		# Mapping positions to keys
		@keypos=@k_hash{@{$p_desc->{'keys'}}};
	} else {
		print STDERR "Algo huele a podrido en los Alpes\n";
	}
	
	
	
	my $sortkeys = join(' ',map { my $p = $_ + 1 ; '-k'.$p.','.$p } @keypos);
	
	# This command line reorders the lines
	# keeping the comments and the header at the beginning of the file
	# and sorting by the input keys
	# my $cmd = "('$GREP' '^#' '$infile' ; '$GREP' -v '^#' '$infile' | head -n 1 ; '$GREP' -v '^#' '$infile' | tail -n +2 | '$SORT' --parallel=$NUMCPUS -S 50% $sortkeys ) | gzip -9c > '$tmpoutfilename'";
	
	# This command line reorders the lines
	# pruning both the comments and header at the beginning of the file
	my $cmd = "'$GREP' -v '^#' '$infile' | tail -n +2 | '$SORT' --parallel=$NUMCPUS -S 50% $sortkeys | gzip -9c > '$tmpoutfilename'";
	
	system($cmd);
	
	return ($tmpout,\@coldesc,\@keypos);
}

# mapValues parameters:
#	line:
#	p_keypos: Array with the column numbers of the keys
#	p_coldesc: Array with a coarse description of the columns (names + types)
# It returns:
#	entry: A reference to a hash with the entry -> keys are column names and their values, the values
#	keyvals: A reference to an array, with the string value assigned to the columns
sub mapValues($$$) {
	my($line,$p_keypos,$p_coldesc)=@_;
	# The cells
	my @cells = split(/\t/,$line);
	
	# The key values to be compared to
	my @keyvals = @cells[@{$p_keypos}];
	
	my %entry = ();
	my $pos = 0;
	foreach my $cell (@cells) {
		$entry{$p_coldesc->[$pos][0]} = ($p_coldesc->[$pos][1] eq 'i')?($cell+0):(($p_coldesc->[$pos][1] eq 'd')?($cell+0.0):$cell);
		
		$pos++;
	}
	
	return (\%entry,\@keyvals);
}

sub keyMatches($$) {
	my($l,$r)=@_;
	
	return 0  if @{$l}~~@{$r};
	my $maxidx = $#{$l};
	
	foreach my $pos (0..$maxidx) {
		return -1  if($l->[$pos] lt $r->[$pos]);
		return 1  if($l->[$pos] gt $r->[$pos]);
	}
	
	return 0;
}

sub tiempo($$) {
	my($label,$prev)=@_;
	my $next = time();
	
	print STDERR 'DEBUG: ',$label,' ',$next-$prev,"\n";
	
	return $next;
}

# It takes as input arrayrefs of [filename, DCC::Model::Concept]
# The order of the concepts is crucial!!!
sub batchCorrelatedInsert($@) {
	my($coll,@corrConcepts)=@_;
	
	# Second, let's prepare the input data
	my @correlatedConcepts = ();
	my $prev = time();
	foreach my $corrConcept (@corrConcepts) {
		$corrConcept->prepare();
		push(@correlatedConcepts,$corrConcept);
		
		$prev = tiempo('SORT-',$prev);
	}
	
	# Third, let's read it in a correlated way, and let's insert it in batches!
	# As headers and comments have been previously parsed
	# and yanked when the contents were parsed, we don't
	# have to bother about them
	
	
	my($p_entry,$p_keyvals) = (undef,undef);
	my $H = $correlatedConcepts[$#correlatedConcepts][0];
	my $line = <$H>;
	unless(eof($H)) {
		chomp($line);
		
		
		@{$correlatedConcepts[$#correlatedConcepts]}[1,2] = 
	}
}

if(scalar(@ARGV)>=3) {
	my $modelFile = shift(@ARGV);
	my $primaryFile = shift(@ARGV);
	my $secondaryFile = shift(@ARGV);
	my $model = undef;
	
	eval {
		$model = DCC::Model->new($modelFile);
	};
	
	if($@) {
		Carp::croak('ERROR: Model loading and validation failed. Reason: '.$@);
	}
	
	# TO UNHARDCODE :P
	my $conceptDomainName = 'dlat';
	my $primaryConceptName = 'mr';
	my $secondaryConceptName = 's';
	
	my $conceptDomain = $model->getConceptDomain($conceptDomainName);
		
	unless(exists($conceptDomain->conceptHash->{$primaryConceptName})) {
		Carp::croak("ERROR: Unknown concept name $primaryConceptName in concept domain $conceptDomainName");
	}
	my $primaryConcept = $conceptDomain->conceptHash->{$primaryConceptName};
	
	my $primaryConceptType = $primaryConcept->baseConceptType;
	unless(defined($primaryConceptType) && $primaryConceptType->isCollection && defined($primaryConceptType->collection)) {
		Carp::croak("ERROR: Concept $primaryConceptName has no destination collection!");
	}
	# The destination collection
	my $destColl = $primaryConceptType->collection->path;

	unless(exists($conceptDomain->conceptHash->{$secondaryConceptName})) {
		Carp::croak("ERROR: Unknown concept name $secondaryConceptName in concept domain $conceptDomainName");
	}
	my $secondaryConcept = $conceptDomain->conceptHash->{$secondaryConceptName};
	
	die "ERROR: primary file '$primaryFile' unreadable!\n"  unless(-f $primaryFile && -r $primaryFile);
	die "ERROR: secondary file '$secondaryFile' unreadable!\n"  unless(-f $primaryFile && -r $primaryFile);
	
	# First, let's test the connection
	my $connection = MongoDB::Connection->new(host => $MONGOHOST, port => $MONGOPORT);
	my $db = $connection->get_database($MONGODB);
	my $coll = $db->get_collection($destColl);
	
	# This is hard-coded, but it will be dynamic
	my $secondaryCorrConcept = DCC::Loader::CorrelatableConcept->new($secondaryFile,$secondaryConcept);
	my $primaryCorrConcept = DCC::Loader::CorrelatableConcept->new($primaryFile,$primaryConcept,$secondaryCorrConcept);
	
	batchCorrelatedInsert($coll,$primaryCorrConcept,$secondaryCorrConcept);
	
	# Second, let's prepare the input data
	my $prev = time();
	my($primarySorted,$p_coldesc,$p_keypos) = sortCompressed($primaryFile,%P_DESC);
	$prev = tiempo('SORT-P',$prev);
	my($secondarySorted,$s_coldesc,$s_keypos) = sortCompressed($secondaryFile,%S_DESC);
	$prev = tiempo('SORT-S',$prev);
	
	# Third, let's read it in a correlated way, and let's insert it in batches!
	if(open(my $P,'-|','gunzip','-c',$primarySorted->filename())) {
		if(open(my $S,'-|','gunzip','-c',$secondarySorted->filename())) {
			# As headers and comments have been previously parsed
			# and yanked when the contents were parsed, we don't
			# have to bother about them
			
			# Prefilling secondaries
			my($p_secondary,$s_keyvals)=(undef,undef);
			my $sline=<$S>;
			unless(eof($S)) {
				chomp($sline);
				
				($p_secondary,$s_keyvals) = mapValues($sline,$s_keypos,$s_coldesc);
			}
			
			# And now, let's read the primaries
			my @batch=();
			my $batchsize = 0;
			while(my $pline=<$P>) {
				chomp($pline);
				
				# Let's prepare the field for the serialization
				my @secondaries = ();
				
				my($p_primary,$p_keyvals) = mapValues($pline,$p_keypos,$p_coldesc);
				
				$p_primary->{'secondaries'}=\@secondaries;
				
				if(defined($s_keyvals)) {
					my $cval = keyMatches($p_keyvals,$s_keyvals);
					#print STDERR "DEBUG: @{$p_keyvals} @{$s_keyvals}\n"  if($cval>0);
					if($cval>=0) {
						# Let's save this, if they match
						push(@secondaries,$p_secondary)  if($cval==0);
						
						# And let's seek more!
						my $more = undef;
						while(my $sline=<$S>) {
							chomp($sline);
							
							($p_secondary,$s_keyvals) = mapValues($sline,$s_keypos,$s_coldesc);
							
							$cval = keyMatches($p_keyvals,$s_keyvals);
							#print STDERR "DEBUG2: $cval\n";
							if($cval>=0) {
								# Let's also save this one, if they really match!
								push(@secondaries,$p_secondary)  if($cval==0);
							} else {
								$more = 1;
								last;
							}
						}
						
						# We could have finished the secondaries file!
						$s_keyvals = undef  unless(defined($more));
					}
				}
				
				# And now, let's insert it!
				# TODO
				#print to_json($p_primary),"\n";
				push(@batch,$p_primary);
				$batchsize++;
				if($batchsize>=$BMAX) {
					$coll->batch_insert(\@batch);
					@batch = ();
					$batchsize = 0;
				}
			}
			
			$coll->batch_insert(\@batch)  if($batchsize>0);
			$prev = tiempo('BATCHINSERT',$prev);
			
			close($S);
		} else {
			close($P);
			Carp::croak("FATAL ERROR: Unable to open sorted temporal secondary file!");
		}
		
		close($P);
	} else {
		Carp::croak("FATAL ERROR: Unable to open sorted temporal primary file!");
	}
	
} else{
	print STDERR "ERROR: This program takes as input a model and two files. The files are the primary data, and the secondary data";
}


