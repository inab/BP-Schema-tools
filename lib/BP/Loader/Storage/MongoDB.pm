#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;

use MongoDB;
use Config::IniFiles;

package BP::Loader::Storage::MongoDB;

use base qw(BP::Loader::Storage);

our $SECTION;

BEGIN {
	$SECTION = 'mongodb';
	$BP::Loader::Storage::storage_names{$SECTION}=__PACKAGE__;
};

my @DEFAULTS = (
	['db' => undef],
	['host' => undef],
	['port' => 27017],
	['batch-size' => 20000]
);

# Constructor parameters:
#	model: a BP::Model instance
#	config: a Config::IniFiles instance
sub new($$) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	# my $self  = $class->SUPER::new();
	my $self = {};
	bless($self,$class);
	
	$self->{model} = shift;
	
	my $config = shift;
	
	if($config->SectionExists($SECTION)) {
		foreach my $param (@DEFAULTS) {
			my($key,$defval) = @{$param};
			
			if(defined($defval)) {
				$self->{$key} = $config->val($SECTION,$key,$defval);
			} elsif($config->exists($SECTION,$key)) {
				$self->{$key} = $config->val($SECTION,$key);
			} else {
				Carp::croak("ERROR: required parameter $key not found in section $SECTION");
			}
		}
		
	} else {
		Carp::croak("ERROR: Unable to read section $SECTION");
	}
	
	return $self;
}

sub isHierarchical {
	return 1;
}

# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
# It returns a list of relative paths to the generated files
sub generateNativeModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
}

# loadConcepts parameters:
#	p_mainCorrelatableConcepts: a reference to an array of BP::Loader::CorrelatableConcept instances.
#	p_otherCorrelatedConcepts: a reference to an array of BP::Loader::CorrelatableConcept instances (the "free slaves" ones).
sub loadConcepts(\@\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_mainCorrelatableConcepts = shift;
	my $p_otherCorrelatedConcepts = shift;
	
	my $MONGODB = $self->{db};
	my $MONGOHOST = $self->{host};
	my $MONGOPORT = $self->{port};
	my $BMAX = $self->{'batch-size'};
	
	# Let's test the connection
	my $connection = MongoDB::Connection->new(host => $MONGOHOST, port => $MONGOPORT);
	my $db = $connection->get_database($MONGODB);
	
	# Phase1: iterate over the main ones
	foreach my $correlatedConcept (@{$p_mainCorrelatableConcepts}) {
		eval {
			# Any needed sort happens here
			$correlatedConcept->openFiles();
			
			# The destination collection
			my $destColl = $correlatedConcept->concept->collection->path;
			my $mongoColl = $db->get_collection($destColl);
			
			# Let's store!!!!
			while(my $entorp = $correlatedConcept->readEntry($BMAX)) {
				$mongoColl->batch_insert($entorp->[0]);
			}
			$correlatedConcept->closeFiles();
		};
		
		if($@) {
			print STDERR "ERROR: While storing the main concepts\n";
			exit 1;
		}
	}
	
	# Phase2: iterate over the other ones which could have chained, but aren't
	foreach my $correlatedConcept (@{$p_otherCorrelatedConcepts}) {
		# Main storage on a fake collection
		eval {
			# Any needed sort happens here
			$correlatedConcept->openFiles();
			
			# The destination 'fake' collection
			my $destColl = 'TEMP_'.$correlatedConcept->concept->key.'_'.int(rand(2**32-1));
			my $mongoColl = $db->get_collection($destColl);
			
			# Let's store!!!!
			while(my $entorp = $correlatedConcept->readEntry($BMAX)) {
				$mongoColl->batch_insert($entorp->[0]);
			}
			$correlatedConcept->closeFiles();
		
			# TODO: Send the mapReduce sentence to join inside the database
			
		};
		
		if($@) {
			print STDERR "ERROR: While storing the main concepts\n";
			exit 1;
		}
	}
}

1;
