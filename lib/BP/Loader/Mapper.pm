#!/usr/bin/perl -W

use strict;
use Carp;

package BP::Loader::Mapper;

use BP::Model;

# This constant is used by several storage models.
# Therefore, it is better defined here.
use constant FILE_PREFIX_KEY => 'file-prefix';

our $SECTION;
our $DEFAULTSECTION;
BEGIN {
	$DEFAULTSECTION = 'main';
	$SECTION = 'mapper';
}

# The registered storage models
our %storage_names;

my @DEFAULTS = (
	[BP::Loader::Mapper::FILE_PREFIX_KEY => 'model'],
	['batch-size' => 20000],
	['release' => 'true'],
);

# Constructor parameters:
#	storageModel: the key identifying the storage model
#	model: a BP::Model instance
#	other parameters
sub newInstance($$;@) {
	# Very special case for multiple inheritance handling
	# This is the seed
	my($self)=shift;
	my($class)=ref($self) || $self;
	
	my $storageModel = shift;
	Carp::croak("ERROR: undefined storage model")  unless(defined($storageModel));
	
	$storageModel = lc($storageModel);
	$storageModel =~ s/[^-a-z]//g;
	
	if(exists($storage_names{$storageModel})) {
		my $class  = $storage_names{$storageModel};
		my $model = shift;
		return $class->new($model,@_);
	} else {
		Carp::croak("ERROR: unregistered storage model $storageModel\n");
	}
}

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
	
	Carp::croak("ERROR: model must be an instance of BP::Model")  unless(ref($self->{model}) && $self->{model}->isa('BP::Model'));
	
	my $config = shift;
	
	Carp::croak("ERROR: config must be an instance of Config::IniFiles")  unless(ref($config) && $config->isa('Config::IniFiles'));
	
	if(scalar(@DEFAULTS)>0) {
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
	}
	
	# "Digitalizing" release configuration variable
	$self->{release}=(defined($self->{release}) && ($self->{release} eq 'true' || $self->{release} eq '1'))?1:undef;
	
	return $self;
}

# setFilePrefix parameters:
#	newPrefix: the new prefix for the generated files
sub setFilePrefix($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	$self->{BP::Loader::Mapper::FILE_PREFIX_KEY} = shift;
}

# This method tells whether we follow a nesting strategy or not for correlated concepts
sub nestedCorrelatedConcepts {
	Carp::croak('Unimplemented method!');
}

# generateNativeModel parameters:
#	workingDir: The directory where the native model files are going to be saved.
# It returns a reference to an array of pairs
#	[absolute paths to the generated files (based on workingDir),is essential]
sub generateNativeModel($) {
	Carp::croak('Unimplemented method!');
}

# This method connects to the database and returns the handler. The connection
# is persistent so next calls to the method should return the same instance.
sub connect() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	$self->{conn} = $self->_connect()  unless(exists($self->{conn}));
	
	return $self->{conn};
}

# This method returns a connection to the database
sub _connect() {
	Carp::croak('Unimplemented method!');
}

# storeNativeModel parameters:
#	workingDir: The optional directory where the native model files are going to be saved.
sub storeNativeModel(;\$) {
	Carp::croak('Unimplemented method!');
}

# _genDestination parameters:
#	corrConcept: An instance of BP::Loader::CorrelatableConcept
#	isTemp: Sets up a temp destination
# It returns the destination to be used in bulkInsert calls, which depends
# on the Mapper implementation. It can prepare a sentence and also start a transaction.
sub _genDestination($;$) {
	Carp::croak('Unimplemented method!');
}

# setDestination parameters:
#	corrConcept: An instance of BP::Loader::CorrelatableConcept
#	isTemp: Sets up a temp destination
# It sets up the destination to be used in bulkInsert calls, which depends
# on the Mapper implementation. It can prepare a sentence and also start a transaction.
sub setDestination($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	Carp::croak('Destination was already setup!')  if(exists($self->{_destination}));
	
	my $correlatedConcept = $_[0];
	
	Carp::croak("ERROR: setDestination needs a BP::Loader::CorrelatableConcept instance")  unless(ref($correlatedConcept) && $correlatedConcept->isa('BP::Loader::CorrelatableConcept'));
	
	# Any needed sort happens here
	$correlatedConcept->openFiles();
	
	$self->{_destination} = $self->_genDestination(@_);
	$self->{_correlatedConcept} = $correlatedConcept;
}

# _freeDestination parameters:
#	destination: the destination to be freed
#	errflag: The error flag
# It frees a destination previously set up, which dependes on the Mapper implementation.
# It can also finish a transaction, based on the error flag
sub _freeDestination($;$) {
	Carp::croak('Unimplemented method!');
}

# freeDestination parameters:
#	errflag: The error flag
# It frees a destination, in this case a prepared statement
# It can also finish a transaction, based on the error flag
sub freeDestination(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(exists($self->{_destination})) {
		$self->_freeDestination($self->{_destination},@_);
		delete($self->{_destination});
		
		$self->{_correlatedConcept}->closeFiles();
		delete($self->{_correlatedConcept});
	}
}

# _bulkPrepare parameters:
#	correlatedConcept: A BP::Loader::CorrelatableConcept instance
#	entorp: The output of BP::Loader::CorrelatableConcept->readEntry
# It returns the bulkData to be used for the load
sub _bulkPrepare($$) {
	Carp::croak('Unimplemented method!');
}


# bulkInsert parameters:
#	destination: The destination of the bulk insertion.
#	bulkData: a reference to an array of hashes which contain the values to store.
sub _bulkInsert($\@) {
	Carp::croak('Unimplemented method!');
}

# bulkInsert parameters:
#	bulkData: a reference to an array of hashes which contain the values to store.
sub bulkInsert(\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->_bulkInsert($self->{_destination},@_);
}

# parseOrderingHints parameters:
#	a XML::LibXML::Element, type 'dcc:ordering-hints'
# It returns the ordering hints (at this moment, undef or the block where it appears)
sub parseOrderingHints($) {
	my($ordHints) = @_;
	
	my $retvalBlock = undef;
	if(ref($ordHints) && $ordHints->isa('XML::LibXML::Element')
		&& $ordHints->namespaceURI eq BP::Model::dccNamespace
		&& $ordHints->localname eq 'ordering-hints'
	) {
		foreach my $block ($ordHints->getChildrenByTagNameNS(BP::Model::dccNamespace,'block')) {
			$retvalBlock = $block->textContent;
			last;
		}
	}
	
	return ($retvalBlock);
}

# _fancyColumnOrdering parameters:
#	concept: a BP::Concept instance
# It returns an array with the column names from the concept in a fancy
# order, based on several criteria, like annotations
sub _fancyColumnOrdering($) {
	my($concept)=@_;
	
	my $columnSet = $concept->columnSet;

	# First, the idref columns, alphabetically ordered
	my @first = ();
	my @middle = ();
	my @last = ();
	foreach my $columnName (@{$columnSet->idColumnNames}) {
		my $column = $columnSet->columns->{$columnName};
		
		my $p_set = \@middle;
		if(exists($column->annotations->hash->{ordering})) {
			my($block) = parseOrderingHints($column->annotations->hash->{ordering});
			
			if($block eq 'bottom') {
				$p_set = \@last;
			} elsif($block eq 'top') {
				$p_set = \@first;
			}
		}
		
		push(@{$p_set},$columnName);
	}
	
	my @idcolorder = (@first,@middle,@last);
	# Resetting for next use
	@first = ();
	@middle = ();
	@last = ();

	# And then, the others
	my %idcols = map { $_ => undef } @idcolorder;
	foreach my $columnName (@{$columnSet->columnNames}) {
		next  if(exists($idcols{$columnName}));
		
		my $column = $columnSet->columns->{$columnName};
		
		my $p_set = \@middle;
		if(exists($column->annotations->hash->{ordering})) {
			my($block) = parseOrderingHints($column->annotations->hash->{ordering});
			
			if($block eq 'bottom') {
				$p_set = \@last;
			} elsif($block eq 'top') {
				$p_set = \@first;
			}
		}
		
		push(@{$p_set},$columnName);
	}
	
	my @colorder = (@first,@middle,@last);
	
	
	return (@idcolorder,@colorder);
}

# readEntry parameters:
#	BMAX: The max number of entries to fetch
# It reads the entry from the previously registered correlatedConcept
# It returns a reference to a bulk of data which can be managed by bulkInsert
sub readEntry($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = $self->{_correlatedConcept};
	my $BMAX = shift;
	
	my $entorp = $correlatedConcept->readEntry($BMAX);
	
	return $self->_bulkPrepare($correlatedConcept,$entorp);
}

# mapData parameters:
#	p_mainCorrelatableConcepts: a reference to an array of BP::Loader::CorrelatableConcept instances.
#	p_otherCorrelatedConcepts: a reference to an array of BP::Loader::CorrelatableConcept instances (the "free slaves" ones).
sub mapData(\@\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_mainCorrelatableConcepts = shift;
	my $p_otherCorrelatedConcepts = shift;
	
	my $BMAX = $self->{'batch-size'};
	
	my $db = $self->connect();
	
	# Phase1: iterate over the main ones
	foreach my $correlatedConcept (@{$p_mainCorrelatableConcepts}) {
		eval {
			# The destination collection
			$self->setDestination($correlatedConcept);
			
			# Let's store!!!!
			my $errflag = undef;
			my $batchData = undef;
			for($batchData = $self->readEntry($BMAX) ; $correlatedConcept->eof() ; $batchData = $self->readEntry($BMAX)) {
				$self->bulkInsert($batchData);
			}
			$self->freeDestination($errflag);
		};
		
		if($@) {
			Carp::croak("ERROR: While storing the main concepts. Reason: $@");
		}
	}
	
	# Phase2: iterate over the other ones which could have chained, but aren't
	foreach my $correlatedConcept (@{$p_otherCorrelatedConcepts}) {
		# Main storage on a fake collection
		eval {
			# The destination 'fake' collection
			$self->setDestination($correlatedConcept,1);
			
			# Let's store!!!!
			my $errflag = undef;
			my $batchData = undef;
			for($batchData = $self->readEntry($BMAX) ; $correlatedConcept->eof() ; $batchData = $self->readEntry($BMAX)) {
				$self->bulkInsert($batchData);
			}
			$self->freeDestination($errflag);
		
			# TODO: Send the mapReduce sentence to join inside the database
			
		};
		
		if($@) {
			Carp::croak("ERROR: While storing the main concepts. Reason: $@");
		}
	}
}

1;
