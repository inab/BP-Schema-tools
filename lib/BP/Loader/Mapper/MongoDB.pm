#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;

use MongoDB 0.704.0.0;
use Tie::IxHash;

package BP::Loader::Mapper::MongoDB;

use Scalar::Util qw(blessed);

use base qw(BP::Loader::Mapper::NoSQL);

our $SECTION;

BEGIN {
	$SECTION = 'mongodb';
	$BP::Loader::Mapper::storage_names{$SECTION}=__PACKAGE__;
};

my @DEFAULTS = (
	['db' => undef],
	['host' => undef],
	['port' => ''],
	['user' => ''],
	['pass' => ''],
	['timeout' => ''],
	['max-array-terms' => 256]
);

# Constructor parameters:
#	model: a BP::Model instance
#	config: a Config::IniFiles instance
sub new($$) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	my $model = shift;
	my $config = shift;
	
	my $self  = $class->SUPER::new($model,$config);
	bless($self,$class);
	
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

# As there are no native relations, try nesting the correlated concepts
sub nestedCorrelatedConcepts {
	return 1;
}

# This method returns a connection to the database
# In this case, it returns a MongoDB::MongoClient instance
sub _connect() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $MONGODB = $self->{db};
	my $MONGOHOST = $self->{host};
	my $hostString = 'mongodb://'.$MONGOHOST;
	$hostString .= ':'.$self->{port}  if(defined($self->{port} && $self->{port} ne ''));
	
	my @clientParams = ('host' => $hostString);
	push(@clientParams,'username' => $self->{user},'password' => $self->{pass})  if(defined($self->{user}) && $self->{user} ne '');
	
	# We want MongoDB to return booleans as booleans, not as integers
	$MongoDB::BSON::use_boolean = 1;
	# Let's test the connection
	my $client = MongoDB::MongoClient->new(@clientParams);
	
	$client->query_timeout($self->{'timeout'})  if(defined($self->{'timeout'}) && $self->{'timeout'} ne '');
	my $db = $client->get_database($MONGODB);
	# This is needed to fragment the insertions
	$self->{_BSONSIZE} = $client->max_bson_size;
	
	return $db;
}

# getNativeDestination parameters:
#	collection: a BP::Model::Collection instance
# It returns a native collection object, to be used by bulkInsert, for instance
sub getNativeDestination($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $collection = shift;
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $db = $self->connect();
	my $coll = $db->get_collection($collection->path);
	
	return $coll;
}

# _EnsureIndexes parameters:
#	coll: a native instance of the concept of collection
#	indexes: An array of BP::Model::Index instances
sub _EnsureIndexes($@) {
	my($coll,@indexes) = @_;
	foreach my $index  (@indexes) {
		my $idxDecl = Tie::IxHash->new();
		foreach my $p_colIdx (@{$index->indexAttributes}) {
			$idxDecl->Push(@{$p_colIdx});
		}
		$coll->ensure_index($idxDecl,{'unique'=>($index->isUnique?1:0)});
	}
}

# createCollection parameters:
#	collection: A BP::Model::Collection instance
# Given a BP::Model::Collection instance, it is created, along with its indexes
sub createCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $collection = shift;
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $coll = $self->getNativeDestination($collection);
	if(ref($collection->indexes) eq 'ARRAY' && scalar(@{$collection->indexes})>0) {
		_EnsureIndexes($coll,@{$collection->indexes});
	}
	
	# And now, let's fetch all the index declarations related to all the concepts being stored here
	
	return $coll;
}

# storeNativeModel parameters:
sub storeNativeModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# First, let's create the collections and their indexes
	foreach my $collection (values(%{$self->{model}->collections})) {
		$self->createCollection($collection);
	}
	
	# Do we have to store the JSON description of the model?
	if(defined($self->{model}->metadataCollection())) {
		my $p_generatedObjects = $self->generateNativeModel(undef,exists($self->{_BSONSIZE})?$self->{_BSONSIZE}:undef,$self->{'max-array-terms'});
		
		my $metadataCollection = $self->{model}->metadataCollection();
		$metadataCollection->clearIndexes();
		# Let's add the needed meta-indexes for CV terms
		$metadataCollection->addIndexes(BP::Model::Index->new(undef,['terms.term',1]),BP::Model::Index->new(undef,['terms.parents',1]),BP::Model::Index->new(undef,['terms.ancestors',1]));
		
		my $metaColl = $self->createCollection($metadataCollection);
		
		foreach my $p_generatedObject (@{$p_generatedObjects}) {
			$self->bulkInsert($metaColl,[$p_generatedObject]);
		}
	}
}

# _genDestination parameters:
#	correlatedConcept: An instance of BP::Loader::CorrelatableConcept
#	isTemp: should it be a temporary destination?
# It sets up the destination to be used in bulkInsert calls, in this case
# a MongoDB::Collection instance
sub _genDestination($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	my $isTemp = shift;
	
	my $destColl = $isTemp?'TEMP_'.$correlatedConcept->concept->key.'_'.int(rand(2**32-1)):$correlatedConcept->concept->collection->path;
	my $db = $self->connect();
	
	return $db->get_collection($destColl);
}

# _freeDestination parameters:
#	destination: An instance of MongoDB::Collection
#	errflag: The error flag
# As it is not needed to explicitly free them, it is an empty method.
sub _freeDestination($$) {
}

# _bulkPrepare parameters:
#	entorp: The output of BP::Loader::CorrelatableConcept->readEntry
# It returns the bulkData to be used for the load
sub _bulkPrepare($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $entorp = shift;
	$entorp = [ $entorp ]  unless(ref($entorp) eq 'ARRAY');
	
	return $entorp;
}

# _bulkInsert parameters:
#	destination: The destination of the bulk insertion (a MongoDB::Collection instance)
#	p_batch: a reference to an array of hashes which contain the values to store.
sub _bulkInsert($\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $destination = shift;
	my $p_batch = shift;
	
	Carp::croak("ERROR: bulkInsert needs a MongoDB::Collection instance")  unless(blessed($destination) && $destination->isa('MongoDB::Collection'));
	
	return $destination->batch_insert($p_batch);
}

1;
