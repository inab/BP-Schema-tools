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
		my $prefix = $index->prefix;
		$prefix = defined($prefix) ? ($prefix.'.') : '';
		foreach my $p_colIdx (@{$index->indexAttributes}) {
			$idxDecl->Push($prefix.$p_colIdx->[0],$p_colIdx->[1]);
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
	my $colid = $collection+0;
	if(exists($self->{_colConcept}{$colid})) {
		foreach my $concept (@{$self->{_colConcept}{$colid}}) {
			my @derivedIndexes = $concept->derivedIndexes();
			
			_EnsureIndexes($coll,@derivedIndexes)  if(scalar(@derivedIndexes) > 0);
			
			# TODO: deal with weak entities
		}
	}
	
	return $coll;
}

# storeNativeModel parameters:
sub storeNativeModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $metadataCollection = undef;
	if(defined($self->{model}->metadataCollection())) {
		$metadataCollection = $self->{model}->metadataCollection();
		$metadataCollection->clearIndexes();
		# Let's add the needed meta-indexes for CV terms
		$metadataCollection->addIndexes(BP::Model::Index->new('terms',undef,['term',1]),BP::Model::Index->new('terms',undef,['parents',1]),BP::Model::Index->new('terms',undef,['ancestors',1]));
	}	
	
	# First, let's create the collections and their indexes
	foreach my $collection (values(%{$self->{model}->collections})) {
		$self->createCollection($collection);
	}
	
	# Do we have to store the JSON description of the model?
	if(defined($metadataCollection)) {
		my $p_generatedObjects = $self->generateNativeModel(undef,exists($self->{_BSONSIZE})?$self->{_BSONSIZE}:undef,$self->{'max-array-terms'});
		
		my $db = $self->connect();
		my $metaColl = [$db->get_collection($metadataCollection->path),undef,undef];
		
		foreach my $p_generatedObject (@{$p_generatedObjects}) {
			$self->_bulkInsert($metaColl,$self->_bulkPrepare($p_generatedObject));
		}
	}
}

# It sets up the destination to be used in bulkInsert calls
# _genDestination parameters:
#	correlatedConcept: An instance of BP::Loader::CorrelatableConcept
#	isTemp: should it be a temporary destination?
# It returns a reference to a three element array:
#	a MongoDB::Collection instance
#	a list of keys corresponding to the grouping keys used for incremental updates
#	a list of keys corresponding to the submappings taken into account for incremental updates
sub _genDestination($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	my $isTemp = shift;
	
	my $destColl = $isTemp?'TEMP_'.$correlatedConcept->concept->key.'_'.int(rand(2**32-1)):$correlatedConcept->concept->collection->path;
	my $db = $self->connect();
	
	return [$db->get_collection($destColl),$correlatedConcept->groupingColumns,$correlatedConcept->incrementalColumns];
}

# _existingEntries parameters:
#	correlatedConcept: Either a BP::Model::Concept or a BP::Loader::CorrelatableConcept instance
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
#	existingFile: Destination where the file is being saved
# It dumps all the values of these columns to the file, and it returns the number of lines of the file
sub _existingEntries($$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	
	my $p_destination = shift;
	
	my $existingFile = shift;
	
	my $counter = 0;
	if(defined($p_destination->[1])) {
		my $destination  = $p_destination->[0];
		my $p_colNames = $p_destination->[1];
		
		my $concept = $correlatedConcept->isa('BP::Loader::CorrelatableConcept')?$correlatedConcept->concept():$correlatedConcept;
		
		my $cursor = $destination->query->fields(map { { $_ => 1 } } @{$p_colNames});
		
		my $sortColDef = '';
		my $kidx = 2;
		foreach my $colName (@{$p_colNames}) {
			$sortColDef .= " -k${kidx},${kidx}";
			if(exists($concept->columnSet->columns->{$colName})) {
				my $columnBaseType = $concept->columnSet->columns->{$colName}->columnType->type;
				$sortColDef .= $BP::Loader::CorrelatableConcept::SORTMAPS{$columnBaseType}  if(exists($BP::Loader::CorrelatableConcept::SORTMAPS{$columnBaseType}));
			}
			$kidx++;
		}
		
		if(open(my $EXISTING,"| ".BP::Loader::CorrelatableConcept::SORT." -S 50% --parallel=${BP::Loader::CorrelatableConcept::NUMCPUS} $sortColDef | ".BP::Loader::CorrelatableConcept::GZIP." -9c > '$existingFile'")) {
			while(my $doc = $cursor->next) {
				$counter ++;
			#	#print STDERR "DEBUG: ",ref($doc)," ",join(',',keys(%{$doc})),"\n";
			#	#print STDERR "DEBUG: ",join(',',keys(%{$fields})),"\n";
			#	#print $O join("\t",$doc->{_id},exists($fields->{chromosome})?@{$fields->{chromosome}}:(),exists($fields->{chromosome_start})?@{$fields->{chromosome_start}}:(),exists($doc->{mutated_from_allele})?@{$doc->{mutated_from_allele}}:(),exists($fields->{mutated_to_allele})?@{$fields->{mutated_to_allele}}:()),"\n";
			#	print $O join("\t",$doc->{_id},$fields->{chromosome}[0],$fields->{chromosome_start}[0],$fields->{mutated_from_allele}[0],$fields->{mutated_to_allele}[0]),"\n";
				print $EXISTING join("\t",@{$doc}{'_id',@{$p_colNames}}),"\n";
			}
			close($EXISTING);
		}
	}
	
	# Short circuiting this when it is empty
	if($counter==0) {
		$p_destination->[1] = undef;
		$p_destination->[2] = undef;
	}
	
	return $counter;
}

# _freeDestination parameters:
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
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
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
#	p_batch: a reference to an array of hashes which contain the values to store.
sub _bulkInsert($\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_destination = shift;
	
	Carp::croak("ERROR: _bulkInsert needs an array instance")  unless(ref($p_destination) eq 'ARRAY');
	my $destination = $p_destination->[0];
	Carp::croak("ERROR: _bulkInsert needs a MongoDB::Collection instance")  unless(blessed($destination) && $destination->isa('MongoDB::Collection'));
	
	my $p_batch = shift;
	
	Carp::croak("ERROR: _bulkInsert needs an array instance")  unless(ref($p_batch) eq 'ARRAY');
	
	my $p_insertBatch = $p_batch;
	
	my $count = 0;
	if(defined($p_destination->[2])) {
		my @insertBatch = ();
		foreach my $p_entry (@{$p_batch}) {
			unless($self->_incrementalUpdate($p_destination,$p_entry)) {
				push(@insertBatch,$p_entry);
			} else {
				$count++;
			}
		}
		$p_insertBatch = \@insertBatch;
	}
	
	if(scalar(@{$p_insertBatch})>0) {
		$count += $destination->batch_insert($p_insertBatch);
		
		my $db = $self->connect();
		my $lastE = $db->last_error();
		
		unless(exists($lastE->{ok}) && $lastE->{ok} eq '1') {
			Carp::croak("ERROR: Failed batch insert. Reason: ".$lastE->{err}.' '.$lastE->{errmsg});
		}
	}
	
	return $count;
}

# _incrementalUpdate parameters:
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
#	p_entry: The entry to be incrementally updated
sub _incrementalUpdate($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_destination = shift;
	my $p_entry = shift;
	
	my $retval = undef;
	
	if(defined($p_destination->[2]) && exists($p_entry->{BP::Loader::Mapper::COL_INCREMENTAL_UPDATE_ID})) {
		my @existingCols = ();
		my $pushed = undef;
		# Filtering out optional columns with no value
		foreach my $columnName (@{$p_destination->[2]}) {
			if(exists($p_entry->{$columnName})) {
				push(@existingCols,$columnName);
				$pushed=1;
			}
		}
		
		if($pushed) {
			$p_destination->[0]->update(
				{
					'_id'	=> $p_entry->{BP::Loader::Mapper::COL_INCREMENTAL_UPDATE_ID}
				},
				{
					'$push'	=> { map { $_ => { '$each' => $p_entry->{$_} } } @existingCols }
				}
			);
			my $db = $self->connect();
			my $lastE = $db->last_error();
			
			if(exists($lastE->{ok}) && $lastE->{ok} eq '1') {
				$retval = 1;
			} else {
				Carp::croak("ERROR: Failed update. Reason: ".$lastE->{err}.' '.$lastE->{errmsg});
			}
		} else {
			# No-op, but it must not be inserted later!
			$retval = 1;
		}
	}
	
	return $retval;
}

1;
