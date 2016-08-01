#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;

use MongoDB 0.704.0.0;
use Tie::IxHash;

use BP::Loader::Mapper::Autoload::MongoDB;
use BP::Loader::Tools;

package BP::Loader::Mapper::MongoDB;

use Scalar::Util qw(blessed);

use base qw(BP::Loader::Mapper::NoSQL);

my @DEFAULTS = (
	['db' => undef],
	['host' => undef],
	['port' => ''],
	['user' => ''],
	['pass' => ''],
	['timeout' => ''],
	['max-array-terms' => 256]
);

# This one mimics elasticsearch
use constant CUSTOM_TYPE_KEY	=>	'_type';
use constant CUSTOM_EXISTING_KEY	=>	'_c_';
use constant MONGO_ID_KEY	=>	'_id';

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
	
	if($config->SectionExists($BP::Loader::Mapper::MongoDB::SECTION)) {
		foreach my $param (@DEFAULTS) {
			my($key,$defval) = @{$param};
			
			if(defined($defval)) {
				$self->{$key} = $config->val($BP::Loader::Mapper::MongoDB::SECTION,$key,$defval);
			} elsif($config->exists($BP::Loader::Mapper::MongoDB::SECTION,$key)) {
				$self->{$key} = $config->val($BP::Loader::Mapper::MongoDB::SECTION,$key);
			} else {
				Carp::croak("ERROR: required parameter $key not found in section $BP::Loader::Mapper::MongoDB::SECTION");
			}
		}
		
	} else {
		Carp::croak("ERROR: Unable to read section $BP::Loader::Mapper::MongoDB::SECTION");
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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

# existsCollection parameters:
#	collection: a BP::Model::Collection instance
# It returns true if the collection was already created
sub existsCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $collection = shift;
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $destName = $collection->path;
	
	my $db = $self->connect();
	
	my @collNames = $db->collection_names();
	
	foreach my $collName (@collNames) {
		return 1  if($collName eq $destName);
	}
	
	return undef;
}

# getNativeDestination parameters:
#	collection: a BP::Model::Collection instance
# It returns a native collection object, to be used by bulkInsert, for instance
sub getNativeDestination($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	# MongoDB cannot create more than one text index on a given collection (sigh)
	# So, we will create a separate, compound index with all the indexable text fields
	my $textIdxDecl = Tie::IxHash->new();
	my $numTextIdx = 0;
	
	foreach my $index  (@indexes) {
		my $idxDecl = Tie::IxHash->new();
		my $numIdx = 0;
		my $prefix = $index->prefix;
		$prefix = defined($prefix) ? ($prefix.'.') : '';
		
		foreach my $p_colIdx (@{$index->indexAttributes}) {
			my $key = $prefix.$p_colIdx->[0];
			
			# We avoid stale repetitions
			if($p_colIdx->[1] eq 'text') {
				unless($textIdxDecl->EXISTS($key)) {
					$textIdxDecl->Push($key,$p_colIdx->[1]);
					$numTextIdx++;
				}
			} elsif(!$idxDecl->EXISTS($key)) {
				$idxDecl->Push($key,$p_colIdx->[1]);
				$numIdx++;
			}
		}
		$coll->ensure_index($idxDecl,{'unique'=>($index->isUnique?1:0)})  if($numIdx > 0);
	}
	
	$coll->ensure_index($textIdxDecl,{'unique' => 0})  if($numTextIdx > 0);
}

# createCollection parameters:
#	collection: A BP::Model::Collection instance
# Given a BP::Model::Collection instance, it is created, along with its indexes
sub createCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $collection = shift;
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $coll = $self->getNativeDestination($collection);
	# First, custom index on custom field
	_EnsureIndexes($coll,BP::Model::Index->new(undef,undef,[CUSTOM_TYPE_KEY => 1]));
	
	if(ref($collection->indexes) eq 'ARRAY' && scalar(@{$collection->indexes})>0) {
		_EnsureIndexes($coll,@{$collection->indexes});
	}
	
	# And now, let's fetch all the index declarations related to all the concepts being stored here
	my $colid = $collection+0;
	if(exists($self->{_colConcept}{$colid})) {
		foreach my $concept (@{$self->{_colConcept}{$colid}}) {
			my @derivedIndexes = $concept->derivedIndexes();
			
			_EnsureIndexes($coll,@derivedIndexes)  if(scalar(@derivedIndexes)>0);
			
			# TODO: deal with weak entities, and perhaps their custom _type indexes
		}
	}
	
	return $coll;
}

# queryCollection parameters:
#	collection: Either a BP::Model::Collection or a BP::Model::Concept instance
#	query_body: a native query body
# Given a BP::Model::Collection instance, it returns a MongoDB::Cursor object
# instance, with the prepared query, ready to scroll along its results
sub queryCollection($$;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $collection = shift;
	
	# Is it a concept?
	if(Scalar::Util::blessed($collection) && $collection->isa('BP::Model::Concept')) {
		$collection = $self->getCollectionFromConcept($collection);
	}
	
	Carp::croak("ERROR: Input parameter must be a collection (or a concept)")  unless(Scalar::Util::blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $coll = $self->getNativeDestination($collection);
	
	# my $query_body = shift;
	
	my $cursor = $coll->find(@_);
	return $cursor;
}

# queryConcept parameters:
#	concept: A BP::Model::Concept instance
#	query_body: a native query body
# Given a BP::Model::Concept instance, it returns a native scrolling object
# instance, with the prepared query, ready to scroll along its results
sub queryConcept($$;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $query = shift;
	
	my %augQuery = %{$query};
	
	# This is needed to filter out entries from other concepts which are in the same collection
	$augQuery{CUSTOM_TYPE_KEY()} = $concept->id();
		
	return $self->queryCollection(\%augQuery,@_);
}

# storeNativeModel parameters:
sub storeNativeModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $metadataCollection = undef;
	my $metadataExists = undef;
	if(defined($self->{model}->metadataCollection())) {
		$metadataCollection = $self->{model}->metadataCollection();
		# This must be checked before the creation of the collections
		$metadataExists = $self->existsCollection($metadataCollection);
		$metadataCollection->clearIndexes();
		# Let's add the needed meta-indexes for CV terms
		$metadataCollection->addIndexes(BP::Model::Index->new('terms',undef,['term',1]),BP::Model::Index->new('terms',undef,['parents',1]),BP::Model::Index->new('terms',undef,['ancestors',1]));
	}
	
	# First, let's create the collections and their indexes
	foreach my $collection (values(%{$self->{model}->collections})) {
		$self->createCollection($collection);
	}
	
	# Do we have to store the JSON description of the model?
	if(defined($metadataCollection) && !$metadataExists) {
		my $p_generatedObjects = $self->generateNativeModel(undef,exists($self->{_BSONSIZE})?$self->{_BSONSIZE}:undef,$self->{'max-array-terms'});
		
		my $db = $self->connect();
		my $metaColl = [$db->get_collection($metadataCollection->path),undef,undef];
		
		foreach my $p_generatedObject (@{$p_generatedObjects}) {
			$self->_bulkInsert($metaColl,$self->_bulkPrepare($p_generatedObject));
		}
	}
}

use constant {
	MONGODB_COLLECTION_COL	=>	0,
	GROUPING_COLUMNS_COL	=>	1,
	GROUPING_COLUMN_NAMES_COL	=>	2,
	INCREMENTAL_COLUMN_NAMES_COL	=>	3,
};

# It sets up the destination to be used in bulkInsert calls
# _genDestination parameters:
#	correlatedConcept: An instance of BP::Loader::CorrelatableConcept
#	isTemp: should it be a temporary destination?
# It returns a reference to a three element array:
#	a MongoDB::Collection instance
#	a list of keys corresponding to the grouping keys used for incremental updates
#	a list of key names corresponding to the grouping keys used for incremental updates
#	a list of key names corresponding to the submappings taken into account for incremental updates
sub _genDestination($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $correlatedConcept = shift;
	my $isTemp = shift;
	
	my $destColl = $isTemp?'TEMP_'.$correlatedConcept->concept->key.'_'.int(rand(2**32-1)):$correlatedConcept->concept->collection->path;
	my $db = $self->connect();
	my $destination = $db->get_collection($destColl);
	
	$self->{_conceptId} = $correlatedConcept->concept->id();
	
	return [$destination,$correlatedConcept->groupingColumns,$correlatedConcept->groupingColumnNames,$correlatedConcept->incrementalColumnNames];
}

my %ISMONGOTEXT = (
	BP::Model::ColumnType::STRING_TYPE => undef,
	BP::Model::ColumnType::TEXT_TYPE => undef,
);

# _existingEntries parameters:
#	correlatedConcept: Either a BP::Model::Concept or a BP::Loader::CorrelatableConcept instance
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the submappings taken into account for incremental updates
#	existingFile: Destination where the file is being saved
# It dumps all the values of these columns to the file, and it returns the number of lines of the file
sub _existingEntries($$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $correlatedConcept = shift;
	
	my $p_destination = shift;
	
	my $existingFile = shift;
	
	my $counter = 0;
	if(defined($p_destination->[GROUPING_COLUMNS_COL])) {
		my $destination  = $p_destination->[MONGODB_COLLECTION_COL];
		my $p_cols = $p_destination->[GROUPING_COLUMNS_COL];
		my $p_colNames = $p_destination->[GROUPING_COLUMN_NAMES_COL];
		
		my $concept = $correlatedConcept->isa('BP::Loader::CorrelatableConcept')?$correlatedConcept->concept():$correlatedConcept;
		
		#my $cursor = $destination->query->snapshot->fields({map { $_ => 1 } @{$p_colNames}});
		
		my @projections;
		my %projectionString = ();
		my $p_projectionString;
		
		foreach my $column (@{$p_cols}) {
			unless(exists($ISMONGOTEXT{$column->columnType->type})) {
				$projectionString{$column->name} = { '$substr' => [ '$'.$column->name, 0, -1 ] };
				$p_projectionString = \%projectionString;
			} else {
				$projectionString{$column->name} = 1;
			}
		}
		
		push(@projections,{'$project' => $p_projectionString})  if(defined($p_projectionString));
		
		my @qTokens = map { '$'.$_ => "\t" } @{$p_colNames};
		$qTokens[-1] = "\n";
		push(@projections,{
			'$project' => {
				+CUSTOM_EXISTING_KEY => {
					'$concat' => \@qTokens
				}
			}
		});
		
		my $cursor = $destination->aggregate(\@projections,{'cursor' => {'batchSize' => 5000}});
		
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
		
		if(open(my $EXISTING,"| ".BP::Loader::Tools::SORT." -S 50% --parallel=${BP::Loader::CorrelatableConcept::NUMCPUS} $sortColDef | '".BP::Loader::Tools::GZIP."' -9c > '$existingFile'")) {
			while(my $doc = $cursor->next) {
				$counter ++;
			#	#print STDERR "DEBUG: ",ref($doc)," ",join(',',keys(%{$doc})),"\n";
			#	#print STDERR "DEBUG: ",join(',',keys(%{$fields})),"\n";
			#	print $EXISTING join("\t",@{$doc}{+MONGO_ID_KEY,@{$p_colNames}}),"\n";
				print $EXISTING $doc->{+MONGO_ID_KEY},"\t",$doc->{+CUSTOM_EXISTING_KEY};
			}
			close($EXISTING);
		}
	}
	
	# Short circuiting this when it is empty
	if($counter==0) {
		$p_destination->[GROUPING_COLUMNS_COL] = undef;
		$p_destination->[GROUPING_COLUMN_NAMES_COL] = undef;
		$p_destination->[INCREMENTAL_COLUMN_NAMES_COL] = undef;
	}
	
	return $counter;
}

# _freeDestination parameters:
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the submappings taken into account for incremental updates
#	errflag: The error flag
# As it is not needed to explicitly free them, it is an empty method.
sub _freeDestination($$) {
}

# _bulkPrepare parameters:
#	entorp: The output of BP::Loader::CorrelatableConcept->readEntry
# It returns the bulkData to be used for the load
sub _bulkPrepare($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $entorp = shift;
	$entorp = [ $entorp ]  unless(ref($entorp) eq 'ARRAY');
	
	my $conceptId = $self->{_conceptId};
	
	# Adding the _type attribute
	foreach my $p_entry (@{$entorp}) {
		$p_entry->{+CUSTOM_TYPE_KEY} = $conceptId;
	}
	
	return $entorp;
}

# _bulkInsert parameters:
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the submappings taken into account for incremental updates
#	p_batch: a reference to an array of hashes which contain the values to store.
sub _bulkInsert($\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_destination = shift;
	
	Carp::croak("ERROR: ".(caller(0))[3]." needs an array instance")  unless(ref($p_destination) eq 'ARRAY');
	my $destination = $p_destination->[MONGODB_COLLECTION_COL];
	Carp::croak("ERROR: ".(caller(0))[3]." needs a MongoDB::Collection instance")  unless(blessed($destination) && $destination->isa('MongoDB::Collection'));
	
	my $p_batch = shift;
	
	Carp::croak("ERROR: ".(caller(0))[3]." needs an array instance")  unless(ref($p_batch) eq 'ARRAY');
	
	my $p_insertBatch = $p_batch;
	
	my $count = 0;
	if(defined($p_destination->[INCREMENTAL_COLUMN_NAMES_COL])) {
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
		my $bulk = $destination->initialize_unordered_bulk_op();
		foreach my $p_entry (@{$p_insertBatch}) {
			$bulk->insert($p_entry);
		}
		
		my $result = $bulk->execute();
		$count += $result->nInserted;
		
		#my @ids = $destination->batch_insert($p_insertBatch);
		#$count += scalar(@ids);
		#
		#my $db = $self->connect();
		#my $lastE = $db->last_error();
		#
		#unless(exists($lastE->{ok}) && $lastE->{ok} eq '1') {
		#	Carp::croak("ERROR: Failed batch insert. Reason: ".$lastE->{err}.' '.$lastE->{errmsg});
		#}
	}
	
	return $count;
}

# _incrementalUpdate parameters:
#	p_destination: An array with
#		a MongoDB::Collection instance
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the grouping keys used for incremental updates
#		a list of key names corresponding to the submappings taken into account for incremental updates
#		the concept id
#	p_entry: The entry to be incrementally updated
sub _incrementalUpdate($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_destination = shift;
	my $p_entry = shift;
	
	my $retval = undef;
	
	if(defined($p_destination->[INCREMENTAL_COLUMN_NAMES_COL]) && exists($p_entry->{BP::Loader::Mapper::COL_INCREMENTAL_UPDATE_ID})) {
		my @existingCols = ();
		my $pushed = undef;
		# Filtering out optional columns with no value
		foreach my $columnName (@{$p_destination->[INCREMENTAL_COLUMN_NAMES_COL]}) {
			if(exists($p_entry->{$columnName})) {
				push(@existingCols,$columnName);
				$pushed=1;
			}
		}
		
		if($pushed) {
			$p_destination->[MONGODB_COLLECTION_COL]->update(
				{
					+MONGO_ID_KEY	=> $p_entry->{BP::Loader::Mapper::COL_INCREMENTAL_UPDATE_ID}
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
