#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;

use Search::Elasticsearch 1.12;
use Tie::IxHash;

# Better using something agnostic than JSON::true or JSON::false inside TO_JSON
use boolean 0.32;

use Scalar::Util;

use BP::Loader::Mapper::Autoload::Elasticsearch;
use BP::Loader::Tools;

package BP::Loader::Mapper::Elasticsearch;

use base qw(BP::Loader::Mapper::NoSQL);

use BP::Loader::CorrelatableConcept;


my @DEFAULTS = (
	['use_https' => 'false' ],
	['nodes' => [ 'localhost' ] ],
	['port' => '' ],
	['path_prefix' => '' ],
	['user' => '' ],
	['pass' => '' ],
	['request_timeout' => 300],
);

my %ABSTYPE2ES = (
	BP::Model::ColumnType::STRING_TYPE	=> ['string',['index' => 'not_analyzed']],
	BP::Model::ColumnType::TEXT_TYPE	=> ['string',['include_in_all' => boolean::true]],
	BP::Model::ColumnType::INTEGER_TYPE	=> ['long',undef],
	BP::Model::ColumnType::DECIMAL_TYPE	=> ['double',undef],
	BP::Model::ColumnType::BOOLEAN_TYPE	=> ['boolean',undef],
	BP::Model::ColumnType::TIMESTAMP_TYPE	=> ['date',undef],
	BP::Model::ColumnType::DURATION_TYPE	=> ['string',['index' => 'not_analyzed']],
	#BP::Model::ColumnType::COMPOUND_TYPE	=> ['object',undef],
	# By default, compound types should be treated as 'nested'
	BP::Model::ColumnType::COMPOUND_TYPE	=> ['nested',['include_in_parent' => boolean::true]],
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
	
	if($config->SectionExists($BP::Loader::Mapper::Elasticsearch::SECTION)) {
		foreach my $param (@DEFAULTS) {
			my($key,$defval) = @{$param};
			
			if(defined($defval)) {
				my @values = $config->val($BP::Loader::Mapper::Elasticsearch::SECTION,$key,$defval);
				$self->{$key} = (scalar(@values)>1)?\@values:$values[0];
			} elsif($config->exists($BP::Loader::Mapper::Elasticsearch::SECTION,$key)) {
				my @values = $config->val($BP::Loader::Mapper::Elasticsearch::SECTION,$key);
				$self->{$key} = (scalar(@values)>1)?\@values:((scalar(@values)>0)?$values[0]:undef);
			} else {
				Carp::croak("ERROR: required parameter $key not found in section $BP::Loader::Mapper::Elasticsearch::SECTION");
			}
		}
		
	} else {
		Carp::croak("ERROR: Unable to read section $BP::Loader::Mapper::Elasticsearch::SECTION");
	}
	
	# Normalizing use_https
	if(exists($self->{use_https}) && defined($self->{use_https}) && $self->{use_https} eq 'true') {
		$self->{use_https} = 1;
	} else {
		delete($self->{use_https});
	}
	
	# Normalizing userinfo
	if(exists($self->{user}) && defined($self->{user}) && length($self->{user}) > 0 && exists($self->{pass}) && defined($self->{pass})) {
		$self->{userinfo} = $self->{user} . ':' . $self->{pass};
	}
	
	# Normalizing nodes
	if(exists($self->{nodes})) {
		unless(ref($self->{nodes}) eq 'ARRAY') {
			$self->{nodes} = [split(/ *, */,$self->{nodes})];
		}
	}
	
	# We don't need the internal queue
	$self->{_queue} = undef;
	
	return $self;
}

# As there is the concept of sub-document, avoid nesting the correlated concepts
sub nestedCorrelatedConcepts {
	return undef;
}

# This method returns a connection to the database
# In this case, a Search::Elasticsearch instance
sub _connect() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my @connParams = ();
	
	foreach my $key ('use_https','port','path_prefix','userinfo','request_timeout') {
		if(exists($self->{$key}) && defined($self->{$key}) && length($self->{$key}) > 0) {
			push(@connParams,$key => $self->{$key});
		}
	}
	
	# Let's test the connection
	#my $es = Search::Elasticsearch->new(@connParams,'nodes' => $self->{nodes},serializer => 'JSON::PP');
	my $es = Search::Elasticsearch->new(@connParams,'nodes' => $self->{nodes});
	
	# Setting up the parameters to the JSON serializer
	$es->transport->serializer->JSON->convert_blessed;
	
	return $es;
}

sub _FillMapping($);

# _FillMapping parameters:
#	p_columnSet: an instance of BP::Model::ColumnSet
# It returns a reference to a hash defining a Elasticsearch mapping
sub _FillMapping($) {
	my($p_columnSet) = @_;
	
	my %mappingDesc = ();
	
	my %idColumnMap = map { $_ => undef } @{$p_columnSet->idColumnNames};
	
	foreach my $column (values(%{$p_columnSet->columns()})) {
		my $columnType = $column->columnType();
		my $columnName = $column->name();
		my $esType = $ABSTYPE2ES{$columnType->type()};
		
		my %typeDecl = defined($esType->[1]) ? @{$esType->[1]}: ();
		
		my $p_typeDecl = \%typeDecl;
		
		if($columnType->containerType==BP::Model::ColumnType::HASH_CONTAINER) {
			$p_typeDecl = {
				'dynamic'	=> boolean::true,
				'type'		=> 'nested',
				'include_in_parent'	=> boolean::true,
#				'_source'	=>	{
#					'enabled'	=>	boolean::false
#				},
				'dynamic_templates'	=> [
					{
						'template_'.$columnName => {
							'match'		=> '*',
							'mapping'	=> $p_typeDecl,
						}
					}
				]
			};
		}
		
		$typeDecl{'dynamic'} = boolean::false;	# 'strict' is too strict
		
		$typeDecl{'type'} = $esType->[0];
		
#		$typeDecl{'_source'} = {
#			'enabled'	=>	boolean::false
#		};
		
		# Is this a compound type?
		my $restriction = $columnType->restriction;
		if(Scalar::Util::blessed($restriction) && $restriction->isa('BP::Model::CompoundType')) {
			my $p_subMapping = _FillMapping($restriction->columnSet);
			@typeDecl{keys(%{$p_subMapping})} = values(%{$p_subMapping});
		} else {
			if(exists($idColumnMap{$columnName})) {
				$typeDecl{'include_in_all'} = boolean::true;
			}
			
			if(defined($columnType->default()) && !ref($columnType->default())) {
				$typeDecl{'null_value'} = $columnType->default();
			}
		}
		
		$mappingDesc{$columnName} = $p_typeDecl;
	}
	
	my $retval = {
		'_all' => {
			'enabled' => boolean::true
		}
	};
	
	$retval->{'properties'} = \%mappingDesc  if(scalar(keys(%mappingDesc))>0);

	
	return $retval;
}

# createCollection parameters:
#	collection: A BP::Model::Collection instance
# Given a BP::Model::Collection instance, it is created, along with its indexes
sub createCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $collection = shift;
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(Scalar::Util::blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $es = $self->connect();
	
	my $indexName = $collection->path;
	
	# At least, let's create the index
	$es->indices->create('index' => $indexName)  unless($es->indices->exists('index' => $indexName));
	my $colid = $collection+0;
	
	my @colConcepts = ();
	if(defined($self->{model}->metadataCollection()) && $collection==$self->{model}->metadataCollection()) {
		push(@colConcepts,BP::Model::ToConcept(),BP::Model::CV::Meta::ToConcept());
	} elsif(exists($self->{_colConcept}{$colid})) {
		@colConcepts = @{$self->{_colConcept}{$colid}};
	}
	
	if(scalar(@colConcepts) > 0) {
		foreach my $concept (@colConcepts) {
			my $conceptId = $concept->id();
			
			#$es->indices->delete_mapping('index' => $indexName,'type' => $conceptId)  if($es->indices->exists_type('index' => $indexName,'type' => $conceptId));
			#unless($es->indices->exists_type('index' => $indexName,'type' => $conceptId)) {
				# Build the mapping
				my $p_mappingDesc = _FillMapping($concept->columnSet());
				
				$es->indices->put_mapping(
					'index' => $indexName,
					'type' => $conceptId,
					'body' => {
						$conceptId => $p_mappingDesc
					}
				);
			#}
		}
	}
	
	return $indexName;
}

# existsCollection parameters:
#	collection: A BP::Model::Collection instance
# Given a BP::Model::Collection instance, it is created, along with its indexes
sub existsCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $collection = shift;
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(Scalar::Util::blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $es = $self->connect();
	
	my $indexName = $collection->path;
	
	# At least, let's create the index
	return $es->indices->exists('index' => $indexName);
}

# Trimmed down version of storeNativeModel from MongoDB
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
	}
	
	# First, let's create the collections and their indexes
	foreach my $collection (values(%{$self->{model}->collections})) {
		$self->createCollection($collection);
	}
	
	# Do we have to store the JSON description of the model?
	if(defined($metadataCollection) && !$metadataExists) {
		# Second, generate the native model
		my $p_generatedObjects = $self->generateNativeModel(undef);
		
		# Third, patch the metadata collection, so the
		# two specialized mappings for the metadata model and the controlled vocabularies
		# are generated
		my $modelConcept = BP::Model::ToConcept();
		my $modelCorrelatableConcept = BP::Loader::CorrelatableConcept->new($modelConcept);
		$self->{_conceptCol}{$modelConcept+0} = $metadataCollection;
		
		my $cvConcept = BP::Model::CV::Meta::ToConcept();
		my $cvCorrelatableConcept = BP::Loader::CorrelatableConcept->new($cvConcept);
		$self->{_conceptCol}{$cvConcept+0} = $metadataCollection;
		
		# Do create the collection in case it was not created before
		$self->createCollection($metadataCollection)  unless($self->existsCollection($metadataCollection));
        
		# Fourth, insert
		$self->setDestination($modelCorrelatableConcept,undef,1);
		foreach my $p_generatedObject (@{$p_generatedObjects}) {
			next  if(exists($p_generatedObject->{'terms'}) || exists($p_generatedObject->{'includes'}));
			
			$self->bulkInsert($p_generatedObject);
		}
		$self->freeDestination();
	
		$self->setDestination($cvCorrelatableConcept,undef,1);
		foreach my $p_generatedObject (@{$p_generatedObjects}) {
			next  unless(exists($p_generatedObject->{'terms'}) || exists($p_generatedObject->{'includes'}));
			
			$self->bulkInsert($p_generatedObject);
		}
		$self->freeDestination();
	}
}

# It sets up the destination to be used in bulkInsert calls
# _genDestination parameters:
#	correlatedConcept: An instance of BP::Loader::CorrelatableConcept or BP::Concept
#	isTemp: should it be a temporary destination?
# It returns a reference to a three element array:
#	an instance of Search::Elasticsearch::Bulk
#	a list of keys corresponding to the grouping keys used for incremental updates
#	a list of keys corresponding to the submappings taken into account for incremental updates
sub _genDestination($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $correlatedConcept = shift;
	my $isTemp = shift;
	
	my $concept = $correlatedConcept->isa('BP::Loader::CorrelatableConcept')?$correlatedConcept->concept():$correlatedConcept;
	my $conid = $concept+0;
	my $collection = exists($self->{_conceptCol}{$conid})?$self->{_conceptCol}{$conid}:undef;
	my $indexName = $collection->path();
	my $mappingName = $concept->id();
	
	my $es = $self->connect();
	my @bes_params = (
		index   => $indexName,
		type    => $mappingName,
	);
	
	push(@bes_params,'max_count' => $self->bulkBatchSize)  if($self->bulkBatchSize);
	my $bes = $es->bulk_helper(@bes_params);
	
	return [$bes,$correlatedConcept->groupingColumnNames,$correlatedConcept->incrementalColumnNames];
}

# _existingEntries parameters:
#	correlatedConcept: Either a BP::Model::Concept or a BP::Loader::CorrelatableConcept instance
#	p_destination: An array with
#		an instance of Search::Elasticsearch::Bulk
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
#	existingFile: Destination where the file is being saved
# It dumps all the values of these columns to the file, and it returns the number of lines of the file
sub _existingEntries($$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $correlatedConcept = shift;
	
	my $p_destination = shift;
	
	my $existingFile = shift;
	
	my $counter = 0;
	if(defined($p_destination->[1])) {
		my $p_colNames = $p_destination->[1];
		
		my $concept = $correlatedConcept->isa('BP::Loader::CorrelatableConcept')?$correlatedConcept->concept():$correlatedConcept;
		my $conid = $concept+0;
		my $collection = exists($self->{_conceptCol}{$conid})?$self->{_conceptCol}{$conid}:undef;
		my $indexName = $collection->path();
		my $mappingName = $concept->id();
		
		my $es = $self->connect();

		my $scroll = $es->scroll_helper(
			'index'	=> $indexName,
			'type'	=> $mappingName,
			'size'	=> 5000,
			'search_type'	=> 'scan', # With this, no sort is applied
			#'search_type'	=> 'query_and_fetch',
			'body'	=> {
				#'sort'=> [ {'chromosome' => 'asc'},{'chromosome_start' => 'asc'},{'mutated_from_allele' => 'asc'},{'mutated_to_allele' => 'asc'} ],
				# Each fetched document comes with its unique identifier
				'script_fields' => {
					'_c_' => {
						'lang' => 'groovy',
						'script' => (join('+"\t"+','_fields._id.value',map { 'doc["'.$_.'"].value' } @{$p_colNames}).'+"\n"')
					}
				},
				#'fields'=> $p_colNames,
				'fields' => [],
				'query'	=> {
					'match_all' => {}
				}
			}
		);
		
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
			until($scroll->is_finished) {
				$scroll->refill_buffer();
				my @docs = $scroll->drain_buffer();
				$counter += scalar(@docs);
				foreach my $doc (@docs) {
			#		#print STDERR "DEBUG: ",ref($doc)," ",join(',',keys(%{$doc})),"\n";
					my $fields = $doc->{fields};
			#		#print STDERR "DEBUG: ",join(',',keys(%{$fields})),"\n";
					#print $EXISTING join("\t",$doc->{_id},map { $fields->{$_}[0] } @{$p_colNames}),"\n";
					print $EXISTING $fields->{_c_}[0];
				}
			}
			close($EXISTING);
		}
		# Explicitly freeing the scroll helper
		$scroll->finish;
	}
	
	# Short circuiting this when it is empty
	if($counter==0) {
		$p_destination->[1] = undef;
		$p_destination->[2] = undef;
	}
	
	return $counter;
}

# _flush parameters:
#	destination: The destination of the bulk transfers
# It flushes the contents to the database, and by default is a no-op
sub _flush($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_destination = shift;
	
	my $destination = $p_destination->[0];
	$destination->flush();
}


# _freeDestination parameters:
#	p_destination: An array with
#		an instance of Search::Elasticsearch::Bulk
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
#	errflag: The error flag
# As it is not needed to explicitly free them, only it is assured the data is flushed.
sub _freeDestination($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_destination = shift;
	my $errflag = shift;
		
	# Double-assure last entries are flushed
	$self->_flush($p_destination);
}

# _bulkPrepare parameters:
#	entorp: The output of BP::Loader::CorrelatableConcept->readEntry (usually an array of hashes)
# It returns the bulkData to be used for the load
sub _bulkPrepare($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $entorp = shift;
	$entorp = [ $entorp ]  unless(ref($entorp) eq 'ARRAY');
	
	return $entorp;
}


# _bulkInsert parameters:
#	p_destination: An array with
#		an instance of Search::Elasticsearch::Bulk
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
#	p_batch: a reference to an array of hashes which contain the values to store.
sub _bulkInsert($\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_destination = shift;
	
	Carp::croak("ERROR: ".(caller(0))[3]." needs por p_destination an array instance")  unless(ref($p_destination) eq 'ARRAY');
	my $destination = $p_destination->[0];
	Carp::croak("ERROR: ".(caller(0))[3]." needs a Search::Elasticsearch::Bulk instance")  unless(Scalar::Util::blessed($destination) && $destination->can('index'));
	
	my $p_batch = shift;
	
	Carp::croak("ERROR: ".(caller(0))[3]." needs an array instance")  unless(ref($p_batch) eq 'ARRAY');
	
	my @insertBatch = ();
	my @updateBatch = ();
	
	if(defined($p_destination->[2])) {
		foreach my $p_entry (@{$p_batch}) {
			my $uOrder = $self->_incrementalUpdate($p_destination,$p_entry);
			if(defined($uOrder)) {
				push(@updateBatch,$uOrder);
			} else {
				my $order = {
					source => $p_entry
				};
				
				$order->{id} = $p_entry->{_id}  if(ref($p_entry) eq 'HASH' && exists($p_entry->{_id}));
				push(@insertBatch, $order);
			}
		}
	} else {
		foreach my $p_entry (@{$p_batch}) {
			my $order = {
				source => $p_entry
			};
			
			$order->{id} = $p_entry->{_id}  if(ref($p_entry) eq 'HASH' && exists($p_entry->{_id}));
			push(@insertBatch, $order);
		}
	}
	
	$destination->update(@updateBatch)  if(scalar(@updateBatch)>0);
	$destination->index(@insertBatch)  if(scalar(@insertBatch)>0);
	
	return 1;
}

# flush takes no parameter:
# It sends pending upserts to the database
sub flush() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_destination = $self->getInternalDestination;
	my $retval = undef;
	$retval = $p_destination->[0]->flush()  if($p_destination);
	
	return $retval;
}

# _incrementalUpdate parameters:
#	p_destination: An array with
#		an instance of Search::Elasticsearch::Bulk
#		a list of keys corresponding to the grouping keys used for incremental updates
#		a list of keys corresponding to the submappings taken into account for incremental updates
#	p_entry: The entry to be incrementally updated
sub _incrementalUpdate($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
			$retval = {
				id => $p_entry->{BP::Loader::Mapper::COL_INCREMENTAL_UPDATE_ID},
				lang => 'groovy',
				script => join('; ',map { 'ctx._source.'.$_.' += newdoc_'.$_ } @existingCols),
				params => {
					map { ('newdoc_'.$_) => $p_entry->{$_} } @existingCols
				}
			};
			#$p_destination->[0]->update($retval);
		}
		# No-op, but it must not be inserted later!
	}
	
	return $retval;
}

1;
