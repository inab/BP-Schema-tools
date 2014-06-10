#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;

use MongoDB 0.704.0.0;
use Config::IniFiles;
use Tie::IxHash;

package BP::Loader::Mapper::MongoDB;

# Needed for metadata model serialization
use JSON;

# Better using something agnostic than JSON::true or JSON::false inside TO_JSON
use boolean 0.32;

use base qw(BP::Loader::Mapper);

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
	['timeout' => '']
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

# These methods are called by JSON library, which gives the structure to
# be translated into JSON. They are the patches to the different
# BP::Model subclasses, so JSON-ification works without having this
# specific code in the subclasses

sub BP::Model::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# We need collections by path, not by id
	my %jsonColls = map { $_->path => $_ } values(%{$self->{COLLECTIONS}});
	
	# The main features
	my %jsonModel=(
		'project'	=> $self->{project},
		'schemaVer'	=> $self->{schemaVer},
		'annotations'	=> $self->{ANNOTATIONS},
		'collections'	=> \%jsonColls,
		'domains'	=> $self->{CDOMAINHASH},
	);
	
	return \%jsonModel;
}

sub BP::Model::Collection::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonCollection = (
		'name'	=> $self->name,
		'path'	=> $self->path,
		'indexes'	=> $self->indexes
	);
	
	return \%jsonCollection;
}

sub BP::Model::Index::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return {
		'unique'	=> boolean::boolean($self->isUnique),
		'attrs'	=> [map { { 'name' => $_->[0], 'ord' => $_->[1] } } @{$self->indexAttributes}],
	};
}

sub BP::Model::DescriptionSet::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(scalar(@{$self})>0) {
		my @arrayRef = @{$self};
		
		foreach my $val (@arrayRef) {
			if(ref($val) eq 'ARRAY') {
				$val = join('',map { (ref($_) && $_->can('toString'))?$_->toString():$_ } @{$val});
			} elsif(ref($val) && $val->can('toString')) {
				$val = $val->toString();
			}
		}
		
		return \@arrayRef;
	} else {
		return undef;
	}
}

sub BP::Model::AnnotationSet::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(scalar(keys(%{$self->hash}))>0) {
		my %hashRes = %{$self->hash};
		
		foreach my $val (values(%hashRes)) {
			if(ref($val) eq 'ARRAY') {
				$val = join('',map { (ref($_) && $_->can('toString'))?$_->toString():$_ } @{$val});
			} elsif(ref($val) && $val->can('toString')) {
				$val = $val->toString();
			}
		}
		
		return \%hashRes;
	} else {
		return undef;
	}
}

sub BP::Model::CV::Term::_jsonId() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvPrefix = defined($self->parentCV)?$self->parentCV->id:'_null_';
	
	return join(':','t',$cvPrefix,$self->key);
}

sub BP::Model::CV::Term::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %hashRes = (
		'_id'	=> $self->_jsonId,
		'term'	=> $self->key,
		'name'	=> $self->name,
	);
	
	$hashRes{'alt-id'} = $self->keys  if(scalar(@{$self->keys})>1);
	if($self->isAlias) {
		$hashRes{'alias'} = boolean::true;
		$hashRes{'union-of'} = $self->parents;
	} elsif(defined($self->parents)) {
		$hashRes{'parents'} = $self->parents;
		$hashRes{'ancestors'} = $self->ancestors;
	}
	
	return \%hashRes;
}

sub BP::Model::CV::Abstract::_jsonId() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return 'cv:'.$self->id;
}

sub BP::Model::CV::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %hashRes = (
		'_id'	=> $self->_jsonId,
		'name'	=> $self->name,
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		'terms'	=> [ values(%{$self->CV}) ]
	);
	
	return \%hashRes;
}

sub BP::Model::CV::Meta::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %hashRes = (
		'_id'	=> $self->_jsonId,
		'includes'	=> [ map { $_->_jsonId } @{$self->getEnclosedCVs} ]
	);
	
	return \%hashRes;
}

sub BP::Model::ColumnType::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonColumnType = (
		'type'	=> $self->type,
		'use'	=> $self->use,
		'isArray'	=> boolean::boolean(defined($self->arraySeps)),
	);
	
	if(defined($self->default)) {
		if(ref($self->default)) {
			$jsonColumnType{'defaultCol'} = $self->default->name;
		} else {
			$jsonColumnType{'default'} = $self->default;
		}
	}
	
	if(defined($self->restriction)) {
		if($self->restriction->isa('BP::Model::CV::Abstract')) {
			$jsonColumnType{'cv'} = $self->restriction->_jsonId;
		} elsif($self->restriction->isa('BP::Model::CompoundType')) {
			$jsonColumnType{'columns'} = $self->restriction->columnSet->columns;
		} elsif($self->restriction->isa('Pattern')) {
			$jsonColumnType{'pattern'} = $self->restriction;
		}
	}
	
	return \%jsonColumnType;
}

sub BP::Model::Column::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonColumn = (
		'name'	=> $self->name,
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		'restrictions'	=> $self->columnType
	);
	
	$jsonColumn{'refers'} = join('.',$self->refConcept->conceptDomain->name, $self->refConcept->name, $self->refColumn->name)  if(defined($self->refColumn));
	
	return \%jsonColumn;
}

sub BP::Model::ConceptDomain::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonConceptDomain = (
		'_id'	=> $self->name,
		'name'	=> $self->name,
		'fullname'	=> $self->fullname,
		'isAbstract'	=> boolean::boolean($self->isAbstract),
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		# 'filenamePattern'
		'concepts'	=> $self->conceptHash
	);
	
	return \%jsonConceptDomain;
}


sub BP::Model::Concept::_jsonId() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->id();
}

sub BP::Model::Concept::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $id = $self->_jsonId;
	my %jsonConcept = (
		'_id'	=> $id,
		'name'	=> $self->name,
		'fullname'	=> $self->fullname,
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		'columns'	=> $self->columnSet->columns,
		# TOBEFINISHED
	);
	
	$jsonConcept{'extends'} = $self->parentConcept->_jsonId   if(defined($self->parentConcept));
	$jsonConcept{'identifiedBy'} = $self->idConcept->_jsonId   if(defined($self->idConcept));
	if(scalar(@{$self->relatedConcepts})>0) {
		my %relT = map { $_->concept->_jsonId => undef } @{$self->relatedConcepts};
		$jsonConcept{'relatedTo'} = [ keys(%relT) ];
	}
	
	# Now, giving absolute _id to the columns
	#foreach my $val (values(%{$jsonConcept{'columns'}})) {
	#	$val->{'_id'} = join('.',$id,$val->name);
	#}
	
	return \%jsonConcept;
}

my $DEBUGgroupcounter = 0;

# _TO_JSON parameters:
#	val: The value to be 'json-ified' in memory
#	bsonsize: The max size of a BSON object
#	colpath: The putative collection where it is going to be stored
# It returns an array of objects
sub _TO_JSON($;$$);

sub _TO_JSON($;$$) {
	my($val,$bsonsize,$colpath)=@_;
	
	# First step
	$val = $val->TO_JSON()  if(ref($val) && UNIVERSAL::can($val,'TO_JSON'));
	
	my @results = ();
	
	if(ref($val) eq 'ARRAY') {
		# This is needed to avoid memory structures corruption
		my @newval = @{$val};
		foreach my $elem (@newval) {
			$elem = _TO_JSON($elem);
		}
		push(@results,\@newval);
		print STDERR "DEBUG: array\n"  if(defined($bsonsize));
	} elsif(ref($val) eq 'HASH') {
		# This is needed to avoid memory structures corruption
		my %newval = %{$val};
		foreach my $elem (values(%newval)) {
			$elem = _TO_JSON($elem);
		}
		
		if(defined($bsonsize) && defined($colpath)) {
			my $maxterms = 256;
			
			my $numterms = (exists($newval{terms}) && ref($newval{terms}) eq 'ARRAY')?scalar(@{$newval{terms}}):0;
			my ($insert, $ids) = (undef,undef); 
			
			($insert, $ids) = MongoDB::write_insert($colpath,[\%newval],1)  if($numterms<=$maxterms);
			print STDERR "DEBUG: BSON $DEBUGgroupcounter terms $numterms\n";
			if($numterms > $maxterms || length($insert) > $bsonsize ) {
				my $numSubs = int(($numterms > $maxterms) ? ($numterms / $maxterms) : (length($insert) / $bsonsize))+1;
				my $segsize = ($numterms > $maxterms) ? $maxterms : int($numterms / $numSubs);
				
				my $offset = 0;
				foreach my $i (0..($numSubs-1)) {
					my %i_subCV = %newval;
					my $newOffset = $offset + $segsize;
					my @terms=@{$i_subCV{terms}}[$offset..($newOffset-1)];
					$i_subCV{terms} = \@terms;
					
					if($i == 0) {
						$i_subCV{'num-segments'} = $numSubs;
						
						# Avoiding redundant information
						foreach my $key ('_id','description','annotations') {
							delete($newval{$key});
						}
					}
					
					push(@results,\%i_subCV);
					if(open(my $SUB,'>','/tmp/debug-'.$DEBUGgroupcounter.'-'.$i.'.json')) {
						print $SUB encode_json(\%i_subCV);
						close($SUB);
					}
					$offset = $newOffset;
				}
				
				print STDERR "DEBUG: fragmented hash\n";
			} else {
				push(@results,\%newval);
				print STDERR "DEBUG: hash\n";
				if(open(my $SUB,'>','/tmp/debug-'.$DEBUGgroupcounter.'.json')) {
					print $SUB encode_json(\%newval);
					close($SUB);
				}
			}
		} else {
			push(@results,\%newval);
		}
		
	} else {
		push(@results,$val);
		print STDERR "DEBUG: other\n"  if(defined($bsonsize));
	}
				
	$DEBUGgroupcounter++  if(defined($bsonsize));
	
	return wantarray? @results : $results[0];
}


# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
# It returns a reference to an array of pairs
#	[absolute paths to the generated files (based on workingDir),is essential]
sub generateNativeModel(\$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $workingDir = shift;
	
	my @generatedFiles = ();
	my $JSON = undef;
	
	my $BSONSIZE = exists($self->{_BSONSIZE})?$self->{_BSONSIZE}:undef;
	my $metacollPath = defined($self->{model}->metadataCollection)?$self->{model}->metadataCollection->path:undef;
	
	my $filePrefix = undef;
	my $fullFilePrefix = undef;
	if(defined($workingDir)) {
		# Initializing JSON serializer
		$JSON = JSON->new->convert_blessed;
		$JSON->pretty;
		
		$filePrefix = $self->{BP::Loader::Mapper::FILE_PREFIX_KEY};
		$fullFilePrefix = File::Spec->catfile($workingDir,$filePrefix);
		my $outfileJSON = $fullFilePrefix.'.json';
		
		if(open(my $JSON_H,'>:utf8',$outfileJSON)) {
			print $JSON_H $JSON->encode($self->{model});
			close($JSON_H);
			push(@generatedFiles,[$outfileJSON,1]);
		} else {
			Carp::croak("Unable to create output file $outfileJSON");
		}
	} elsif(defined($metacollPath)) {
		push(@generatedFiles,_TO_JSON($self->{model}));
	} else {
		Carp::croak("ERROR: Rejecting to generate native model objects with no destination metadata collection");
	}
	
	# Now, let's dump the used CVs
	my %cvdump = ();
	foreach my $conceptDomain (@{$self->{model}->conceptDomains}) {
		foreach my $concept (@{$conceptDomain->concepts}) {
			my $columnSet = $concept->columnSet;
			foreach my $column (values(%{$columnSet->columns})) {
				my $columnType = $column->columnType;
				# Registering CVs
				if(defined($columnType->restriction) && $columnType->restriction->isa('BP::Model::CV::Abstract')) {
					my $CV = $columnType->restriction;
					
					my $cvname = $CV->id;
					
					# Second position is the SQL type
					# Third position holds the columns which depend on this CV
					unless(exists($cvdump{$cvname})) {
						# First, the enclosed CVs
						foreach my $subCV (@{$CV->getEnclosedCVs}) {
							my $subcvname = $subCV->id;
							
							unless(exists($cvdump{$subcvname})) {
								if(defined($fullFilePrefix)) {
									my $outfilesubCVJSON = $fullFilePrefix.'-CV-'.$subcvname.'.json';
									if(open(my $JSON_CV,'>:utf8',$outfilesubCVJSON)) {
										print $JSON_CV $JSON->encode($subCV);
										close($JSON_CV);
										push(@generatedFiles,[$outfilesubCVJSON,1]);
										# If we find again this CV, we do not process it again
										$cvdump{$subcvname} = undef;
									} else {
										Carp::croak("Unable to create output file $outfilesubCVJSON");
									}
								} else {
									push(@generatedFiles,_TO_JSON($subCV,$BSONSIZE,$metacollPath));
									# If we find again this CV, we do not process it again
									$cvdump{$subcvname} = undef;
								}
							}
						}
						
						# Second, the possible meta-CV, which could have been already printed.
						unless(exists($cvdump{$cvname})) {
							if(defined($fullFilePrefix)) {
								my $outfileCVJSON = $fullFilePrefix.'-CV-'.$cvname.'.json';
								if(open(my $JSON_CV,'>:utf8',$outfileCVJSON)) {
									print $JSON_CV $JSON->encode($CV);
									close($JSON_CV);
									push(@generatedFiles,[$outfileCVJSON,1]);
									# If we find again this CV, we do not process it again
									$cvdump{$cvname} = undef;
								} else {
									Carp::croak("Unable to create output file $outfileCVJSON");
								}
							} else {
								push(@generatedFiles,_TO_JSON($CV,$BSONSIZE,$metacollPath));
								# If we find again this CV, we do not process it again
								$cvdump{$cvname} = undef;
							}
						}
					}
				}
				
			}
		}
	}
	
	return \@generatedFiles;
}

# This method returns a connection to the database
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

# _EnsureIndexes parameters:
#	coll: a MongoDB::Collection instance
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
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(ref($collection) && $collection->isa('BP::Model::Collection'));
	
	if(ref($collection->indexes) eq 'ARRAY' && scalar(@{$collection->indexes})>0) {
		my $db = $self->connect();
		my $coll = $db->get_collection($collection->path);
		_EnsureIndexes($coll,@{$collection->indexes});
	}
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
	if(defined($self->{model}->metadataCollection)) {
		my $p_generatedObjects = $self->generateNativeModel(undef);
		
		my $db = $self->connect();
	
		my $metacollPath = $self->{model}->metadataCollection->path;
		my $metacoll = $db->get_collection($metacollPath);
		
		# Let's add the needed meta-indexes for CV terms
		_EnsureIndexes($metacoll,BP::Model::Index->new(undef,['terms.term',1]),BP::Model::Index->new(undef,['terms.parents',1]),BP::Model::Index->new(undef,['terms.ancestors',1]));
		
		foreach my $p_generatedObject (@{$p_generatedObjects}) {
			$metacoll->save($p_generatedObject,{safe=>1});
		}
	}
}

# getDestination parameters:
#	correlatedConcept: An instance of BP::Loader::CorrelatableConcept
#	isTemp: should it be a temporary destination?
# It returns a MongoDB::Collection instance
sub getDestination($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	my $isTemp = shift;
	
	Carp::croak("ERROR: getDestination needs a BP::Loader::CorrelatableConcept instance")  unless(ref($correlatedConcept) && $correlatedConcept->isa('BP::Loader::CorrelatableConcept'));
	
	my $destColl = $isTemp?'TEMP_'.$correlatedConcept->concept->key.'_'.int(rand(2**32-1)):$correlatedConcept->concept->collection->path;
	my $db = $self->connect();
	
	return $db->get_collection($destColl);
}

# freeDestination parameters:
#	destination: An instance of MongoDB::Collection
#	errflag: The error flag
# As it is not needed to explicitly free them, it is an empty method.
sub freeDestination($) {
}

# bulkPrepare parameters:
#	correlatedConcept: A BP::Loader::CorrelatableConcept instance
#	entorp: The output of BP::Loader::CorrelatableConcept->readEntry
# It returns the bulkData to be used for the load
sub bulkPrepare($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	my $entorp = shift;
	
	return $entorp->[0];
}

# bulkInsert parameters:
#	destination: The destination of the bulk insertion (a MongoDB::Collection instance)
#	p_batch: a reference to an array of hashes which contain the values to store.
sub bulkInsert($\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $destination = shift;
	my $p_batch = shift;
	
	Carp::croak("ERROR: bulkInsert needs a MongoDB::Collection instance")  unless(ref($destination) && $destination->isa('MongoDB::Collection'));
	
	return $destination->batch_insert($p_batch);
}

1;
