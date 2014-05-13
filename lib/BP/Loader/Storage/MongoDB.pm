#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;

use MongoDB;
use Config::IniFiles;

package BP::Loader::Storage::MongoDB;

# Needed for JSON::true and JSON::false declarations
use JSON;

use base qw(BP::Loader::Storage);

our $SECTION;

BEGIN {
	$SECTION = 'mongodb';
	$BP::Loader::Storage::storage_names{$SECTION}=__PACKAGE__;
};

my @DEFAULTS = (
	[BP::Loader::Storage::FILE_PREFIX_KEY => 'model'],
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
		'unique'	=> $self->isUnique ? JSON::true : JSON::false,
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

sub BP::Model::CV::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# TOBESTARTED
	# TOBEFINISHED
	
}

sub BP::Model::ColumnType::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonColumnType = (
		'type'	=> $self->type,
		'use'	=> $self->use,
		'isArray'	=> defined($self->arraySeps) ? JSON::true : JSON::false,
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
		'isAbstract'	=> $self-> isAbstract ? JSON::true : JSON::false,
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		# 'filenamePattern'
		'concepts'	=> $self->conceptHash
	);
	
	return \%jsonConceptDomain;
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




# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
# It returns a list of relative paths to the generated files
sub generateNativeModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $workingDir = shift;
	
	my $filePrefix = $self->{BP::Loader::Storage::FILE_PREFIX_KEY};
	my $fullFilePrefix = File::Spec->catfile($workingDir,$filePrefix);
	my $outfileJSON = $fullFilePrefix.'.json';
	
	if(open(my $JSON_H,'>:utf8',$outfileJSON)) {
		my $JSON = JSON->new->convert_blessed;
		$JSON->pretty;
		print $JSON_H $JSON->encode($self->{model});
		close($JSON_H);
	} else {
		Carp::croak("Unable to create output file $outfileJSON");
	}
	
	return [$outfileJSON];
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
	
	# We want MongoDB to return booleans as booleans, not as integers
	$MongoDB::BSON::use_boolean = 1;
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
