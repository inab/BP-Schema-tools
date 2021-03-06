#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;
use XML::LibXML;

package BP::Loader::Mapper::NoSQL;

# Needed for metadata model serialization
use JSON;

# Better using something agnostic than JSON::true or JSON::false inside TO_JSON
use boolean 0.32;

use Scalar::Util qw(blessed);

use base qw(BP::Loader::Mapper);

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
	
	# Finding the correspondence between collections and concepts
	# needed for type mapping
	my %colcon = ();
	my %concol = ();
	foreach my $conceptDomain (@{$model->conceptDomains}) {
		next  if($self->{release} && $conceptDomain->isAbstract());
		
		foreach my $concept (@{$conceptDomain->concepts()}) {
			my $collection;
			for(my $lookconcept = $concept , $collection = undef ; !defined($collection) && defined($lookconcept) ; ) {
				if($lookconcept->goesToCollection()) {
					# If it has a destination collection, save the collection
					$collection = $lookconcept->collection();
				} elsif(defined($lookconcept->idConcept())) {
					# If it has an identifying concept, does that concept (or one ancestor idconcept) have a destination collection
					$lookconcept = $lookconcept->idConcept();
				} else {
					$lookconcept = undef;
				}
			}
			
			if(defined($collection)) {
				# Perl hack to have something 'comparable'
				my $colid = $collection+0;
				$colcon{$colid} = []  unless(exists($colcon{$colid}));
				push(@{$colcon{$colid}}, $concept);
				
				# Perl hack to have something 'comparable'
				my $conid = $concept+0;
				$concol{$conid} = $collection;
			}
		}
	}
	
	$self->{_colConcept} = \%colcon;
	$self->{_conceptCol} = \%concol;
	
	return $self;
}

# TO_JSON methods are called by JSON library, which gives the structure to
# be translated into JSON. They are the patches to the different
# BP::Model subclasses, so JSON-ification works without having this
# specific code in the subclasses

sub BP::Model::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	# We need collections by path, not by id
	my %jsonColls = map { $_->path => $_ } values(%{$self->collections()});
	
	# The main features
	my %jsonModel=(
		'project'	=> $self->projectName(),
		'schemaVer'	=> $self->versionString(),
		'annotations'	=> $self->annotations(),
		'collections'	=> \%jsonColls,
		'domains'	=> $self->conceptDomainsHash(),
	);
	
	return \%jsonModel;
}

# ToColumnSet methods are called in order to get a column-set representation
# for the meta-something
sub BP::Model::ToColumnSet($) {
	my $model = shift;
	
	# Schema preparation
	my $dccschema = $model->schemaModel();
	
	# Model validated against the XML Schema
	my $modelDOM;
	eval {
		$modelDOM = XML::LibXML->load_xml(string => <<'EOT');
<concept-type name="fake" collection="fake" xmlns="http://www.blueprint-epigenome.eu/dcc/schema">
	<column name="project">
		<column-type item-type="string" column-kind="required"/>
	</column>
	<column name="schemaVer">
		<column-type item-type="string" column-kind="required"/>
	</column>
	<column name="annotations">
		<column-type item-type="string" column-kind="required" container-type="hash"/>
	</column>
	<column name="collections">
		<column-type item-type="compound" column-kind="required" container-type="hash">
			<compound-type>
				<column name="name">
					<column-type item-type="string" column-kind="required"/>
				</column>
				<sep>*</sep>
				<column name="path">
					<column-type item-type="string" column-kind="required"/>
				</column>
				<sep>/</sep>
				<column name="indexes">
					<column-type item-type="string" column-kind="required" container-type="set" set-seps=";"/>
				</column>
			</compound-type>
		</column-type>
	</column>
	<column name="domains">
		<column-type item-type="compound" column-kind="required" container-type="hash">
			<compound-type>
				<column name="name">
					<column-type item-type="string" column-kind="required"/>
				</column>
			</compound-type>
		</column-type>
	</column>
</concept-type>
EOT
		$dccschema->validate($modelDOM);
	};
	
	# Was there some schema validation error?
	if($@) {
		Carp::croak("Error while validating metadata model against the schema: ".$@);
	}
	
	my $modelColumnSet = BP::Model::ColumnSet->parseColumnSet($modelDOM->documentElement(),undef,$model);
	
	return $modelColumnSet;
}

# ToConcept methods are called in order to get a concept representation
# either the meta-model or the meta-controlled vocabularies

# This is to compute it only once
{
my $modelConcept = undef;	

use constant CONCEPT_META_MODEL	=>	'model';

sub BP::Model::ToConcept($) {
	my $model = shift;
	
	unless(defined($modelConcept)) {
		
		my $modelColumnSet = BP::Model::ToColumnSet($model);
		
		$modelConcept = BP::Model::Concept->new([
			CONCEPT_META_MODEL, # name
			undef, # fullname
			undef, # basetype
			undef, # concept domain
			undef, # Description Set
			BP::Model::AnnotationSet->new(), # Annotation Set
			$modelColumnSet, # ColumnSet
			undef, # identifying concept
			undef, # related conceptNames
			undef, # parent concept
			CONCEPT_META_MODEL # id: The id of this QuasiConcept
		]);
	}
	
	return $modelConcept;
}

}

sub BP::Model::Collection::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my %jsonCollection = (
		'name'	=> $self->name,
		'path'	=> $self->path,
		'indexes'	=> $self->indexes
	);
	
	return \%jsonCollection;
}

sub BP::Model::Index::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return {
		'unique'	=> boolean::boolean($self->isUnique),
		'attrs'	=> [map { { 'name' => $_->[0], 'ord' => $_->[1] } } @{$self->indexAttributes}],
	};
}

sub BP::Model::DescriptionSet::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	if(scalar(@{$self})>0) {
		my @arrayRef = @{$self};
		
		foreach my $val (@arrayRef) {
			if(ref($val) eq 'ARRAY') {
				$val = join('',map { (blessed($_) && $_->can('toString'))?$_->toString():$_ } @{$val});
			} elsif(blessed($val) && $val->can('toString')) {
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	if(scalar(keys(%{$self->hash}))>0) {
		my %hashRes = %{$self->hash};
		
		foreach my $val (values(%hashRes)) {
			if(ref($val) eq 'ARRAY') {
				$val = join('',map { (blessed($_) && $_->can('toString'))?$_->toString():$_ } @{$val});
			} elsif(blessed($val) && $val->can('toString')) {
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $cvPrefix = defined($self->parentCV)?$self->parentCV->id:'_null_';
	
	return join(':','t',$cvPrefix,$self->key);
}

sub BP::Model::CV::Term::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my %hashRes = (
		'_id'	=> $self->_jsonId,
		'term'	=> $self->key,
		'name'	=> $self->name,
	);
	
	my $uriKey = $self->uriKey;
	$hashRes{'term_uri'} = $uriKey  if(defined($uriKey));
	
	my $namespace = $self->namespace;
	$hashRes{'ns'} = $namespace->ns_uri  if(defined($namespace));
	$hashRes{'alt_id'} = [@{$self->keys},@{$self->uriKeys}];
	if($self->isAlias) {
		$hashRes{'alias'} = boolean::true;
		$hashRes{'union_of'} = $self->parents;
	} elsif(defined($self->parents)) {
		$hashRes{'parents'} = $self->parents;
		$hashRes{'ancestors'} = $self->ancestors;
	}
	
	return \%hashRes;
}

sub BP::Model::CV::Term::ToColumnSet($) {
	my $model = shift;
	
	# Schema preparation
	my $dccschema = $model->schemaModel();
	
	# Model validated against the XML Schema
	my $cvTermDOM;
	eval {
		$cvTermDOM = XML::LibXML->load_xml(string => <<'EOT');
<concept-type name="fake" collection="fake" xmlns="http://www.blueprint-epigenome.eu/dcc/schema">
	<column name="term">
		<column-type item-type="string" column-kind="required"/>
	</column>
	<column name="term_uri">
		<column-type item-type="string" column-kind="required"/>
	</column>
	<column name="name">
		<column-type item-type="string" column-kind="required"/>
	</column>
	<column name="ont">
		<column-type item-type="string" column-kind="required" container-type="set" set-seps=";"/>
	</column>
	<column name="ns">
		<column-type item-type="string" column-kind="optional"/>
	</column>
	<column name="alt_id">
		<column-type item-type="string" column-kind="required" container-type="set" set-seps=";"/>
	</column>
	<column name="alias">
		<column-type item-type="boolean" column-kind="optional"/>
	</column>
	<column name="union_of">
		<column-type item-type="string" column-kind="optional" container-type="set" set-seps=";"/>
	</column>
	<column name="parents">
		<column-type item-type="string" column-kind="optional" container-type="set" set-seps=";"/>
	</column>
	<column name="ancestors">
		<column-type item-type="string" column-kind="optional" container-type="set" set-seps=";"/>
	</column>
	
	<index unique="false">
		<attr name="ont" />
	</index>
	
	<index unique="false">
		<attr name="alt_id" />
	</index>
	
	<index unique="false">
		<attr name="term" />
	</index>
	
	<index unique="false">
		<attr name="term_uri" />
	</index>
</concept-type>
EOT
		$dccschema->validate($cvTermDOM);
	};
	
	# Was there some schema validation error?
	if($@) {
		Carp::croak("Error while validating metadata CV term against the schema: ".$@);
	}
	
	my $cvTermColumnSet = BP::Model::ColumnSet->parseColumnSet($cvTermDOM->documentElement(),undef,$model);
	
	return $cvTermColumnSet;
}

# This is to compute it only once
{

my $cvTermConcept = undef;

use constant CONCEPT_META_CV_TERM	=>	'cvterm';

sub BP::Model::CV::Term::ToConcept($) {
	my $model = shift;
	
	unless(defined($cvTermConcept)) {
		my $cvTermColumnSet = BP::Model::CV::Term::ToColumnSet($model);
		
		$cvTermConcept = BP::Model::Concept->new([
			CONCEPT_META_CV_TERM, # name
			undef, # fullname
			undef, # basetype
			undef, # concept domain
			undef, # Description Set
			BP::Model::AnnotationSet->new(), # Annotation Set
			$cvTermColumnSet, # ColumnSet
			undef, # identifying concept
			undef, # related conceptNames
			undef, # parent concept
			CONCEPT_META_CV_TERM # id: The id of this QuasiConcept
		]);
	}
	
	return $cvTermConcept;
}

}

sub BP::Model::CV::Abstract::_jsonId() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return 'cv:'.$self->id;
}

sub BP::Model::CV::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my %hashRes = (
		'_id'	=> $self->_jsonId,
		'includes'	=> [ map { $_->_jsonId } @{$self->getEnclosedCVs} ]
	);
	
	return \%hashRes;
}

sub BP::Model::CV::Meta::ToColumnSet($) {
	my $model = shift;
	
	# Schema preparation
	my $dccschema = $model->schemaModel();
	
	# Model validated against the XML Schema
	my $cvDOM;
	eval {
		$cvDOM = XML::LibXML->load_xml(string => <<'EOT');
<concept-type name="fake" collection="fake" xmlns="http://www.blueprint-epigenome.eu/dcc/schema">
	<column name="name">
		<column-type item-type="string" column-kind="optional"/>
	</column>
	<column name="descriptions">
		<column-type item-type="string" column-kind="optional" container-type="set" set-seps=";"/>
	</column>
	<column name="annotations">
		<column-type item-type="string" column-kind="required" container-type="hash"/>
	</column>
	<column name="includes">
		<column-type item-type="string" column-kind="optional" container-type="set" set-seps=";"/>
	</column>
</concept-type>
EOT
		$dccschema->validate($cvDOM);
	};
	
	# Was there some schema validation error?
	if($@) {
		Carp::croak("Error while validating metadata CV against the schema: ".$@);
	}
	
	my $cvColumnSet = BP::Model::ColumnSet->parseColumnSet($cvDOM->documentElement(),undef,$model);
	
	return $cvColumnSet;
}

# This is to compute it only once
{

my $cvConcept = undef;

use constant CONCEPT_META_CV	=>	'cv';

sub BP::Model::CV::Meta::ToConcept($) {
	my $model = shift;
	
	unless(defined($cvConcept)) {
		my $cvColumnSet = BP::Model::CV::Meta::ToColumnSet($model);
		
		$cvConcept = BP::Model::Concept->new([
			undef, # name
			undef, # fullname
			undef, # basetype
			undef, # concept domain
			undef, # Description Set
			BP::Model::AnnotationSet->new(), # Annotation Set
			$cvColumnSet, # ColumnSet
			undef, # identifying concept
			undef, # related conceptNames
			undef, # parent concept
			'cv' # id: The id of this QuasiConcept
		]);
	}
	
	return $cvConcept;
}

}

sub BP::Model::ColumnType::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my %jsonColumnType = (
		'type'	=> $self->type,
		'use'	=> $self->use,
		'isArray'	=> boolean::boolean(defined($self->arraySeps) || defined($self->setSeps)),
	);
	
	if(defined($self->default)) {
		if(ref($self->default)) {
			$jsonColumnType{'defaultCol'} = blessed($self->default)?$self->default->name:${$self->default};
		} else {
			$jsonColumnType{'default'} = $self->default;
		}
	}
	
	if(blessed($self->restriction)) {
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->id();
}

sub BP::Model::Concept::TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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

#my $DEBUGgroupcounter = 0;

# _TO_JSON parameters:
#	val: The value to be 'json-ified' in memory
#	colpath: The putative collection where it is going to be stored (optional)
#	bsonsize: The max size of a BSON object (optional)
#	maxterms: Max number of elements in an array (optional)
# It returns an array of objects
sub _TO_JSON($;$$$);

sub _TO_JSON($;$$$) {
	my($val,$colpath,$bsonsize,$maxterms)=@_;
	
	# First step
	$val = $val->TO_JSON()  if(blessed($val) && $val->can('TO_JSON') && !$val->isa('boolean'));
	
	my @results = ();
	
	if(ref($val) eq 'ARRAY') {
		# This is needed to avoid memory structures corruption
		my @newval = @{$val};
		foreach my $elem (@newval) {
			$elem = _TO_JSON($elem);
		}
		push(@results,\@newval);
		#print STDERR "DEBUG: array\n"  if(defined($bsonsize));
	} elsif(ref($val) eq 'HASH') {
		# This is needed to avoid memory structures corruption
		my %newval = %{$val};
		foreach my $elem (values(%newval)) {
			$elem = _TO_JSON($elem);
		}
		
		if(defined($colpath) && (defined($bsonsize) || defined($maxterms))) {
			my $numterms = (exists($newval{terms}) && ref($newval{terms}) eq 'ARRAY')?scalar(@{$newval{terms}}):0;
			my ($insert, $ids) = (undef,undef); 
			
			($insert, $ids) = MongoDB::write_insert($colpath,[\%newval],1)  if(defined($bsonsize) && (!defined($maxterms) || $numterms<=$maxterms));
			#print STDERR "DEBUG: BSON $DEBUGgroupcounter terms $numterms\n";
			if( (defined($maxterms) && $numterms > $maxterms) || (defined($bsonsize) && length($insert) > $bsonsize) ) {
				my $numSubs = undef;
				my $segsize = undef;
				
				if(defined($maxterms) && $numterms > $maxterms) {
					$numSubs = $numterms / $maxterms;
					$segsize = $maxterms;
				} else {
					$numSubs = length($insert) / $bsonsize;
					$segsize = int($numterms / $numSubs);
				}
				$numSubs = int($numSubs) + 1;
				
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
					#if(open(my $SUB,'>','/tmp/debug-'.$DEBUGgroupcounter.'-'.$i.'.json')) {
					#	print $SUB encode_json(\%i_subCV);
					#	close($SUB);
					#}
					$offset = $newOffset;
				}
				
				#print STDERR "DEBUG: fragmented hash\n";
			} else {
				push(@results,\%newval);
				#print STDERR "DEBUG: hash\n";
				#if(open(my $SUB,'>','/tmp/debug-'.$DEBUGgroupcounter.'.json')) {
				#	print $SUB encode_json(\%newval);
				#	close($SUB);
				#}
			}
		} else {
			push(@results,\%newval);
		}
		
	} else {
		push(@results,$val);
		#print STDERR "DEBUG: other\n"  if(defined($bsonsize));
	}
				
	#$DEBUGgroupcounter++  if(defined($bsonsize));
	
	return wantarray? @results : $results[0];
}


# hasConceptsInCollection parameters:
#	collection: A BP::Model::Collection instance
sub hasConceptsInCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $collection = shift;
	
	Carp::croak("ERROR: Input parameter must be a collection")  unless(blessed($collection) && $collection->isa('BP::Model::Collection'));
	
	my $colid = $collection+0;
	
	return exists($self->{_colConcept}{$colid});
}

# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
#	BSONSIZE: optional parameter with the max BSON size (used to partitionate on arrays)
#	maxterms: optional parameter with the max number of elements in an array (used to partitionate)
# It returns a reference to an array of pairs
#	[absolute paths to the generated files (based on workingDir),is essential]
sub generateNativeModel(\$;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $workingDir = shift;
	
	my @generatedFiles = ();
	my $JSON = undef;
	
	my $BSONSIZE = shift;
	my $maxterms = shift;
	
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
				if(blessed($columnType->restriction) && $columnType->restriction->isa('BP::Model::CV::Abstract')) {
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
									push(@generatedFiles,_TO_JSON($subCV,$metacollPath,$BSONSIZE,$maxterms));
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
								push(@generatedFiles,_TO_JSON($CV,$metacollPath,$BSONSIZE,$maxterms));
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

# getNativeIndexNameFromConcept parameters:
#	concept: A BP::Model::Concept instance
# Given a BP::Model::Concept instance, it returns a BP::Model::Collection
sub getCollectionFromConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $concept = shift;
	
	Carp::croak("ERROR: Input parameter must be a concept")  unless(Scalar::Util::blessed($concept) && $concept->isa('BP::Model::Concept'));
	
	my $conid = $concept+0;
	return exists($self->{_conceptCol}{$conid})?$self->{_conceptCol}{$conid}:undef;
}

# getNativeDestination parameters:
#	collection: a BP::Model::Collection instance
# It returns a native collection object, to be used by bulkInsert, for instance
sub getNativeDestination($) {
	Carp::croak('Unimplemented method!');
}

# createCollection parameters:
#	collection: A BP::Model::Collection instance
# Given a BP::Model::Collection instance, it is created its native correspondence,
# along with its indexes. It also returns it.
sub createCollection($) {
	Carp::croak('Unimplemented method!');
}

# existsCollection parameters:
#	collection: A BP::Model::Collection instance
# Given a BP::Model::Collection instance, it tells whether the collection was created
sub existsCollection($) {
	Carp::croak('Unimplemented method!');
}

# queryCollection parameters:
#	collection: Either a BP::Model::Collection or a BP::Model::Concept instance
#	query_body: a native query body
# Given a BP::Model::Collection instance, it returns a native scrolling object
# instance, with the prepared query, ready to scroll along its results
sub queryCollection($$;$) {
	Carp::croak('Unimplemented method!');
}

# queryConcept parameters:
#	concept: A BP::Model::Concept instance
#	query_body: a native query body
# Given a BP::Model::Concept instance, it returns a native scrolling object
# instance, with the prepared query, ready to scroll along its results
sub queryConcept($$;$) {
	Carp::croak('Unimplemented method!');
}

# _incrementalUpdate parameters:
#	destination: The destination of the bulk insertion.
#	p_entry: an entry to be updated
sub _incrementalUpdate($$) {
	Carp::croak('Unimplemented method!');
}

1;
