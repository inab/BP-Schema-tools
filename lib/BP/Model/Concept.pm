#!/usr/bin/perl -W

use strict;

use Carp;
use File::Basename;
use File::Copy;
use File::Spec;
use IO::File;
use XML::LibXML;
use Encode;
use Digest::SHA1;
use URI;
use Archive::Zip;
use Archive::Zip::MemberRead;
use Scalar::Util;

use BP::Model::Common;

use BP::Model::AnnotationSet;
use BP::Model::ColumnSet;
use BP::Model::DescriptionSet;
use BP::Model::RelatedConcept;

package BP::Model::Concept;

# constants of the different elements inside the instance
use constant {
	C_NAME	=>	0,
	C_FULLNAME	=>	1,
	C_BASECONCEPTTYPES	=>	2,
	C_CONCEPTDOMAIN	=>	3,
	C_DESCRIPTION	=>	4,
	C_ANNOTATIONS	=>	5,
	C_COLUMNSET	=>	6,
	C_IDCONCEPT	=>	7,
	C_RELATEDCONCEPTS	=>	8,
	C_PARENTCONCEPT	=>	9,
	C_ID	=>	10
};

# Prototypes of static methods
sub ParseConceptContainer($$$;$);

# Static method
# ParseConceptContainer parameters:
#	conceptContainerDecl: A XML::LibXML::Element 'dcc:concept-domain'
#		or 'dcc:weak-concepts' instance
#	conceptDomain: A BP::Model::ConceptDomain instance, where this concept
#		has been defined.
#	model: a BP::Model instance used to validate the concepts, columns, etc...
#	idConcept: An optional, identifying BP::Model::Concept instance of
#		all the (weak) concepts to be parsed from the container
# it returns an array of BP::Model::Concept instances, which are all the
# concepts and weak concepts inside the input concept container
sub ParseConceptContainer($$$;$) {
	my $conceptContainerDecl = shift;
	my $conceptDomain = shift;
	my $model = shift;
	my $idConcept = shift;	# This is optional (remember!)
	
	# Let's get the annotations inside the concept container
	my $weakAnnotations = BP::Model::AnnotationSet->parseAnnotations($conceptContainerDecl,$model->annotations);
	foreach my $conceptDecl ($conceptContainerDecl->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'concept')) {
		# Concepts self register on the concept domain!
		my $concept = BP::Model::Concept->parseConcept($conceptDecl,$conceptDomain,$model,$idConcept,$weakAnnotations);
		
		# There should be only one!
		foreach my $weakContainerDecl ($conceptDecl->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'weak-concepts')) {
			BP::Model::Concept::ParseConceptContainer($weakContainerDecl,$conceptDomain,$model,$concept);
			last;
		}
	}
}

# The concepts created using this constructor can be quasiconcepts:
#	they are not real concepts. It is only an object with id and columnSet methods
# It is only needed by Elasticsearch (and others) metadata generation

# Constructor parameters in the array
#	name
#	fullname
#	basetype
#	concept domain
#	Description Set
#	Annotation Set
#	ColumnSet
#	identifying concept
#	related conceptNames
#	parent concept
#	id: The id of this QuasiConcept
sub new(\@) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	my $p_thisConcept = shift;
	Carp::croak("ERROR: Input parameter must be an array")  unless(ref($p_thisConcept) eq 'ARRAY');
	
	return bless($p_thisConcept, $class);
}

# This is the constructor
# parseConcept paramereters:
#	conceptDecl: A XML::LibXML::Element 'dcc:concept' instance
#	conceptDomain: A BP::Model::ConceptDomain instance, where this concept
#		has been defined.
#	model: a BP::Model instance used to validate the concepts, columsn, etc...
#	idConcept: An optional, identifying BP::Model::Concept instance of
#		the concept to be parsed from conceptDecl
#	weakAnnotations: The weak annotations for the columns of the identifying concept
# it returns an array of BP::Model::Concept instances, the first one
# corresponds to this concept, and the other ones are the weak-concepts
sub parseConcept($$$;$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $conceptDecl = shift;
	my $conceptDomain = shift;
	my $model = shift;
	my $idConcept = shift;	# This is optional (remember!)
	my $weakAnnotations = shift;	# This is also optional (remember!)

	my $conceptName = $conceptDecl->getAttribute('name');
	my $conceptFullname = $conceptDecl->getAttribute('fullname');
	
	my $parentConceptDomainName = undef;
	my $parentConceptName = undef;
	my $parentConceptDomain = undef;
	my $parentConcept = undef;
	
	# There must be at most one
	foreach my $baseConcept ($conceptDecl->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'extends')) {
		$parentConceptDomainName = $baseConcept->hasAttribute('domain')?$baseConcept->getAttribute('domain'):undef;
		$parentConceptName = $baseConcept->getAttribute('concept');
		
		$parentConceptDomain = $conceptDomain;
		if(defined($parentConceptDomainName)) {
			$parentConceptDomain = $model->getConceptDomain($parentConceptDomainName);
			Carp::croak("Concept domain $parentConceptDomainName with concept $parentConceptName does not exist!"."\nOffending XML fragment:\n".$baseConcept->toString()."\n")  unless(defined($parentConceptDomain));
		} else {
			# Fallback name
			$parentConceptDomainName = $parentConceptDomain->name;
		}
		
		Carp::croak("Concept $parentConceptName does not exist in concept domain ".$parentConceptDomainName."\nOffending XML fragment:\n".$baseConcept->toString()."\n")  unless(exists($parentConceptDomain->conceptHash->{$parentConceptName}));
		$parentConcept = $parentConceptDomain->conceptHash->{$parentConceptName};
		last;
	}
	
	my @conceptBaseTypes = ();
	push(@conceptBaseTypes,@{$parentConcept->baseConceptTypes})  if(defined($parentConcept));
	
	# Now, let's get the base concept types
	my @baseConceptTypesDecl = $conceptDecl->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'base-concept-type');
	Carp::croak("Concept $conceptFullname ($conceptName) has no base type (no dcc:base-concept-type)!"."\nOffending XML fragment:\n".$conceptDecl->toString()."\n")  if(scalar(@baseConceptTypesDecl)==0 && !defined($parentConcept));

	my @basetypes = ();
	foreach my $baseConceptTypeDecl (@baseConceptTypesDecl) {
		my $basetypeName = $baseConceptTypeDecl->getAttribute('name');
		my $basetype = $model->getConceptType($basetypeName);
		Carp::croak("Concept $conceptFullname ($conceptName) is based on undefined base type $basetypeName"."\nOffending XML fragment:\n".$baseConceptTypeDecl->toString()."\n")  unless(defined($basetype));
		
		foreach my $conceptBase (@conceptBaseTypes) {
			if($conceptBase eq $basetype) {
				$basetype = undef;
				last;
			}
		}
		
		# Only saving new basetypes, skipping old ones to avoid strange effects
		if(defined($basetype)) {
			push(@basetypes,$basetype);
			push(@conceptBaseTypes,$basetype);
		}
	}
	# First, let's process the basetypes columnSets
	my $baseColumnSet = (scalar(@basetypes) > 1)?BP::Model::ColumnSet->combineColumnSets(1,map { $_->columnSet } @basetypes):((scalar(@basetypes) == 1)?$basetypes[0]->columnSet:undef);
	
	# And now, let's process the inherited columnSets, along with the basetypes ones
	if(defined($baseColumnSet)) {
		# Let's combine!
		if(defined($parentConcept)) {
			# Restrictive mode, with croaks
			$baseColumnSet = BP::Model::ColumnSet->combineColumnSets(undef,$parentConcept->columnSet,$baseColumnSet);
		}
	} elsif(defined($parentConcept)) {
		# Should we clone this, to avoid potentian side effects?
		$baseColumnSet = $parentConcept->columnSet;
	} else {
		# This shouldn't happen!!
		Carp::croak("No concept types and no parent concept for $conceptFullname ($conceptName)"."\nOffending XML fragment:\n".$conceptDecl->toString()."\n");
	}
	
	# Preparing the columns
	my $columnSet = BP::Model::ColumnSet->parseColumnSet($conceptDecl,$baseColumnSet,$model);
	
	# Adding the ones from the identifying concept
	# (and later from the related stuff)
	$columnSet->addColumns($idConcept->idColumns(! $idConcept->goesToCollection,$weakAnnotations),1)  if(defined($idConcept));
	
	# This array will contain the names of the related concepts
	my @related = ();
	
	# This hash will contain the named related concepts
	my %relPos = ();
	
	# Let's resolve inherited relations topic
	if(defined($parentConcept)) {
		# First, cloning
		@related = map { $_->clone() } @{$parentConcept->relatedConcepts};
		
		# Second, reparenting the RelatedConcept objects
		# Third, updating inheritable related concepts
		my $parentDomainChanges = $parentConceptDomain ne $conceptDomain;
		my $pos = 0;
		foreach my $relatedConcept (@related) {
			# Setting the name of the concept domain, in the cases where it was relative
			$relatedConcept->setConceptDomainName($parentConceptDomainName)  if($parentDomainChanges && !defined($relatedConcept->conceptDomainName));
			
			# Resetting the name of the related concept
			my $relatedConceptDomainName = defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$parentConceptDomainName;
			if($relatedConcept->isInheritable && $relatedConcept->conceptName eq $parentConceptName && $relatedConceptDomainName eq $parentConceptDomainName) {
				$relatedConcept->setConceptName($conceptName);
			}
			
			# Registering it (if it could be substituted)
			$relPos{$relatedConcept->id} = $pos  if(defined($relatedConcept->id));
			$pos ++;
		}
		
	}
	
	# Saving the related concepts (the ones explicitly declared within this concept)
	foreach my $relatedDecl ($conceptDecl->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'related-to')) {
		my $parsedRelatedConcept = BP::Model::RelatedConcept->parseRelatedConcept($relatedDecl,$model->annotations);
		if(defined($parsedRelatedConcept->id) && exists($relPos{$parsedRelatedConcept->id})) {
			# TODO Validation of a legal substitution
			$related[$relPos{$parsedRelatedConcept->id}] = $parsedRelatedConcept;
		} else {
			push(@related,$parsedRelatedConcept);
		}
	}
	
	# Let's resolve the default values which depend on the context (i.e. the value of other columns)
	$columnSet->resolveDefaultCalculatedValues();
	
	# name
	# fullname
	# basetype
	# concept domain
	# Description Set
	# Annotation Set
	# ColumnSet
	# identifying concept
	# related conceptNames
	# parent concept
	# id
	my @thisConcept = (
		$conceptName,
		$conceptFullname,
		\@conceptBaseTypes,
		$conceptDomain,
		BP::Model::DescriptionSet->parseDescriptions($conceptDecl),
		BP::Model::AnnotationSet->parseAnnotations($conceptDecl,$model->annotations,defined($parentConcept)?$parentConcept->annotations:undef),
		$columnSet,
		$idConcept,
		\@related,
		$parentConcept,
		undef
	);
	
	my $me = $class->new(\@thisConcept);
	
	# Registering on our concept domain
	$conceptDomain->registerConcept($me);
	
	# The weak concepts must be processed outside (this constructor does not mind them)
	return  $me;
}

# name
sub name {
	return $_[0]->[C_NAME];
}

# fullname
sub fullname {
	return $_[0]->[C_FULLNAME];
}

# A reference to the array of BP::Model::ConceptType instances basetypes
sub baseConceptTypes {
	return $_[0]->[C_BASECONCEPTTYPES];
}

# The BP::Model::ConceptType instance basetype is the first element of the array
# It returns undef if there is no one
sub baseConceptType {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_arr = $self->baseConceptTypes;
	return (scalar(@{$p_arr})>0)?$p_arr->[0]:undef;
}

# It tells whether the concept data should go to a collection or not
sub goesToCollection {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $baseConceptType = $self->baseConceptType;
	return defined($baseConceptType) ? $baseConceptType->goesToCollection : undef;
}

# If goesToCollection is true, it returns a BP::Model::Collection instance
sub collection {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $baseConceptType = $self->baseConceptType;
	return (defined($baseConceptType) && $baseConceptType->goesToCollection) ? $baseConceptType->collection : undef;
}

# If goesToCollection is undef, it returns a string with the key
sub key {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $baseConceptType = $self->baseConceptType;
	return (defined($baseConceptType) && !$baseConceptType->goesToCollection) ? $baseConceptType->key : undef;
}

# The BP::Model::ConceptDomain instance where this concept is defined
sub conceptDomain {
	return $_[0]->[C_CONCEPTDOMAIN];
}

# A BP::Model::DescriptionSet instance, with all the descriptions
sub description {
	return $_[0]->[C_DESCRIPTION];
}

# A BP::Model::AnnotationSet instance, with all the annotations
sub annotations {
	return $_[0]->[C_ANNOTATIONS];
}

# A BP::Model::ColumnSet instance with all the columns (including the inherited ones) of this concept
sub columnSet {
	return $_[0]->[C_COLUMNSET];
}

# A BP::Model::Concept instance, which represents the identifying concept of this one
sub idConcept {
	return $_[0]->[C_IDCONCEPT];
}

# related conceptNames, an array of BP::Model::RelatedConcept (trios concept domain name, concept name, prefix)
sub relatedConcepts {
	return $_[0]->[C_RELATEDCONCEPTS];
}

# A BP::Model::Concept instance, which represents the concept which has been extended
sub parentConcept {
	return $_[0]->[C_PARENTCONCEPT];
}

# refColumns parameters:
#	relatedConcept: The BP::Model::RelatedConcept instance which rules this (with the optional prefix to put on cloned idref columns)
# It returns a BP::Model::ColumnSet instance with clones of all the idref columns
# referring to this object and with a possible prefix.
sub refColumns($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $relatedConcept = shift;
	
	return $self->columnSet->relatedColumns($self,$relatedConcept);
}

# idColumns parameters:
#	doMask: Are the columns masked for storage?
#	weakAnnotations: BP::Model::AnnotationSet from weak-concepts
sub idColumns(;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $doMask = shift;
	my $weakAnnotations = shift;
	
	return $self->columnSet->idColumns($self,$doMask,$weakAnnotations);
}

sub id() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $id = defined($self->[C_ID])?$self->[C_ID]:join('.',$self->conceptDomain->name, $self->name);
	
	return $id;
}

# It returns an array of derived indexes
sub derivedIndexes() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->columnSet->derivedIndexes();
}

# validateAndEnactInstances parameters:
# It validates the correctness of the entries in entorp, and it fills in-line the default values
sub validateAndEnactInstances(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $entorp = undef;
	my $foundNull = undef;
	my @entries = @_;
	
	if(scalar(@entries) > 0) {
		if(scalar(@entries)>1 || ref($entries[0]) ne 'ARRAY') {
			$entorp = \@entries;
		} else {
			$entorp = $entries[0];
		}
		
		#Carp::croak((caller(0))[3].' expects an array!')  unless(ref($entorp) eq 'ARRAY');
		my @resEntOrp=();
		$foundNull = $self->columnSet->checkerEnactor($entorp,\@resEntOrp);
		$entorp = \@resEntOrp  if(scalar(@resEntOrp)>0);
	}

	return wantarray?($entorp,$foundNull):$entorp;
}

# fakeValidateAndEnactInstances parameters:
# It neither does validation nor fills in-line the default values
sub fakeValidateAndEnactInstances(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $entorp = undef;
	my $foundNull = undef;
	my @entries = @_;
	
	if(scalar(@entries) > 0) {
		if(scalar(@entries)>1 || ref($entries[0]) ne 'ARRAY') {
			$entorp = \@entries;
		} else {
			$entorp = $entries[0];
		}
		
		foreach my $entry (@{$entorp}) {
			unless(defined($entry)) {
				$foundNull = 1;
				next;
			}
		}
	}

	return wantarray?($entorp,$foundNull):$entorp;
}

1;
