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
use BP::Model::ColumnType;
use BP::Model::DescriptionSet;

package BP::Model::Column;

use constant {
	NAME => 0,
	DESCRIPTION => 1,
	ANNOTATIONS => 2,
	COLUMNTYPE => 3,
	ISMASKED => 4,
	REFCONCEPT => 5,
	REFCOLUMN => 6,
	RELATED_CONCEPT => 7
};

# This is the constructor.
# parseColumn parameters:
#	colDecl: a XML::LibXML::Element 'dcc:column' node, which defines
#		a column
#	model: a BP::Model instance, used to validate
# returns a BP::Model::Column instance, with all the information related to
# types, restrictions and enumerated values used by this column.
sub parseColumn($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $colDecl = shift;
	my $model = shift;
	
	# Column name, description, annotations, column type, is masked, related concept, related column from the concept
	my @column = (
		$colDecl->getAttribute('name'),
		BP::Model::DescriptionSet->parseDescriptions($colDecl),
		BP::Model::AnnotationSet->parseAnnotations($colDecl),
		BP::Model::ColumnType->parseColumnType($colDecl,$model,$colDecl->getAttribute('name')),
		undef,
		undef,
		undef,
		undef
	);
	
	return bless(\@column,$class);
}

# This is a constructor.
# new parameters:
#	name: The column name
#	description: A BP::Model::DescriptionSet instance
#	annotations: A BP::Model::AnnotationSet instance
#	columnType: A BP::Model::ColumnType instance
# returns a BP::Model::Column instance, with all the information related to
# types, restrictions and enumerated values used by this column.
sub new($$$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $name = shift;
	my $description = shift;
	$description = BP::Model::DescriptionSet->new()  unless(Scalar::Util::blessed($description));
	my $annotations = shift;
	$annotations = BP::Model::AnnotationSet->new()  unless(Scalar::Util::blessed($annotations));
	my $columnType = shift;
	
	# Column name, description, annotations, column type, is masked, related concept, related column from the concept
	my @column = (
		$name,
		$description,
		$annotations,
		$columnType,
		undef,
		undef,
		undef,
		undef
	);
	
	return bless(\@column,$class);
}

# The column name
sub name {
	return $_[0]->[BP::Model::Column::NAME];
}

# The description, a BP::Model::DescriptionSet instance
sub description {
	return $_[0]->[BP::Model::Column::DESCRIPTION];
}

# Annotations, a BP::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[BP::Model::Column::ANNOTATIONS];
}

# It returns a BP::Model::ColumnType instance
sub columnType {
	return $_[0]->[BP::Model::Column::COLUMNTYPE];
}

# If this column is masked (because it is a inherited idref on a concept hosted in a hash)
# it will return true, otherwise undef
sub isMasked {
	return $_[0]->[BP::Model::Column::ISMASKED];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a BP::Model::Concept instance
# Otherwise, it will return undef
sub refConcept {
	return $_[0]->[BP::Model::Column::REFCONCEPT];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a BP::Model::Column instance
# which correlates to
# Otherwise, it will return undef
sub refColumn {
	return $_[0]->[BP::Model::Column::REFCOLUMN];
}

# If this column is part of a foreign key pointing
# to a concept using dcc:related-to, this method will return a BP::Model::RelatedConcept
# instance which correlates to
# Otherwise, it will return undef
sub relatedConcept {
	return $_[0]->[BP::Model::Column::RELATED_CONCEPT];
}

# clone parameters:
#	doMask: optional, it signals whether to mark cloned column
#		as masked, so it should not be considered for value storage in the database.
#	prefix: optional, it is a prefix to add to the column name
#	scalarize: optional, translate into scalar
# it returns a BP::Model::Column instance
sub clone(;$$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $doMask = shift;
	my $prefix = shift;
	my $scalarize = shift;
	
	# Cloning this object
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	# Cloning the description and the annotations
	$retval->[BP::Model::Column::NAME] = $prefix.$self->name  if(defined($prefix));
	$retval->[BP::Model::Column::DESCRIPTION] = $self->description->clone;
	$retval->[BP::Model::Column::ANNOTATIONS] = $self->annotations->clone;
	
	$retval->[BP::Model::Column::ISMASKED] = ($doMask)?1:undef;
	
	if($scalarize) {
		$retval->[BP::Model::Column::COLUMNTYPE] = $self->columnType->clone(undef,1);
	}
	
	return $retval;
}

# cloneRelated parameters:
#	refConcept: A BP::Model::Concept instance, which this column is related to.
#		The kind of relation could be inheritance, or 1:N
#	relatedConcept: optional, BP::Model::RelatedConcept, which contains the prefix to be set to the name when the column is cloned
#	doMask: optional, it signals whether to mark cloned column
#		as masked, so it should not be considered for value storage in the database.
#	weakAnnotations: optional, BP::Model::AnnotationSet
# it returns a BP::Model::Column instance
sub cloneRelated($;$$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $refConcept = shift;
	my $relatedConcept = shift;
	my $prefix = defined($relatedConcept)?$relatedConcept->keyPrefix:undef;
	my $doMask = shift;
	my $weakAnnotations = shift;
	
	# Cloning this object
	my $retval = $self->clone($doMask);
	
	# Adding the prefix
	$retval->[BP::Model::Column::NAME] = $prefix.$retval->[BP::Model::Column::NAME]  if(defined($prefix) && length($prefix)>0);
	
	# Adding the annotations from the related concept
	$retval->annotations->addAnnotations($relatedConcept->annotations)  if(defined($relatedConcept));
	# And from the weak-concepts annotations
	$retval->annotations->addAnnotations($weakAnnotations)  if(defined($weakAnnotations));
	
	# And adding the relation info
	# to this column
	$retval->[BP::Model::Column::REFCONCEPT] = $refConcept;
	$retval->[BP::Model::Column::REFCOLUMN] = $self;
	$retval->[BP::Model::Column::RELATED_CONCEPT] = $relatedConcept;
	
	# Does this column become optional due the participation?
	# Does this column become an array due the arity?
	if(defined($relatedConcept) && ($relatedConcept->isPartial || $relatedConcept->arity eq 'M')) {
		# First, let's clone the concept type, to avoid side effects
		$retval->[BP::Model::Column::COLUMNTYPE] = $self->columnType->clone($relatedConcept);
	}
	
	return $retval;
}

sub derivedIndexes(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $prefix = shift;
	
	return $self->columnType->derivedIndexes($prefix,$self->name);
}

1;
