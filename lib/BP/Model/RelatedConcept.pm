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

package BP::Model::RelatedConcept;

# This is the constructor.
# parseRelatedConcept parameters:
#	relatedDecl: A XML::LibXML::Element 'dcc:related-to' instance
# It returns a BP::Model::RelatedConcept instance
sub parseRelatedConcept($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $relatedDecl = shift;
	
	return bless([
		($relatedDecl->hasAttribute('domain'))?$relatedDecl->getAttribute('domain'):undef ,
		$relatedDecl->getAttribute('concept') ,
		($relatedDecl->hasAttribute('prefix'))?$relatedDecl->getAttribute('prefix'):undef ,
		undef,
		undef,
		($relatedDecl->hasAttribute('arity') && $relatedDecl->getAttribute('arity') eq 'M')?'M':1,
		($relatedDecl->hasAttribute('m-ary-sep'))?$relatedDecl->getAttribute('m-ary-sep'):',',
		($relatedDecl->hasAttribute('partial-participation') && $relatedDecl->getAttribute('partial-participation') eq 'true')?1:undef,
		($relatedDecl->hasAttribute('inheritable') && $relatedDecl->getAttribute('inheritable') eq 'true')?1:undef,
		BP::Model::AnnotationSet->parseAnnotations($relatedDecl),
		$relatedDecl->hasAttribute('id')?$relatedDecl->getAttribute('id'):undef,
	],$class);
}

sub conceptDomainName {
	return $_[0]->[0];
}

sub conceptName {
	return $_[0]->[1];
}

sub keyPrefix {
	return $_[0]->[2];
}

# It returns a BP::Model::Concept instance
sub concept {
	return $_[0]->[3];
}

# It returns a BP::Model::ColumnSet with the remote columns used for this relation
sub columnSet {
	return $_[0]->[4];
}

# It returns 1 or M
sub arity {
	return $_[0]->[5];
}

# It returns the separator
sub mArySeparator {
	return $_[0]->[6]
}

# It returns 1 or undef
sub isPartial {
	return $_[0]->[7];
}

# It returns 1 or undef
sub isInheritable {
	return $_[0]->[8];
}

# It returns a BP::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[9];
}

# The id of this relation. If returns undef, it is an anonymous relation
sub id {
	return $_[0]->[10];
}

# setRelatedConcept parameters:
#	concept: the BP::Model::Concept instance being referenced
#	columnSet: the columns inherited from the concept, already with the key prefix
sub setRelatedConcept($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $concept = shift;
	my $columnSet = shift;
	
	Carp::croak('Parameter must be either a BP::Model::Concept or undef')  unless(!defined($concept) || (Scalar::Util::blessed($concept) && $concept->isa('BP::Model::Concept')));
	Carp::croak('Parameter must be either a BP::Model::ColumnSet or undef')  unless(!defined($columnSet) || (Scalar::Util::blessed($columnSet) && $columnSet->isa('BP::Model::ColumnSet')));
	
	$self->[3] = $concept;
	$self->[4] = $columnSet;
}

# clone creates a new BP::Model::RelatedConcept
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	return $retval;
}

#	newConceptDomainName: the new concept domain to point to
sub setConceptDomainName($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $newConceptDomainName = shift;
	
	$self->[0] = $newConceptDomainName;
}

# setConceptName parameters:
#	newConceptName: the new concept name to point to
#	newConceptDomainName: the new concept domain to point to
sub setConceptName($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $newConceptName = shift;
	my $newConceptDomainName = shift;
	
	$self->[0] = $newConceptDomainName;
	$self->[1] = $newConceptName;
}

1;
