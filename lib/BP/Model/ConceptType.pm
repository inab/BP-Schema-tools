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

use BP::Model::ColumnSet;

package BP::Model::ConceptType;

# Prototypes of static methods
sub parseConceptTypeLineage($$;$);

# This is an static method.
# parseConceptTypeLineage parameters:
#	ctypeElem: a XML::LibXML::Element 'dcc:concept-type' node
#	model: a BP::Model instance where the concept type was defined
#	ctypeParent: an optional 'BP::Model::ConceptType' instance, which is the
#		parent concept type
# returns an array of 'BP::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseConceptTypeLineage($$;$) {
	my $ctypeElem = shift;
	my $model = shift;
	# Optional parameter, the conceptType parent
	my $ctypeParent = undef;
	$ctypeParent = shift  if(scalar(@_) > 0);
	
	# The returning values array
	my $me = BP::Model::ConceptType->parseConceptType($ctypeElem,$model,$ctypeParent);
	my @retval = ($me);
	
	# Now, let's find subtypes
	foreach my $subtypes ($ctypeElem->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'subtypes')) {
		foreach my $childCTypeElem ($subtypes->childNodes()) {
			next  unless($childCTypeElem->nodeType == XML::LibXML::XML_ELEMENT_NODE && $childCTypeElem->localname() eq 'concept-type');
			
			# Parse subtypes and store them!
			push(@retval,BP::Model::ConceptType::parseConceptTypeLineage($childCTypeElem,$model,$me));
		}
		last;
	}
	
	return @retval;
}

# This is the constructor.
# parseConceptType parameters:
#	ctypeElem: a XML::LibXML::Element 'dcc:concept-type' node
#	model: a BP::Model instance where the concept type was defined
#	ctypeParent: an optional 'BP::Model::ConceptType' instance, which is the
#		parent concept type
# returns an array of 'BP::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseConceptType($$;$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $ctypeElem = shift;
	my $model = shift;
	# Optional parameter, the conceptType parent
	my $ctypeParent = undef;
	$ctypeParent = shift  if(scalar(@_) > 0);
	
	# concept-type name (could be anonymous)
	# collection/key based (true,undef)
	# collection/key name (value,undef)
	# parent
	# columnSet
	# indexes
	my @ctype = (undef,undef,undef,$ctypeParent,undef,undef);
	
	# If it has name, then it has to have either a collection name or a key name
	# either inline or inherited from the ancestors
	if($ctypeElem->hasAttribute('name')) {
		$ctype[0] = $ctypeElem->getAttribute('name');
		
		if($ctypeElem->hasAttribute('collection')) {
			if($ctypeElem->hasAttribute('key')) {
				Carp::croak("A concept type cannot have a quantum storage state of physical and virtual collection"."\nOffending XML fragment:\n".$ctypeElem->toString()."\n");
			}
			
			$ctype[1] = 1;
			my $collName = $ctypeElem->getAttribute('collection');
			# Let's link the BP::Model::Collection
			my $collection = $model->getCollection($collName);
			if(defined($collection)) {
				$ctype[2] = $collection;
			} else {
				Carp::croak("Collection '$collName', used by concept type '$ctype[0]', does not exist"."\nOffending XML fragment:\n".$ctypeElem->toString()."\n");
			}
		} elsif($ctypeElem->hasAttribute('key')) {
			$ctype[2] = $ctypeElem->getAttribute('key');
		} elsif(defined($ctypeParent) && defined($ctypeParent->path)) {
			# Let's fetch the inheritance
			$ctype[1] = $ctypeParent->goesToCollection;
			$ctype[2] = $ctypeParent->path;
		} else {
			Carp::croak("A concept type must have a storage state of physical or virtual collection"."\nOffending XML fragment:\n".$ctypeElem->toString()."\n");
		}
	}
	
	# Let's parse the columns
	$ctype[4] = BP::Model::ColumnSet->parseColumnSet($ctypeElem,defined($ctypeParent)?$ctypeParent->columnSet:undef,$model);
	
	# The returning values array
	return bless(\@ctype,$class);
}

# concept-type name (could be anonymous), so it would return undef
sub name {
	return $_[0]->[0];
}

# collection/key based (true,undef)
sub goesToCollection {
	return $_[0]->[1];
}

# It can be either a BP::Model::Collection instance
# or a string
sub path {
	return $_[0]->[2];
}

# collection, a BP::Model::Collection instance
# An abstract concept type will return undef here
sub collection {
	return $_[0]->[2];
}

# The key name when this concept is stored as a value of an array inside a bigger concept
# An abstract concept type will return undef here
sub key {
	return $_[0]->[2];
}

# parent
# It returns either undef (when it has no parent),
# or a BP::Model::ConceptType instance
sub parent {
	return $_[0]->[3];
}

# columnSet
# It returns a BP::Model::ColumnSet instance, with all the column declarations
sub columnSet {
	return $_[0]->[4];
}

1;
