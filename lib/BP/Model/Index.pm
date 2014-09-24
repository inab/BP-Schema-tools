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

package BP::Model::Index;

# Indexes must be interpreted as hints, as not all the platforms support explicit index declarations

# This is an static method.
# ParseIndexes parameters:
#	container: a XML::LibXML::Element container of 'dcc:index' elements
# returns an array reference, containing BP::Model::Index instances
sub ParseIndexes($) {
	my $container = shift;
	
	# And the index declarations for this collection
	my @indexes = ();
	foreach my $ind ($container->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'index')) {
		push(@indexes,BP::Model::Index->parseIndex($ind));
	}
	
	return \@indexes;
}

# This is the constructor.
# parseIndex parameters:
#	ind: a XML::LibXML::Element which is a 'dcc:index'
# returns a BP::Model::Index instance
sub parseIndex($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $ind = shift;
	
	# Is index unique?, attributes (attribute name, ascending/descending)
	my @index = (($ind->hasAttribute('unique') && $ind->getAttribute('unique') eq 'true')?1:undef,[]);

	foreach my $attr ($ind->childNodes()) {
		next  unless($attr->nodeType == XML::LibXML::XML_ELEMENT_NODE && $attr->localname eq 'attr');
		
		push(@{$index[1]},[$attr->getAttribute('name'),($attr->hasAttribute('ord') && $attr->getAttribute('ord') eq '-1')?-1:1]);
	}

	return bless(\@index,$class);
}

# This is the constructor
# new parameters:
#	isUnique: Whether the index is unique or not
#	indexColumns: The columns, i.e. an array of pairs [column name,ascending(1)/descending(-1) ordering]
sub new($@) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $isUnique = shift;
	my @indexColumns = @_;
	
	return bless([$isUnique,\@indexColumns],$class);
}

# Is index unique?
sub isUnique {
	return $_[0]->[0];
}

# attributes (attribute name, ascending/descending)
sub indexAttributes {
	return $_[0]->[1];
}

# hasValidColumns parameters:
#	columns: A reference to an array or a hash (only the keys are used)
# It validates the attribute names against this set of column names, returning
# true if all the attributes are present in the input columns, and false otherwise
sub hasValidColumns($) {
	my $self = shift;
	
	my $p_columns = shift;
	$p_columns = { map { $_ => undef} @{$p_columns} }  if(ref($p_columns) eq 'ARRAY');
	
	foreach my $attr (@{$self->indexAttributes}) {
		return undef  unless(exists($p_columns->{$attr->[0]}));
	}
	
	return 1;
}

# This is a constructor
# relatedIndex parameters:
#	columns: A reference to a hash (keys and values are used)
# It validates the attribute names against this set of column names, returning
# a new BP::Model::Index instance 
sub relatedIndex($) {
	my $self = shift;
	
	my $p_columns = shift;
	Carp::croak("This method expects an instance of a hash of column names")  unless(ref($p_columns) eq 'HASH');
	
	my @indexAttr = map { [@{$_}] } @{$self->indexAttributes};
	my $retval = bless([$self->isUnique,\@indexAttr]);
	
	foreach my $attr (@indexAttr) {
		if(exists($p_columns->{$attr->[0]})) {
			$attr->[0] = $p_columns->{$attr->[0]};
		} else {
			$retval = undef;
			last;
		}
	}
	
	return $retval;
}

# This is a constructor
# clonePrefixed parameters:
#	prefix: The prefix to prepend to the column names.
# This method returns a cloned BP::Model::Index instance, whose
# attributes optionally have prepended the prefix given as input parameters
sub clonePrefixed($) {
	my $self = shift;
	
	my $prefix = shift;
	
	my @indexAttr = map { [@{$_}] } @{$self->indexAttributes};
	my $retval = bless([$self->isUnique,\@indexAttr]);
	
	if(defined($prefix)) {
		$prefix = $prefix . '.';
		foreach my $attr (@indexAttr) {
			$attr->[0] = $prefix . $attr->[0];
		}
	}
	
	return $retval;
}

1;
