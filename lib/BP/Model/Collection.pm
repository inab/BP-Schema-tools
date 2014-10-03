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

use BP::Model::Index;

package BP::Model::Collection;

sub parseCollections($) {
	my $colDom = shift;
	
	my %collections = ();
	foreach my $coll ($colDom->childNodes()) {
		next  unless($coll->nodeType == XML::LibXML::XML_ELEMENT_NODE && $coll->localname eq 'collection');
		
		my $collection = BP::Model::Collection->parseCollection($coll);
		$collections{$collection->name} = $collection;
	}
	
	return \%collections;
}

# This is the constructor
sub parseCollection($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $coll = shift;
	
	# Collection name, collection path, index declarations
	my @collection = ($coll->getAttribute('name'),$coll->getAttribute('path'),BP::Model::Index::ParseIndexes($coll));
	return bless(\@collection,$class);
}

# Collection name
sub name {
	return $_[0]->[0];
}

# collection path
sub path {
	return $_[0]->[1];
}

# index declarations
# It returns a reference to an array filled with BP::Model::Index instances
sub indexes {
	return $_[0]->[2];
}

# addIndexes parameters:
#	index: one or more BP::Model::Index instance
# It adds these index declarations to the collection (needed to patch the metadata collection)
sub addIndexes(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	while(scalar(@_)>0) {
		my $index = shift;
		
		Carp::croak("ERROR: Input parameter must be an index declaration")  unless(Scalar::Util::blessed($index) && $index->isa('BP::Model::Index'));
		
		push(@{$self->[2]},$index);
	}
}

# This method is mainly here for metadata collection, where the index is more or less dynamic
sub clearIndexes() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	$self->[2] = [];
}

#my @collection = ($coll->getAttribute('name'),$coll->getAttribute('path'),[]);

1;
