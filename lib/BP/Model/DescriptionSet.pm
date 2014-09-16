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

package BP::Model::DescriptionSet;

# This is the empty constructor.
sub new() {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	return bless([],$class);
}

# This is the constructor.
# parseDescriptions parameters:
#	container: a XML::LibXML::Element container of 'dcc:description' elements
# returns a BP::Model::DescriptionSet array reference, containing the contents of all
# the 'dcc:description' XML elements found
sub parseDescriptions($) {
	my $self = shift;
	
	my $container = shift;
	
	# Dual instance/class method behavior
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new();
	}
	
	foreach my $description ($container->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'description')) {
		my @dChildren = $description->nonBlankChildNodes();
		
		my $values = 0;
		# We only save the nodeset when 
		foreach my $dChild (@dChildren) {
			unless($dChild->nodeType == XML::LibXML::XML_TEXT_NODE || $dChild->nodeType == XML::LibXML::XML_CDATA_SECTION_NODE) {
				$values = undef;
				last;
			}
			
			$values++;
		}
		
		push(@{$self},defined($values)?$description->textContent():\@dChildren);
	}
	
	return $self;
}

# This method adds a new annotation to the annotation set
sub addDescription($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	my $desc = shift;

	push(@{$self},$desc);
}

# The clone method
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	return $retval;
}

1;
