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

package BP::Model::AnnotationSet;

# This is the empty constructor.
#	seedAnnotationSet: an optional BP::Model::AnnotationSet used as seed
sub new(;$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $seedAnnotationSet = shift;
	
	my %annotationHash = ();
	my @annotationOrder = ();
	if(defined($seedAnnotationSet)) {
		%annotationHash = %{$seedAnnotationSet->hash};
		@annotationOrder = @{$seedAnnotationSet->order};
	}
	
	my @annotations = (\%annotationHash,\@annotationOrder);
	
	return bless(\@annotations,$class);
}

# This is the constructor.
# parseAnnotations paremeters:
#	container: a XML::LibXML::Element container of 'dcc:annotation' elements
#	seedAnnotationSet: an optional BP::Model::AnnotationSet used as seed
# It returns a BP::Model::AnnotationSet hash reference, containing the contents of all the
# 'dcc:annotation' XML elements found
sub parseAnnotations($;$) {
	my $self = shift;
	
	my $container = shift;
	my $seedAnnotationSet = shift;
	
	# Dual instance/class method behavior
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new($seedAnnotationSet);
	}
	
	my $p_hash = $self->hash;
	my $p_order = $self->order;
	
	foreach my $annotation ($container->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'annotation')) {
		unless(exists($p_hash->{$annotation->getAttribute('key')})) {
			push(@{$p_order},$annotation->getAttribute('key'));
		}
		my @aChildren = $annotation->nonBlankChildNodes();
		
		my $value = undef;
		foreach my $aChild (@aChildren) {
			next  unless($aChild->nodeType == XML::LibXML::XML_ELEMENT_NODE);
			if($aChild->namespaceURI() eq BP::Model::Common::dccNamespace) {
				$value = $aChild;
			} else {
				$value = \@aChildren;
			}
			last;
		}
		$p_hash->{$annotation->getAttribute('key')} = defined($value)?$value:$annotation->textContent();
	}
	
	return $self;
}

sub hash {
	return $_[0]->[0];
}

# The order of the keys (when they are given in a description)
sub order {
	return $_[0]->[1];
}

# This method adds a new annotation to the annotation set
sub addAnnotation($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	my $key = shift;
	my $value = shift;
	
	my $hash = $self->hash;
	push(@{$self->order},$key)  unless(exists($hash->{$key}));
	$hash->{$key} = $value;
}

# This method adds the annotations from an existing BP::Model::AnnotationSet instance
sub addAnnotations($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	my $annotations = shift;
	
	my $hash = $self->hash;
	my $annotationsHash = $annotations->hash;
	foreach my $key (@{$annotations->order}) {
		push(@{$self->order},$key)  unless(exists($hash->{$key}));
		$hash->{$key} = $annotationsHash->{$key};
	}
}

# The clone method
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my %hash = %{$self->hash};
	my @order = @{$self->order};
	my $retval = bless([\%hash,\@order],ref($self));
	
	return $retval;
}

1;
