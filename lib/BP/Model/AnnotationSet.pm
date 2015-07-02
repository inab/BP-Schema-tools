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
#	globalAnnotationSet: an optional BP::Model::AnnotationSet,
#		used to fetch values from "global-key-ref" references
#	seedAnnotationSet: an optional BP::Model::AnnotationSet used as seed
# It returns a BP::Model::AnnotationSet hash reference, containing the contents of all the
# 'dcc:annotation' XML elements found
sub parseAnnotations($;$$) {
	my $self = shift;
	
	my $container = shift;
	my $globalAnnotationSet = shift;
	my $seedAnnotationSet = shift;
	
	# Dual instance/class method behavior
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new($seedAnnotationSet);
	}
	
	my $p_hash = $self->hash;
	my $p_order = $self->order;
	
	foreach my $annotation ($container->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'annotation')) {
		my $annotationKey = $annotation->getAttribute('key');
		
		unless(exists($p_hash->{$annotationKey})) {
			push(@{$p_order},$annotationKey);
		}
		
		if($annotation->hasAttribute('global-key-ref')) {
			my $globalKeyRef = $annotation->getAttribute('global-key-ref');
			if(defined($globalAnnotationSet) && exists($globalAnnotationSet->hash->{$globalKeyRef})) {
				$p_hash->{$annotationKey} = $globalAnnotationSet->hash->{$globalKeyRef};
			} else {
				Carp::croak("Global annotation '$globalKeyRef' used to fill in annotation '$annotationKey' does not exist!!!");
			}
		} else {
			my $value = undef;
			my @aChildren = $annotation->nonBlankChildNodes();
			foreach my $aChild (@aChildren) {
				next  unless($aChild->nodeType == XML::LibXML::XML_ELEMENT_NODE);
				if($aChild->namespaceURI() eq BP::Model::Common::dccNamespace) {
					$value = $aChild;
				} else {
					$value = \@aChildren;
				}
				last;
			}
			$p_hash->{$annotationKey} = defined($value)?$value:$annotation->textContent();
		}
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

# This method takes as input a list of strings or reference to strings,
# and it applies the substitutions to the referred annotations
sub applyAnnotations(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my @input = @_;
	
	# Processing the list
	foreach my $rvar (@input) {
		my $refvar = ref($rvar)?$rvar:\$rvar;
		
		# We want the unique replacements
		my %replacements = map { $_ => undef } $$refvar =~ /\{([^}]+)\}/g;
		
		foreach my $var (keys(%replacements)) {
			if(exists($self->hash->{$var})) {
				my $val = $self->hash->{$var};
				$$refvar =~ s/\Q{$var}\E/$val/g;
			} else {
				Carp::croak("ERROR: annotation $var does not exist");
			}
		}
	}
	
	return @input;
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
