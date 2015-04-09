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
use BP::Model::CV;
use BP::Model::DescriptionSet;

package BP::Model::CV::Meta;

use base qw(BP::Model::CV::Abstract);

use constant {
	CVID		=>	0,	# The CV id (used by SQL and MongoDB uniqueness purposes)
	CVNAME		=>	1,	# The optional CV name
	CVDESC		=>	2,	# the documentation paragraphs
	CVANNOT		=>	3,	# the annotations
	CVLAX		=>	4,	# Disable checks on this CV
	CVFIRST		=>	5	# The first element from the embedded controlled vocabularies
};

# This is the empty constructor
sub new(;$$) {
	my($self)=shift;
	my($class)=ref($self) || $self;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $name = shift;
	my $isLax = shift;
	
	$self = $class->SUPER::new()  unless(ref($self));
	$self->[BP::Model::CV::Meta::CVID] = undef;
	$self->[BP::Model::CV::Meta::CVNAME] = $name;
	$self->[BP::Model::CV::Meta::CVLAX] = defined($isLax)?boolean($isLax):undef;
	$self->[BP::Model::CV::Meta::CVANNOT] = BP::Model::AnnotationSet->new();
	$self->[BP::Model::CV::Meta::CVDESC] = BP::Model::DescriptionSet->new();
	
	return $self;
}

# This is the constructor and/or the parser
# parseCV parameters:
#	container: a XML::LibXML::Element, either 'dcc:column-type' or 'dcc:meta-cv' nodes
#	model: a BP::Model instance
#	skipCVparse: skip ontology parsing (for chicken and egg cases)
# returns either a BP::Model::CV, a BP::Model::CV::Meta or a Regexp
sub parseMetaCV($$;$) {
	my $self = shift;
	
	# Dual instance/class method
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new();
	}
	
	my $container = shift;
	my $model = shift;
	my $skipCVparse = shift;
	
	my $restriction = undef;
	my $metaCVname = undef;
	my $isLax = undef;
	
	if($container->hasAttribute('name')) {
		$metaCVname = $container->getAttribute('name');
	}
	$isLax = boolean($container->hasAttribute('lax') && $container->getAttribute('lax') eq 'true')  if($container->hasAttribute('lax'));

	my @restrictChildren = $container->childNodes();
	foreach my $restrictChild (@restrictChildren) {
		next  unless($restrictChild->nodeType == XML::LibXML::XML_ELEMENT_NODE && $restrictChild->namespaceURI() eq BP::Model::Common::dccNamespace);
		
		if($restrictChild->localName eq 'cv') {
			$restriction = BP::Model::CV::Meta->new($metaCVname,$isLax)  unless(defined($restriction));
			
			$restriction->add(BP::Model::CV->parseCV($restrictChild,$model,$skipCVparse));
		} elsif($restrictChild->localName eq 'cv-ref') {
			my $namedCV = $model->getNamedCV($restrictChild->getAttribute('name'));
			if(defined($namedCV)) {
				$restriction = BP::Model::CV::Meta->new($metaCVname,$isLax)  unless(defined($restriction));
				
				$restriction->add($namedCV);
			} else {
				Carp::croak("Element cv-ref tried to use undeclared CV ".$namedCV."\nOffending XML fragment:\n".$container->toString()."\n");
			}
		} elsif($restrictChild->localName eq 'pattern') {
			$restriction = BP::Model::Common::__parse_pattern($restrictChild);
		}
	}
	
	# Only when we have a meta cv is when we look for documentation and/or annotations
	if(Scalar::Util::blessed($restriction) && $restriction->isa(__PACKAGE__)) {
		$restriction->description->parseDescriptions($container);
		$restriction->annotations->parseAnnotations($container);
	}
	
	return $restriction;
}

# This method add enclosed CVs to the meta-CV
sub add(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	# It stores only the ones which are from BP::Model::CV
	foreach my $p_cv (@_) {
		if(Scalar::Util::blessed($p_cv) && $p_cv->isa('BP::Model::CV::Abstract')) {
			#my $baseNumCV = scalar(@{$self})-(BP::Model::CV::Meta::CVFIRST);
			push(@{$self},@{$p_cv->getEnclosedCVs});
			
			#unless(defined($self->name)) {
			#	my $newNumCV = scalar(@{$self})-(BP::Model::CV::Meta::CVFIRST);
			#	
			#	# When there is only one enclosed CV, be water my friend :P
			#	if($baseNumCV==0 && $newNumCV==1) {
			#		$self->[BP::Model::CV::Meta::CVID] = $self->[BP::Model::CV::Meta::CVFIRST]->id;
			#	} elsif($baseNumCV<=1 && $newNumCV>1) {
			#		$self->[BP::Model::CV::Meta::CVID] = $self->_anonId;
			#	}
			#}
		}
	}
}

# The id
sub id() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	unless(defined($self->[BP::Model::CV::Meta::CVID])) {
		if(defined($self->name)) {
			$self->[BP::Model::CV::Meta::CVID] = $self->name;
		} elsif((scalar(@{$self})-(BP::Model::CV::Meta::CVFIRST))==1) {
			$self->[BP::Model::CV::Meta::CVID] = $self->[BP::Model::CV::Meta::CVFIRST]->id;
		} else {
			$self->[BP::Model::CV::Meta::CVID] = $self->_anonId;
		}
	}
	
	return $self->[BP::Model::CV::Meta::CVID];
}

# The name
sub name {
	return $_[0]->[BP::Model::CV::Meta::CVNAME];
}

# An instance of a BP::Model::AnnotationSet, holding the annotations
# for this CV
sub annotations {
	return $_[0]->[BP::Model::CV::Meta::CVANNOT];
}

# An instance of a BP::Model::DescriptionSet, holding the documentation
# for this CV
sub description {
	return $_[0]->[BP::Model::CV::Meta::CVDESC];
}

# lax checks?
sub isLax() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $isLax = $self->[BP::Model::CV::Meta::CVLAX];
	
	unless(defined($isLax)) {
		foreach my $p_cv (@{$self}[BP::Model::CV::Meta::CVFIRST..$#{$self}]) {
			$isLax = $p_cv->isLax();
			
			last  if($isLax);
		}
	}
	
	return $isLax;
}

# With this method a term or a term-alias is validated
# TODO: fetch on demand the CV if it is not materialized
sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $cvkey = shift;
	
	my $isValid = undef;
	foreach my $p_cv (@{$self}[BP::Model::CV::Meta::CVFIRST..$#{$self}]) {
		$isValid = $p_cv->isValid($cvkey);
		
		last  if($isValid);
	}
	
	return $isValid;
}

# A instance of BP::Model::CV::Term
sub getTerm($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $cvkey = shift;
	
	my $term = undef;
	foreach my $p_cv (@{$self}[BP::Model::CV::Meta::CVFIRST..$#{$self}]) {
		$term = $p_cv->getTerm($cvkey);
		
		last  if(defined($term));
	}
	
	return $term;
}

# It returns an array of instances of descendants of BP::Model::CV
sub getEnclosedCVs() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my @enclosedCVs = @{$self}[BP::Model::CV::Meta::CVFIRST..$#{$self}];
	
	return \@enclosedCVs;
}

# It returns an array of BP::Model::CV::External instances (or undef)
sub uri() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_enclosedCVs = $self->getEnclosedCVs();
	
	my @uris = ();
	
	foreach my $p_CV (@{$p_enclosedCVs}) {
		my $p_uris = $p_CV->uri;
		
		push(@uris,@{$p_uris})  if(ref($p_uris) eq 'ARRAY');
	}
	
	return \@uris;
}

1;
