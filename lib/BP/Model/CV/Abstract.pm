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
use BP::Model::ColumnType::Common;

package BP::Model::CV::Abstract;

# The anonymous controlled vocabulary counter, used for anonymous id generation
my $_ANONCOUNTER = 0;

# This is the empty constructor
sub new() {
	# Very special case for multiple inheritance handling
	# This is the seed
	my($facet)=shift;
	my($class)=ref($facet) || $facet;
	
	return bless([],$class);
}

sub _anonId() {
	my $anonId = '_anonCV_'.$_ANONCOUNTER;
	$_ANONCOUNTER++;
	
	return $anonId;
}

sub id() {
	Carp::croak("Unimplemented method!");
}

sub name() {
	Carp::croak("Unimplemented method!");
}

sub isLax() {
	Carp::croak("Unimplemented method!");
}

# An instance of a BP::Model::AnnotationSet, holding the annotations
# for this CV
sub annotations {
	Carp::croak("Unimplemented method!");
}

# An instance of a BP::Model::DescriptionSet, holding the documentation
# for this CV
sub description {
	Carp::croak("Unimplemented method!");
}

# With this method a term or a term-alias is validated
sub isValid($) {
	Carp::croak("Unimplemented method!");
}

# A instance of BP::Model::CV::Term
sub getTerm($) {
	Carp::croak("Unimplemented method!");
}

# It returns an array of BP::Model::CV::External instances
sub uri() {
	Carp::croak("Unimplemented method!");
}

# mirror remote uris
#	workdir: working directory where to fetch them
# It returns an array of pairs (uri, localFile)
sub mirrorURIs($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_uris = $self->uri;
	
	Carp::croak((caller(0))[3].' can be only called on CVs with URIs!')  unless(ref($p_uris) eq 'ARRAY');
	
	my $workdir = shift;
	
	my @mirrored = ();
	
	foreach my $p_ext (@{$p_uris}) {
		push(@mirrored,[$p_ext,$p_ext->mirrorURI($workdir)]);
	}
	
	return \@mirrored;
}

# With this method a reference to a validator is given
sub dataChecker {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->isLax() ? \&BP::Model::ColumnType::__true : sub($) {
		$self->isValid($_[0]);
	};
}

# With this method, it is possible to normalize the CVs
sub dataMangler($) {
	my $self = shift;
	
	my $doNormalize = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return (!$doNormalize || $self->isLax()) ? undef :
		sub($) {
			my $term = $self->getTerm($_[0]);
			my $retval = $term->uriKey();
			return defined($retval) ? $retval : $term->key();
		};
}

# It returns an array of instances of descendants of BP::Model::CV
sub getEnclosedCVs() {
	Carp::croak("Unimplemented method!");
}

1;
