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
use BP::Model::Concept;
use BP::Model::DescriptionSet;

package BP::Model::ConceptDomain;

# This is the constructor.
# parseConceptDomain parameters:
#	conceptDomainDecl: a XML::LibXML::Element 'dcc:concept-domain' element
#	model: a BP::Model instance used to validate the concepts, columsn, etc...
# it returns a BP::Model::ConceptDomain instance, with all the concept domain
# structures and data
sub parseConceptDomain($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && ref($class));
	
	my $conceptDomainDecl = shift;
	my $model = shift;
	
	# concept domain name
	# full name of the concept domain
	# Filename Pattern for the filenames
	# An array with the concepts under this concept domain umbrella
	# the concept hash
	# The is abstract flag
	# The descriptions
	# The annotations
	my @concepts = ();
	my %conceptHash = ();
	my @conceptDomain = (
		$conceptDomainDecl->getAttribute('domain'),
		$conceptDomainDecl->getAttribute('fullname'),
		undef,
		\@concepts,
		\%conceptHash,
		($conceptDomainDecl->hasAttribute('is-abstract') && ($conceptDomainDecl->getAttribute('is-abstract') eq 'true'))?1:undef,
		BP::Model::DescriptionSet->parseDescriptions($conceptDomainDecl),
		BP::Model::AnnotationSet->parseAnnotations($conceptDomainDecl,$model->annotations),
	);
	
	# Does the filename-pattern exist?
	my $filenameFormatName = $conceptDomainDecl->getAttribute('filename-format');
		
	my $fpattern = $model->getFilenamePattern($filenameFormatName);
	unless(defined($fpattern)) {
		Carp::croak("Concept domain $conceptDomain[0] uses the unknown filename format $filenameFormatName"."\nOffending XML fragment:\n".$conceptDomainDecl->toString()."\n");
	}
	
	$conceptDomain[2] = $fpattern;
	
	# Last, chicken and egg problem, part 1
	my $retConceptDomain = bless(\@conceptDomain,$class);

	# And now, next method handles parsing of embedded concepts
	# It must register the concepts in the concept domain as soon as possible
	BP::Model::Concept::ParseConceptContainer($conceptDomainDecl,$retConceptDomain,$model);
	
	# Last, chicken and egg problem, part 2
	# This step must be delayed, because we need a enumeration of all the concepts
	# just at this point
	$retConceptDomain->filenamePattern->registerConceptDomain($retConceptDomain);
	
	return $retConceptDomain;
}

# concept domain name
sub name {
	return $_[0]->[0];
}

# full name of the concept domain
sub fullname {
	return $_[0]->[1];
}

# Filename Pattern for the filenames
# A BP::Model::FilenamePattern instance
sub filenamePattern {
	return $_[0]->[2];
}

# An array with the concepts under this concept domain umbrella
# An array of BP::Model::Concept instances
sub concepts {
	return $_[0]->[3];
}

# A hash with the concepts under this concept domain umbrella
# A hash of BP::Model::Concept instances
sub conceptHash {
	return $_[0]->[4];
}

# It returns 1 or undef, so it tells whether the whole concept domain is abstract or not
sub isAbstract {
	return $_[0]->[5];
}

# An instance of a BP::Model::DescriptionSet, holding the documentation
# for this Conceptdomain
sub description {
	return $_[0]->[6];
}

# A BP::Model::AnnotationSet instance, with all the annotations
sub annotations {
	return $_[0]->[7];
}

# registerConcept parameters:
#	concept: a BP::Model::Concept instance, which is going to be registered in the concept domain
sub registerConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $concept = shift;
	
	push(@{$self->concepts},$concept);
	$self->conceptHash->{$concept->name} = $concept;
}

1;
