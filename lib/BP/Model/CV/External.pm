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

package BP::Model::CV::External;

# This is the constructor
# parseCVExternal parameters:
#	el: a XML::LibXML::Element 'dcc:cv-uri' node
#	model: a BP::Model instance
sub parseCVExternal($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $el = shift;
	
	# Although it is not going to be materialized here (at least, not yet)
	# let's check whether it is a valid cv-uri
	my $cvURI = $el->textContent();
	
	# TODO: validate URI
	my @externalCV = (URI->new($cvURI),$el->getAttribute('format'),$el->hasAttribute('doc')?URI->new($el->getAttribute('doc')):undef);
	bless(\@externalCV,$class);
}

# It returns a URI object, pointing to the fetchable controlled vocabulary
sub uri {
	return $_[0]->[0];
}

# It describes the format, so validators know how to handle it
sub format {
	return $_[0]->[1];
}

# It returns a URI object, pointing to the documentation about the CV
sub docURI {
	return $_[0]->[2];
}

1;

