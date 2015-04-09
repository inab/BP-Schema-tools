#!/usr/bin/perl -W

use strict;

use Carp;
use XML::LibXML;
use URI;

use BP::Model::Common;

use File::Spec;
use LWP::UserAgent;

package BP::Model::CV::External;

# This is the constructor
# parseCVExternal parameters:
#	el: a XML::LibXML::Element 'dcc:cv-uri' node
#	defName: a default filename
sub parseCVExternal($;$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $el = shift;
	my $defName = shift;
	
	# Although it is not going to be materialized here (at least, not yet)
	# let's check whether it is a valid cv-uri
	my $cvURI = $el->textContent();
	
	# TODO: validate URI
	my $uri = URI->new($cvURI);
	my @segments = $uri->path_segments();
	my $name = $el->hasAttribute('name')?URI->new($el->getAttribute('name')):$defName;
	my $mirrorname = $segments[-1];
	$mirrorname = $name  unless(defined($mirrorname) && $mirrorname ne '');
	
	my @externalCV = ($uri,$el->getAttribute('format'),$el->hasAttribute('doc')?URI->new($el->getAttribute('doc')):undef,$name,$mirrorname);
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

# It returns a symbolic name, if available
sub name {
	return $_[0]->[3];
}

# It returns a mirror name, if possible
sub mirrorname {
	return $_[0]->[4];
}

# mirror the remote uri
#	workdir: working directory where to fetch it
# It returns the local path of the mirrored content
sub mirrorURI($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $workdir = shift;
	
	my $ua = LWP::UserAgent->new();
	
	my $name = $self->mirrorname;
	
	# Generating a fake name
	$name = time().'-'.int(rand(65536))  unless(defined($name) && $name ne '');
	
	my $destfile = File::Spec->catfile($workdir,$name);
	
	my $res = $ua->mirror($self->uri,$destfile);
	if($res->code >= 400) {
		Carp::croak("ERROR: Unable to mirror ".$self->uri." in $destfile. Reason: ".$res->error_as_html);
	}
	
	return $destfile;
}

1;

