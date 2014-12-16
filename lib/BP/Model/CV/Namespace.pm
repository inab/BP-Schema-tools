#!/usr/bin/perl -w

use strict;

use Carp;
use XML::LibXML;
use URI;

use BP::Model::Common;

package BP::Model::CV::Namespace;

use constant {
	URI	=>	0,
	SHORTNAME	=>	1,
	ISDEFAULT	=>	2,
};

# This is the constructor constructor
sub new($$;$) {
	# Very special case for multiple inheritance handling
	# This is the seed
	my($facet)=shift;
	my($class)=ref($facet) || $facet;
	
	my $namespace_URI = shift;
	my $namespace_short = shift;
	
	return bless([$namespace_URI,$namespace_short,undef],$class);
}

# The namespace URI
sub ns_uri {
	return $_[0]->[URI];
}

# The namespace short name
sub ns_name {
	return $_[0]->[SHORTNAME];
}

# Is this a default namespace?
sub isDefaultNamespace {
	return $_[0]->[ISDEFAULT];
}


# Label this namespace as default
sub setDefaultNamespace {
	$_[0]->[ISDEFAULT]=1;
}


1;

