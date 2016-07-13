#!/usr/bin/perl -w

use strict;

use Carp;
use XML::LibXML;
use URI;

use BP::Model::Common;
use BP::Model::CV::Common;

package BP::Model::CV::Namespace;

use constant {
	URI	=>	0,
	SHORTNAME	=>	1,
	ISDEFAULT	=>	2,
	URI_FIXED	=>	3,
};

# This is the constructor constructor
# Constructor parameters:
#	p_namespace_URI:
#	namespace_short:
#	isDefault: Is it a default namespace?
sub new($$;$) {
	# Very special case for multiple inheritance handling
	# This is the seed
	my($facet)=shift;
	my($class)=ref($facet) || $facet;
	
	my $p_namespace_URI = shift;
	$p_namespace_URI = [ $p_namespace_URI ]  unless(ref($p_namespace_URI) eq 'ARRAY');
	my $namespace_short = shift;
	my $isDefault = shift;
	my $p_namespace_URI_fixed = [ @{$p_namespace_URI} ];
	
	# Fixing the namespaces
	foreach my $namespace_URI_fixed (@{$p_namespace_URI_fixed}) {
		$namespace_URI_fixed .= '/'  unless($namespace_URI_fixed =~ /[#\/?=]$/);
	}
	
	return bless([$p_namespace_URI,$namespace_short,$isDefault,$p_namespace_URI_fixed],$class);
}

# The namespace URI
sub ns_uri {
	return wantarray ? @{$_[0]->[URI]} : $_[0]->[URI][0];
}

# It returns an array of fixed uris
sub ns_uri_fixed {
	return wantarray ? @{$_[0]->[URI_FIXED]} : $_[0]->[URI_FIXED][0];
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

# This method serializes the BP::Model::CV::Namespace instance into a OBO structure
# when the namespace is a default one
# serialize parameters:
#	O: the output file handle
sub OBOserialize($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $O = shift;
	
	# We need these
	BP::Model::CV::Common::printOboKeyVal($O,'default-namespace',$self->ns_name())  if($self->isDefaultNamespace() && defined($self->ns_name) && length($self->ns_name)>0);
	BP::Model::CV::Common::printOboKeyVal($O,'ontology',$self->ns_uri());
	
}

1;

