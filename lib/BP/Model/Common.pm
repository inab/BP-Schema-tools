#!/usr/bin/perl -W

use strict;

use Carp;
use IO::File;
use XML::LibXML;
use Scalar::Util;

package BP::Model::Common;

use constant dccNamespace => 'http://www.blueprint-epigenome.eu/dcc/schema';

sub __parse_pattern($;$);

sub parsePatterns($);

# __parse_pattern parameters:
#	pattern: a XML::LibXML::Element 'dcc:pattern' node
#	localname: an optional local name given to this
#		pattern, used when an error is signaled.
# returns a Regexp instance, which has been extracted from the input node
sub __parse_pattern($;$) {
	my($pattern,$localname)=@_;
	
	$localname='(anonymous)'  unless(defined($localname));
	
	my($pat)=undef;

	if($pattern->hasAttribute('match')) {
		$pat=$pattern->getAttribute('match');
		eval {
			$pat = qr/$pat/;
		};

		if($@) {
			Carp::croak("ERROR: Invalid pattern '$localname' => $pat. Reason: $@");
		}
	}
	
	# Undef patterns is no pattern
	return $pat;
}

# parsePatterns parameters:
#	patternDecl: a XML::LibXML::Element 'dcc:pattern-declarations' node
# it returns a reference to a hash containing all the parsed patterns
sub parsePatterns($) {
	my $patternDecl = shift;
	
	my %PATTERN = ();
	
	my(@patterns)=$patternDecl->childNodes();
	foreach my $pattern (@patterns) {
		next unless($pattern->nodeType()==XML::LibXML::XML_ELEMENT_NODE);
		
		my($localname)=$pattern->localname();
		if($localname eq 'pattern') {
			my($name)=$pattern->getAttribute('name');
			
			Carp::croak("ERROR: duplicate pattern declaration: $name")  if(exists($PATTERN{$name}));
			
			$PATTERN{$name} = __parse_pattern($pattern,$name);
		#} else {
		#	warn "WARNING: definitions file is garbled. Found unexpected element in pattern section: $localname\n";
		}
	}
	
	return \%PATTERN;
}

1;
