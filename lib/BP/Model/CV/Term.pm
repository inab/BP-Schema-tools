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
use BP::Model::CV::Common;

package BP::Model::CV::Term;

use constant {
	KEY	=>	0,
	KEYS	=>	1,
	NAME	=>	2,
	PARENTS	=>	3,
	ANCESTORS	=>	4,
	ISALIAS	=>	5,
	PARENTCV	=>	6
};

# Constructor
# new parameters:
#	key: a string, or an array of strings
#	name: a string
#	parents: undef or an array of strings
#	isAlias: undef or true
sub new($$;$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $key = shift;
	my $keys = undef;
	my $name = shift;
	# Optional parameters
	my $parents = shift;
	my $isAlias = shift;
	
	if(ref($key) eq 'ARRAY') {
		$keys = $key;
		$key = $keys->[0];
	} else {
		$keys = [$key];
	}
	
	# Ancestors will be resolved later
	my @term=($key,$keys,$name,$parents,undef,$isAlias,undef);
	
	return bless(\@term,$class);
}

# Alternate constructor
# parseAlias parameters:
#	termAlias: a XML::LibXML::Element node, 'dcc:term-alias'
sub parseAlias($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $termAlias = shift;
	my $key = $termAlias->getAttribute('name');
	my $name = '';
	my @parents = ();
	foreach my $el ($termAlias->childNodes()) {
		next  unless($el->nodeType == XML::LibXML::XML_ELEMENT_NODE);
		
		if($el->localname eq 'e') {
			# Saving the "parents" of the alias
			push(@parents,$el->getAttribute('v'));
		} elsif($el->localname eq 'description') {
			$name = $el->textContent();
		}
	}
	
	return $class->new($key,$name,\@parents,1);
}

# The main key of the term
sub key {
	return $_[0]->[KEY];
}

# All the keys of the term: the main and the alternate ones
sub keys {
	return $_[0]->[KEYS];
}

# The name of the term
sub name {
	return $_[0]->[NAME];
}

# The parent(s) of the term, (through "is a" relationships), as their keys
# undef, when they have no parent
sub parents {
	return $_[0]->[PARENTS];
}

# All the ancestors of the term, through transitive "is a",
# or term aliasing. No order is guaranteed, but there will
# be no duplicates.
sub ancestors {
	return $_[0]->[ANCESTORS];
}

# If it returns true, it is an alias (the union of several terms,
# which have been given the role of the parents)
sub isAlias {
	return $_[0]->[ISALIAS];
}

# If it returns true, a call to ancestors method returns the
# calculated lineage
sub gotLineage {
	return defined($_[0]->[ANCESTORS]);
}

# The BP::Model::CV instance which defines the term
sub parentCV {
	return $_[0]->[PARENTCV];
}

# The BP::Model::CV instance which defines the term
sub _setParentCV {
	$_[0]->[PARENTCV] = $_[1];
}

# calculateAncestors parameters:
#	p_CV: a reference to a hash, which is the pool of
#		BP::Model::CV::Term instances where this instance can
#		find its parents.
#	doRecover: If true, it tries to recover from unknown parents,
#		removing them
#	p_visited: a reference to an array, which is the pool of
#		BP::Model::CV::Term keys which are still being
#		visited.
sub calculateAncestors($;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	# The pool where I should find my parents
	my $p_CV = shift;
	my $doRecover = shift;
	my $p_visited = shift;
	$p_visited = []  unless(defined($p_visited));
	
	# The output
	my $p_ancestors = undef;
	
	unless($self->gotLineage()) {
		my @ancestors= ();
		my %ancHash = ();
		my @curatedParents = ();
		
		my $saveCurated = undef;
		if(defined($self->parents)) {
			# Let's gather the lineages
			my @visited = (@{$p_visited},$self->key);
			my %visHash = map { $_ => undef } @visited;
			foreach my $parentKey (@{$self->parents}) {
				unless(exists($p_CV->{$parentKey})) {
					Carp::croak("Parent $parentKey does not exist on the CV for term ".$self->key)  unless($doRecover);
					$saveCurated = 1;
					next;
				}
				
				# Sanitizing alternate keys
				my $parent = $p_CV->{$parentKey};
				$parentKey = $parent->key;
				
				Carp::croak("Detected a lineage term loop: @visited $parentKey\n")  if(exists($visHash{$parentKey}));
				
				# Getting the ancestors
				my $parent_ancestors = $parent->calculateAncestors($p_CV,$doRecover,\@visited);
				
				# Saving only the new ones
				foreach my $parent_ancestor (@{$parent_ancestors},$parentKey) {
					unless(exists($ancHash{$parent_ancestor})) {
						push(@ancestors,$parent_ancestor);
						$ancHash{$parent_ancestor} = undef;
					}
				}
				# And the curated parent
				push(@curatedParents,$parentKey);
			}
		}
		
		# Last, setting up the information
		$p_ancestors = \@ancestors;
		$self->[ANCESTORS] = $p_ancestors;
		$self->[PARENTS] = \@curatedParents  if(defined($saveCurated));
	
	} else {
		$p_ancestors = $self->[ANCESTORS];
	}
	
	return $p_ancestors;
}

# This method serializes the BP::Model::CV::Term instance into a OBO structure
# serialize parameters:
#	O: the output file handle
sub OBOserialize($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $O = shift;
	
	# We need this
	print $O "[Term]\n";
	BP::Model::CV::Common::printOboKeyVal($O,'id',$self->key);
	BP::Model::CV::Common::printOboKeyVal($O,'name',$self->name);
	
	# The alterative ids
	my $first = 1;
	foreach my $alt_id (@{$self->keys}) {
		# Skipping the first one, which is the main id
		if(defined($first)) {
			$first=undef;
			next;
		}
		BP::Model::CV::Common::printOboKeyVal($O,'alt_id',$alt_id);
	}
	if(defined($self->parents)) {
		my $propLabel = ($self->isAlias)?'union_of':'is_a';
		foreach my $parKey (@{$self->parents}) {
			BP::Model::CV::Common::printOboKeyVal($O,$propLabel,$parKey);
		}
	}
	print $O "\n";
}

1;
