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

use BP::Model::ColumnSet;

package BP::Model::CompoundType;

# This is the constructor.
# parseCompoundType parameters:
#	compoundTypeElem: a XML::LibXML::Element 'dcc:compound-type' node
#	model: a BP::Model instance where the concept type was defined
# returns an array of 'BP::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseCompoundType($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $compoundTypeElem = shift;
	my $model = shift;
	my $columnName = shift;
	
	# compound-type name (could be anonymous)
	# columnSet
	# separators
	# template
	# dataMangler
	# isValid
	my @compoundType = (undef,undef,undef,undef,undef,undef);
	
	# If it has name, then it has to have either a collection name or a key name
	# either inline or inherited from the ancestors
	$compoundType[0] = $compoundTypeElem->getAttribute('name')  if($compoundTypeElem->hasAttribute('name'));
	
	# Let's parse the columns
	my $columnSet = BP::Model::ColumnSet->parseColumnSet($compoundTypeElem,undef,$model);
	$compoundType[1] = $columnSet;
	
	my @seps = ();
	foreach my $sepDecl ($compoundTypeElem->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'sep')) {
		push(@seps,$sepDecl->textContent());
	}
	$compoundType[2] = \@seps;
	
	# Now, let's create a string template
	my $template = $columnSet->columnNames->[0];
	foreach my $pos (0..$#seps) {
		$template .= $seps[$pos];
		$template .= $columnSet->columnNames->[$pos+1];
	}
	
	$compoundType[3] = $template;
	
	# the data mangler!
	my @colMangler = map { [$_, $columnSet->columns->{$_}->columnType->dataMangler] } @{$columnSet->columnNames};
	my @colChecker = map { $columnSet->columns->{$_}->columnType->dataChecker } @{$columnSet->columnNames};
	$compoundType[4] = sub {
		my %result = ();
		
		my $input = $_[0];
		my $idx = 0;
		foreach my $sep (@seps) {
			my $limit = index($input,$sep);
			if($limit!=-1) {
				$result{$colMangler[$idx][0]} = $colMangler[$idx][1]->(substr($input,0,$limit));
				$input = substr($input,$limit+length($sep));
			} else {
				Carp::croak('Data mangler of '.$template.' complained parsing '.$_[0].' on facet '.$colMangler[$idx][0]);
			}
			$idx++;
		}
		# And last one
		$result{$colMangler[$idx][0]} = $colMangler[$idx][1]->($input);
		
		return  \%result;
	};
	
	# And the subref for the dataChecker method
	$compoundType[5] = sub {
		my $input = $_[0];
		my $idx = 0;
		foreach my $sep (@seps) {
			my $limit = index($input,$sep);
			if($limit!=-1) {
				return undef  unless($colChecker[$idx]->(substr($input,0,$limit)));
				$input = substr($input,$limit+length($sep));
			} else {
				return undef;
			}
			$idx++;
		}
		# And last one
		return $colChecker[$idx]->($input);
	};
	
	# The returning values array
	return bless(\@compoundType,$class);
}

# compound type name (could be anonymous), so it would return undef
sub name {
	return $_[0]->[0];
}

# columnSet
# It returns a BP::Model::ColumnSet instance, with all the column declarations inside this compound type
sub columnSet {
	return $_[0]->[1];
}

sub seps {
	return $_[0]->[2];
}

sub template {
	return $_[0]->[3];
}

sub tokens {
	return $_[0]->[1]->columnNames;
}

sub dataMangler {
	return $_[0]->[4];
}

sub dataChecker {
	return $_[0]->[5];
}

sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $val = shift;
	
	return $self->dataChecker->($val);
}

1;
