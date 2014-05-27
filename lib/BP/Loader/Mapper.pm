#!/usr/bin/perl -W

use strict;
use Carp;

package BP::Loader::Mapper;

use BP::Model;

# This constant is used by several storage models.
# Therefore, it is better defined here.
use constant FILE_PREFIX_KEY => 'file-prefix';

our $SECTION;
BEGIN {
	$SECTION = 'main';
}

# The registered storage models
our %storage_names;

# Constructor parameters:
#	storageModel: the key identifying the storage model
#	model: a BP::Model instance
#	other parameters
sub new($$;@) {
	# Very special case for multiple inheritance handling
	# This is the seed
	my($self)=shift;
	my($class)=ref($self) || $self;
	
	my $storageModel = shift;
	Carp::croak("ERROR: undefined storage model")  unless(defined($storageModel));
	
	$storageModel = lc($storageModel);
	$storageModel =~ s/[^-a-z]//g;
	
	if(exists($storage_names{$storageModel})) {
		my $class  = $storage_names{$storageModel};
		my $model = shift;
		return $class->new($model,@_);
	} else {
		Carp::croak("ERROR: unregistered storage model $storageModel\n");
	}
}

sub isHierarchical {
	Carp::croak('Unimplemented method!');
}

# generateNativeModel parameters:
#	workingDir: The optional directory where the native model files are going to be saved.
# It returns a reference to an array of absolute paths to the generated files, based on workingDir
sub generateNativeModel($) {
	Carp::croak('Unimplemented method!');
}

# This method connects to the database and returns the handler. The connection
# is persistent so next calls to the method should return the same instance.
sub connect() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	$self->{conn} = $self->_connect()  unless(exists($self->{conn}));
	
	return $self->{conn};
}

# This method returns a connection to the database
sub _connect() {
	Carp::croak('Unimplemented method!');
}

# storeNativeModel parameters:
#	workingDir: The optional directory where the native model files are going to be saved.
sub storeNativeModel(\$) {
	Carp::croak('Unimplemented method!');
}

# mapData parameters:
#	p_mainCorrelatableConcepts: a reference to an array of BP::Loader::CorrelatableConcept instances.
#	p_otherCorrelatedConcepts: a reference to an array of BP::Loader::CorrelatableConcept instances (the "free slaves" ones).
sub mapData(\@\@) {
	Carp::croak('Unimplemented method!');
}

# parseOrderingHints parameters:
#	a XML::LibXML::Element, type 'dcc:ordering-hints'
# It returns the ordering hints (at this moment, undef or the block where it appears)
sub parseOrderingHints($) {
	my($ordHints) = @_;
	
	my $retvalBlock = undef;
	if(ref($ordHints) && $ordHints->isa('XML::LibXML::Element')
		&& $ordHints->namespaceURI eq BP::Model::dccNamespace
		&& $ordHints->localname eq 'ordering-hints'
	) {
		foreach my $block ($ordHints->getChildrenByTagNameNS(BP::Model::dccNamespace,'block')) {
			$retvalBlock = $block->textContent;
			last;
		}
	}
	
	return ($retvalBlock);
}

# _fancyColumnOrdering parameters:
#	concept: a BP::Concept instance
# It returns an array with the column names from the concept in a fancy
# order, based on several criteria, like annotations
sub _fancyColumnOrdering($) {
	my($concept)=@_;
	
	my $columnSet = $concept->columnSet;

	# First, the idref columns, alphabetically ordered
	my @first = ();
	my @middle = ();
	my @last = ();
	foreach my $columnName (@{$columnSet->idColumnNames}) {
		my $column = $columnSet->columns->{$columnName};
		
		my $p_set = \@middle;
		if(exists($column->annotations->hash->{ordering})) {
			my($block) = parseOrderingHints($column->annotations->hash->{ordering});
			
			if($block eq 'bottom') {
				$p_set = \@last;
			} elsif($block eq 'top') {
				$p_set = \@first;
			}
		}
		
		push(@{$p_set},$columnName);
	}
	
	my @idcolorder = (@first,@middle,@last);
	# Resetting for next use
	@first = ();
	@middle = ();
	@last = ();

	# And then, the others
	my %idcols = map { $_ => undef } @idcolorder;
	foreach my $columnName (@{$columnSet->columnNames}) {
		next  if(exists($idcols{$columnName}));
		
		my $column = $columnSet->columns->{$columnName};
		
		my $p_set = \@middle;
		if(exists($column->annotations->hash->{ordering})) {
			my($block) = parseOrderingHints($column->annotations->hash->{ordering});
			
			if($block eq 'bottom') {
				$p_set = \@last;
			} elsif($block eq 'top') {
				$p_set = \@first;
			}
		}
		
		push(@{$p_set},$columnName);
	}
	
	my @colorder = (@first,@middle,@last);
	
	
	return (@idcolorder,@colorder);
}

1;
