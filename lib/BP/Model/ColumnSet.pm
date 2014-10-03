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

use BP::Model::Column;
use BP::Model::Index;

package BP::Model::ColumnSet;

use constant {
	ID_COLUMN_NAMES	=>	0,
	COLUMN_NAMES	=>	1,
	COLUMNS		=>	2,
	INDEXES		=>	3
};

# This is the constructor.
# new parameters:
#	parentColumnSet: a BP::Model::ColumnSet instance, which is the parent.
#	p_indexes: a reference to an array of BP::Model::Index instances.
#	columns: an array of BP::Model::Column instances
# returns a BP::Model::ColumnSet instance with all the BP::Model::Column instances (including
# the inherited ones from the parent).
sub new($$@) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	my $parentColumnSet = shift;
	my $p_indexes = shift;
	$p_indexes = []  unless(ref($p_indexes) eq 'ARRAY');
	my @columns = @_;
	
	my @columnNames = ();
	my @idColumnNames = ();
	my %columnDecl = ();
	my @indexDecl = ();
	my %indexDeclHash = ();
	# Inheriting column information from parent columnSet
	if(Scalar::Util::blessed($parentColumnSet)) {
		@idColumnNames = @{$parentColumnSet->idColumnNames};
		@columnNames = @{$parentColumnSet->columnNames};
		%columnDecl = %{$parentColumnSet->columns};
		# As these indexes were validated in the parent
		# they are not validated again
		foreach my $index (@{$parentColumnSet->indexes}) {
			push(@indexDecl,$index);
			$indexDeclHash{$index+0} = undef;
		}
	}
	
	# Array with the idref column names
	# Array with column names (all)
	# Hash of BP::Model::Column instances
	my @columnSet = (\@idColumnNames,\@columnNames,\%columnDecl,\@indexDecl);
	
	my @checkDefault = ();
	foreach my $column (@columns) {
		# We want to keep the original column order as far as possible
		if(exists($columnDecl{$column->name})) {
			if($columnDecl{$column->name}->columnType->use eq BP::Model::ColumnType::IDREF) {
				Carp::croak('It is not allowed to redefine column '.$column->name.'. It is an idref one!');
			}
		} else {
			push(@columnNames,$column->name);
			# Is it a id column?
			push(@idColumnNames,$column->name)  if($column->columnType->use eq BP::Model::ColumnType::IDREF);
		}
		
		$columnDecl{$column->name}=$column;
		push(@checkDefault,$column)  if(ref($column->columnType->default));
	}
	
	# Skip duplicated index declarations
	foreach my $index (@{$p_indexes}) {
		my $indexKey = $index+0;
		unless(exists($indexDeclHash{$indexKey})) {
			# Store it after validating columns
			#if($index->hasValidColumns(\%columnDecl)) {
				push(@indexDecl,$index);
				$indexDeclHash{$indexKey} = undef;
			#} else {
			#	Carp::croak('Invalid index declaration ('.join(',',map { $_->[0] } @{$index->indexAttributes}).') for columns ['.join(',',@columnNames).']');
			#}
		}
	}
	
	# And now, second pass, where we check the consistency of default values
	foreach my $column (@checkDefault) {
		if(exists($columnDecl{${$column->columnType->default}})) {
			my $defColumn = $columnDecl{${$column->columnType->default}};
			
			if($defColumn->columnType->use >= BP::Model::ColumnType::IDREF) {
				$column->columnType->setDefault($defColumn);
			} else {
				Carp::croak('Column '.$column->name.' pretends to use as default value generator column '.${$column->columnType->default}.' which is not neither idref nor required!');
			}
		} else {
			Carp::croak('Column '.$column->name.' pretends to use as default value generator unknown column '.${$column->columnType->default});
		}
	}
	
	return bless(\@columnSet,$class);
}

# This is a constructor.
# parseColumnSet parameters:
#	container: a XML::LibXML::Element node, containing 'dcc:column' elements
#	parentColumnSet: a BP::Model::ColumnSet instance, which is the parent.
#	model: a BP::Model instance, used to validate the columns.
# returns a BP::Model::ColumnSet instance with all the BP::Model::Column instances (including
# the inherited ones from the parent).
sub parseColumnSet($$$) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	my $container = shift;
	my $parentColumnSet = shift;
	my $model = shift;
	
	my @columns = ();
	foreach my $colDecl ($container->childNodes()) {
		next  unless($colDecl->nodeType == XML::LibXML::XML_ELEMENT_NODE && $colDecl->localname() eq 'column');
		
		my $column = BP::Model::Column->parseColumn($colDecl,$model);
		push(@columns,$column);
	}
	
	my $p_indexes = BP::Model::Index::ParseIndexes($container);
	
	return $class->new($parentColumnSet,$p_indexes,@columns);
}


# This is a constructor
# combineColumnSets parameters:
#	dontCroak: A flag which must be set to true when we want to skip
#		idref column redefinitions, instead of complaining about
#	columnSets: An array of BP::Model::ColumnSet instances
# It returns a columnSet which is the combination of the input ones. The
# order of the columns is preserved the most.
# It is not allowed to override IDREF columns
sub combineColumnSets($@) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my($dontCroak,$firstColumnSet,@columnSets) = @_;
	
	# First column set is the seed
	my @columnNames = @{$firstColumnSet->columnNames};
	my @idColumnNames = @{$firstColumnSet->idColumnNames};
	my %columnDecl = %{$firstColumnSet->columns};
	my @indexDecl = ();
	my %indexDeclHash = ();
	
	foreach my $index (@{$firstColumnSet->indexes}) {
		push(@indexDecl,$index);
		$indexDeclHash{$index+0} = undef;
	}
	
	# Array with the idref column names
	# Array with column names (all)
	# Hash of BP::Model::Column instances
	# Array of BP::Model::Index instances
	my @combinedColumnSet = (\@idColumnNames,\@columnNames,\%columnDecl,\@indexDecl);
	
	# And now, the next ones!
	foreach my $columnSet (@columnSets) {
		my $p_columns = $columnSet->columns;
		foreach my $columnName (@{$columnSet->columnNames}) {
			# We want to keep the original column order as far as possible
			if(exists($columnDecl{$columnName})) {
				# Same column, so skip!
				next  if($columnDecl{$columnName} == $p_columns->{$columnName});
				if($columnDecl{$columnName}->columnType->use eq BP::Model::ColumnType::IDREF) {
					next  if($dontCroak);
					Carp::croak('It is not allowed to redefine column '.$columnName.'. It is an idref one!');
				}
			} else {
				push(@columnNames,$columnName);
				# Is it a id column?
				push(@idColumnNames,$columnName)  if($p_columns->{$columnName}->columnType->use eq BP::Model::ColumnType::IDREF);
			}
			$columnDecl{$columnName} = $p_columns->{$columnName};
		}
	
		# Skip duplicated index declarations
		# no need to validate indexes, as columns are not removed
		foreach my $index (@{$columnSet->indexes}) {
			my $indexKey = $index+0;
			unless(exists($indexDeclHash{$indexKey})) {
				push(@indexDecl,$index);
				$indexDeclHash{$indexKey} = undef;
			}
		}
	}
	
	return bless(\@combinedColumnSet,$class);
}

# Cloning facility
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	# A cheap way to clone itself
	return ref($self)->combineColumnSets(1,$self);
}

# Reference to an array with the idref column names
sub idColumnNames {
	return $_[0]->[BP::Model::ColumnSet::ID_COLUMN_NAMES];
}

# Array with column names (all)
sub columnNames {
	return $_[0]->[BP::Model::ColumnSet::COLUMN_NAMES];
}

# Hash of BP::Model::Column instances
sub columns {
	return $_[0]->[BP::Model::ColumnSet::COLUMNS];
}

# Array of BP::Model::Index instances
sub indexes {
	return $_[0]->[BP::Model::ColumnSet::INDEXES];
}

# derivedIndexes parameters:
#	prefix: the prefix for the column names
sub derivedIndexes(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $prefix = shift;
	
	my @retval = map { $_->clonePrefixed($prefix) } @{$self->indexes};
	
	foreach my $column (values(%{$self->columns})) {
		push(@retval,$column->derivedIndexes($prefix));
	}
	
	return @retval;
}

# idColumns parameters:
#	idConcept: The BP::Model::Concept instance owning the id columns
#	doMask: Are the columns masked for storage?
#	weakAnnotations: BP::Model::AnnotationSet from weak-concepts.
# It returns a BP::Model::ColumnSet instance, with the column declarations
# corresponding to columns with idref restriction
sub idColumns($;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $idConcept = shift;
	my $doMask = shift;
	my $weakAnnotations = shift;
	
	my @columnNames = @{$self->idColumnNames};
	my $p_columns = $self->columns;
	my %columns = map { $_ => $p_columns->{$_}->cloneRelated($idConcept,undef,$doMask,$weakAnnotations) } @columnNames;
	
	# Keep indexes defined over idColumns
	my @indexDecl = ();
	foreach my $index (@{$self->indexes}) {
		push(@indexDecl,$index)  if($index->hasValidColumns(\%columns));
	}
	
	my @columnSet = (
		\@columnNames,
		[@columnNames],
		\%columns,
		\@indexDecl
	);
	
	return bless(\@columnSet,ref($self));
}

# relatedColumns parameters:
#	myConcept: A BP::Model::Concept instance, which this columnSet belongs
#		The kind of relation could be inheritance, or 1:N
#	relatedConcept: A BP::Model::RelatedConcept instance
#		(which contains the optional prefix to be set to the name when the columns are cloned)
# It returns a BP::Model::ColumnSet instance, with the column declarations
# corresponding to columns with idref restriction
sub relatedColumns(;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $myConcept = shift;
	
	my $relatedConcept = shift;
	
	my @columnNames = @{$self->idColumnNames};
	my $p_columns = $self->columns;
	my @refColumnNames = ();
	my %columnCorrespondence = ();
	my %columns = map {
		my $refColumn = $p_columns->{$_}->cloneRelated($myConcept,$relatedConcept);
		my $refColumnName = $refColumn->name;
		$columnCorrespondence{$_} = $refColumnName;
		push(@refColumnNames,$refColumnName);
		$refColumnName => $refColumn
	} @columnNames;
	
	# keep and rework indexes defined over refColumns
	my @indexDecl = ();
	foreach my $index (@{$self->indexes}) {
		push(@indexDecl,$index->relatedIndex(\%columnCorrespondence))  if($index->hasValidColumns(\%columnCorrespondence));
	}
	
	my @columnSet = (
		\@refColumnNames,
		[@refColumnNames],
		\%columns,
		\@indexDecl
	);
	
	return bless(\@columnSet,ref($self));
}

# addColumn parameters:
#	inputColumn: A BP::Model::Column instance. Those
#		columns with the same name are overwritten.
#	isPKFriendly: if true, when the column is idref, its role is
#		kept if there are already idref columns
# the method stores the column in the current columnSet.
# New columns can override old ones, unless some of the old ones
# is typed as idref. In that case, an exception is fired.
sub addColumn($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $inputColumn = shift;
	my $isPKFriendly = shift;
	
	# First, let's see whether there is some of our idkeys in the
	# input, so we can stop early
	my $inputColumnName = $inputColumn->name;
	if(exists($self->columns->{$inputColumnName})) {
		Carp::croak("Trying to add already declared idcolumn".$inputColumnName);
	}
	my $doAddIDREF = scalar(@{$self->idColumnNames}) > 0 && $isPKFriendly;
	
	# And now, let's add them!
	my $p_columnsHash = $self->columns;
	my $p_columnNames = $self->columnNames;
	my $p_idColumnNames = $self->idColumnNames;
	
	# We want to keep the original column order as far as possible
	# Registering the column names (if there is no column with that name!)
	unless(exists($p_columnsHash->{$inputColumnName})) {
		push(@{$p_columnNames},$inputColumnName);
		# Is it a id column which should be added?
		if($doAddIDREF && $inputColumn->columnType->use eq BP::Model::ColumnType::IDREF) {
			push(@{$p_idColumnNames},$inputColumnName);
		}
	}
	
	$p_columnsHash->{$inputColumnName} = $inputColumn;
}

# addColumns parameters:
#	inputColumnSet: A BP::Model::ColumnSet instance which contains
#		the columns to be added to this column set. Those
#		columns with the same name are overwritten.
#	isPKFriendly: if true, when the columns are idref, their role are
#		kept if there are already idref columns
# the method stores the columns in the input columnSet in the current
# one. New columns can override old ones, unless some of the old ones
# is typed as idref. In that case, an exception is fired.
sub addColumns($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $inputColumnSet = shift;
	my $isPKFriendly = shift;
	
	my $inputColumnsHash = $inputColumnSet->columns;
	
	# First, let's see whether there is some of our idkeys in the
	# input, so we can stop early
	foreach my $columnName (@{$self->idColumnNames}) {
		if(exists($inputColumnsHash->{$columnName})) {
			Carp::croak("Trying to add already declared idcolumn $columnName");
		}
	}
	my $doAddIDREF = scalar(@{$self->idColumnNames}) > 0 && $isPKFriendly;
	
	# And now, let's add them!
	my $p_columnsHash = $self->columns;
	my $p_columnNames = $self->columnNames;
	my $p_idColumnNames = $self->idColumnNames;
	
	# We want to keep the original column order as far as possible
	foreach my $inputColumnName (@{$inputColumnSet->columnNames}) {
		# Registering the column names (if there is no column with that name!)
		my $inputColumn = $inputColumnsHash->{$inputColumnName};
		unless(exists($p_columnsHash->{$inputColumnName})) {
			push(@{$p_columnNames},$inputColumnName);
			# Is it a id column which should be added?
			if($doAddIDREF && $inputColumn->columnType->use eq BP::Model::ColumnType::IDREF) {
				push(@{$p_idColumnNames},$inputColumnName);
			}
		}
		
		$p_columnsHash->{$inputColumnName} = $inputColumn;
	}
	
	# Skip repetitions on index declarations
	my %indexDeclHash = map { $_ => undef } @{$self->indexes};
	
	foreach my $index (@{$inputColumnSet->indexes}) {
		my $indexKey = $index+0;
		unless(exists($indexDeclHash{$indexKey})) {
			push(@{$self->indexes},$index);
			
			$indexDeclHash{$indexKey} = undef;
		}
	}
}

# resolveDefaultCalculatedValues parameters:
#	(none)
# the method resolves the references of default values to other columns
sub resolveDefaultCalculatedValues() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_columns = $self->columns;
	foreach my $column (values(%{$p_columns})) {
		if(ref($column->columnType->default) eq 'SCALAR') {
			my $defCalColumnName = ${$column->columnType->default};
			
			if(exists($p_columns->{$defCalColumnName})) {
				$column->columnType->setCalculatedDefault($p_columns->{$defCalColumnName});
			} else {
				Carp::croak('Unknown column '.$defCalColumnName.' use for default value of column '.$column->name);
			}
		}
	}
}

1;
