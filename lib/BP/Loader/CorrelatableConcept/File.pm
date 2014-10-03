#!/usr/bin/perl -W

use v5.12;
no warnings qw(experimental);
use strict;
use Carp;
use BP::Model;

use File::Temp;
use Sys::CPU;
#use Sys::MemInfo;
#use Time::HiRes;

# These two are to prepare the values to be inserted
use boolean;
use DateTime::Format::ISO8601;

use BP::Model::Common;

package BP::Loader::CorrelatableConcept::File;

use Scalar::Util qw(blessed);

my $GREP = 'grep';
#my $PHYSMEM = Sys::MemInfo::totalmem();

# Constructor parameters:
#	conceptFile: the file(name) with the content
sub new($) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	# my $self  = $class->SUPER::new();
	my $self = {};
	bless($self,$class);
	$self->{conceptFile} = shift;
	
	return $self;
}

# The filename
sub filename {
	$_[0]->{conceptFile};
}

# readColDesc parameters:
#	concept: a BP::Model::Concept
#	isIdentifying: 1 or undef
#	isSlave: 1 or undef
# This method reads the line with the column order from the input file
# and maps their positions to their corresponding BP::Model::Column
# instances from the BP::Model::Concept used to create this instance
sub readColDesc($$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	unless(exists($self->{coldesc})) {
		my $concept = shift;
		
		Carp::croak('Parameter must be a BP::Model::Concept')  unless(blessed($concept) && $concept->isa('BP::Model::Concept'));
		my $isIdentifying = shift;
		my $isSlave = shift;
		
		my $columnSet = $concept->columnSet();
		my $colHash = $columnSet->columns;
		
		my $hasPK = scalar(@{$columnSet->idColumnNames}) > 0;
		# An alias for this variable would be $hasFK
		my $idConcept = $concept->idConcept;
		
		Carp::croak('ASSERTION ERROR: Identifying concept with no keys!')  if(!$hasPK && $isIdentifying);
		#Carp::croak('FATAL ERROR: No correlatable source!')  if($idConcept && !$isSlave);
		
		my $infile = $self->filename;
		# Let's fetch the header
		# so we can map the reading positions
		if(open(my $H,'<',$infile)) {
			# Reading only the header
			my $header = undef;
			while(my $line = <$H>) {
				next  if(substr($line,0,1) eq '#');
				$header = $line;
				last;
			}
			
			close($H);
			Carp::croak('File '.$infile.' did not have a header!')  unless(defined($header));
			
			# And now, let's process the header!
			chomp($header);
			my %pk_hash = $hasPK ? map { $_ => undef } @{$columnSet->idColumnNames} : ();
			my @fkColumnNames = ();
			my %fk_hash = ();
			if($idConcept) {
				# First, let's get the positions as primary key
				my %refpos = ();
				my $rpos = 0;
				foreach my $refName (@{$idConcept->columnSet->idColumnNames}) {
					$refpos{$refName} = $rpos;
					$rpos ++;
				}
				
				# Then, give the identifying PK order to the FK columns here!
				foreach my $column (values(%{$colHash})) {
					if($column->refColumn && ! $column->relatedConcept) {
						$fkColumnNames[$refpos{$column->refColumn->name}] = $column->name;
						$fk_hash{$column->name} = undef;
					}
				}
			}
			
			# Mapping the columns in the file to the BP::Model::Column instances in the BP::Model::Concept
			my %unmappedCols = map { $_ => undef } @{$columnSet->columnNames};
			my $pos = 0;
			my $effpos = 0;
			my @coldesc = ();
			my @colpos = ();
			my @colMap = ();
			foreach my $columnName (split(/\t/,$header)) {
				my $typePrep = undef;
				my $typeCheck = undef;
				my $column = undef;
				if(exists($colHash->{$columnName})) {
					Carp::croak('Column '.$columnName.' is repeated in file '.$infile)  unless(exists($unmappedCols{$columnName}));
					# As it is being mapped, remove it from the hash!
					delete($unmappedCols{$columnName});
					
					# Store it for column mapping when there are multiple files
					push(@colMap,[$pos,$columnName]);
					
					# We have to use the effective position instead of the real one, because
					# unknown columns have to be dropped
					$pk_hash{$columnName}=$effpos  if(exists($pk_hash{$columnName}));
					$fk_hash{$columnName}=$effpos  if(exists($fk_hash{$columnName}));
					
					# We are not going to translate and store foreign keys from correlated entries
					unless($isSlave && exists($fk_hash{$columnName})) {
						$column = $colHash->{$columnName};
						$typePrep = $column->columnType->dataMangler;
						$typeCheck = $column->columnType->dataChecker;
						
						# Optimization
						$typePrep = undef  if($typePrep == \&BP::Model::ColumnType::__string);
						$typeCheck = undef  if($typeCheck == \&BP::Model::ColumnType::__true);
						push(@coldesc,[$columnName,$typePrep,$typeCheck,$column]);
						push(@colpos,$effpos);
					}
					$effpos++;
				} else {
					Carp::cluck('Unknown column '.$columnName.' in file '.$infile);
					$self->{hasUnknown} = 1;
				}
				$pos++;
			}
			
			# Is there any unmapped column?
			Carp::croak('File '.$infile.' has unmapped columns: '.join(', ',keys(%unmappedCols)).' . Aborting...')  if(scalar(keys(%unmappedCols))>0);
			
			$self->{numcols} = $pos;
			$self->{coldesc} = \@coldesc;
			# Known columns to be stored
			$self->{colpos} = \@colpos;
			$self->{colMap} = \@colMap;
			
			# Mapping positions to keys (only if needed!)
			$self->{PKkeypos} = ($hasPK && $isIdentifying) ? [@pk_hash{@{$columnSet->idColumnNames}}] : undef;
			$self->{FKkeypos} = ($idConcept && $isSlave) ? [@fk_hash{@fkColumnNames}] : undef;
		} else {
			Carp::croak("Unable to open file ".$infile);
		}
	}
}

# hasUnknownColumns parameters:
#	(none)
# It returns true when the file has unknown columns
sub hasUnknownColumns() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return exists($self->{hasUnknown});
}

# getColumnMap parameters:
#	(none)
# It returns the position -> column name mapping
sub getColumnMap() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{colMap};
}

# generatePipeSentence parameters:
#	referenceFile: a BP::Loader::CorrelatableConcept::File, whose columns are used as reference (optional)
sub generatePipeSentence(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $referenceFile = shift;
	
	my $infile = $self->filename;
	my $retval = [
		[$GREP,'-v','^#',$infile],
		['tail','-n','+2']
	];
	
	my $mode = undef;
	my $columns = undef;
	if(defined($referenceFile) && $referenceFile!=$self) {
		$mode = 'awk';
		if(!$referenceFile->hasUnknownColumns && @{$self->getColumnMap} ~~ @{$referenceFile->getColumnMap}) {
			$mode = undef;
		} else {
			# The order is setup by its master
			my $refColMap = $referenceFile->getColumnMap();
			my %colMapHash = map { $_->[1] => ($_->[0] + 1) } @{$self->getColumnMap()};
			my @masterColMap = @colMapHash{map { $_->[1] } @{$refColMap}};
			
			# Is it monotonically ascendant?
			my $prevCol = $masterColMap[0];
			foreach my $col (@masterColMap[1..$#masterColMap]) {
				if($prevCol >= $col) {
					# As it is not, set up the awk mode
					$columns = '$'.join(' , $',@masterColMap);
					last;
				} else {
					$prevCol = $col;
				}
			}
			
			unless(defined($columns)) {
				$mode = 'cut';
				$columns = join(',',@masterColMap);
			}
		}
	} elsif($self->hasUnknownColumns) {
		$mode = 'cut';
		$columns = join(',',map { $_->[0] + 1 } @{$self->getColumnMap()});
	}
	
	# It is not going to use itself as a reference! C'mon!
	if($mode eq 'awk') {
		push(@{$retval},['awk',"BEGIN{OFS=\"\\t\";} { print $columns;}"]);
	} elsif($mode eq 'cut') {
		# The instance sets up its order
		push(@{$retval},['cut','-f',$columns]);
	} # If there is no unknown column, then it is right!
	
	return $retval;
}

# checkValues parameters:
#	p_cells: The values read from the file, to be mapped
sub checkValues(\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	# The cells
	my $p_cells = shift;
	
	my $p_coldesc = $self->{coldesc};
	my $pos = 0;
	
	# Only the columns to be stored!
	foreach my $cell (@{$p_cells}[@{$self->{colpos}}]) {
		return undef  unless(!$p_coldesc->[$pos][2] || $p_coldesc->[$pos][2]->($cell));
		
		$pos++;
	}
	
	return 1;
}

# mapValues parameters:
#	p_cells: The values read from the file, to be mapped
sub mapValues(\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	# The cells
	my $p_cells = shift;
	
	# The key values to be compared to
	my $p_PKkeyvals = $self->{PKkeypos}?[@{$p_cells}[@{$self->{PKkeypos}}]]:undef;
	my $p_FKkeyvals = $self->{FKkeypos}?[@{$p_cells}[@{$self->{FKkeypos}}]]:undef;
	
	my %entry = ();
	my $p_coldesc = $self->{coldesc};
	my $pos = 0;
	
	# Only the columns to be stored!
	foreach my $cell (@{$p_cells}[@{$self->{colpos}}]) {
		$entry{$p_coldesc->[$pos][0]} = ($p_coldesc->[$pos][1])?$p_coldesc->[$pos][1]->($cell):$cell;
		
		$pos++;
	}
	
	# Saved for subsequent fetch
	$self->{entry} = \%entry;
	$self->{PKkeyvals} = $p_PKkeyvals;
	$self->{FKkeyvals} = $p_FKkeyvals;
}

# entry parameters:
#	(none)
# It returns the last parsed entry, along with the PK values
sub entry() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return ($self->{entry},$self->{PKkeyvals});
}

sub __keyMatches(\@\@) {
	my($l,$r)=@_;
	
	return 0  if @{$l}~~@{$r};
	my $maxidx = $#{$l};
	
	foreach my $pos (0..$maxidx) {
		return -1  if($l->[$pos] lt $r->[$pos]);
		return 1  if($l->[$pos] gt $r->[$pos]);
	}
	
	return 0;
}

# keyMatches parameters:
#	p_PK: The PK values from the parent correlated concept, used to compare
#		against the FK values
# It returns -1 if the PK is less than the FK, 0 if they match and 1 if the
# PK is greater than FK
sub keyMatches(\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $p_PK = shift;
	
	return __keyMatches(@{$p_PK},@{$self->{FKkeyvals}});
}

sub cleanup() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	delete($self->{entry});
	delete($self->{PKkeyvals});
	delete($self->{FKkeyvals});
}

1;
