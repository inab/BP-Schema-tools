#!/usr/bin/perl -W

use strict;
use Carp;
use DCC::Model;

use File::Temp;
use Sys::CPU;
#use Sys::MemInfo;
use Time::HiRes;

# These two are to prepare the values to be inserted
use boolean;
use DateTime::Format::ISO8601;

package DCC::Loader::CorrelatableConcept::File;

my $SORT = 'sort';
my $GREP = 'grep';
my $NUMCPUS = Sys::CPU::cpu_count();
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
#	concept: a DCC::Model::Concept
#	isIdentifying: 1 or undef
#	isSlave: 1 or undef
# This method reads the line with the column order from the input file
# and maps their positions to their corresponding DCC::Model::Column
# instances from the DCC::Model::Concept used to create this instance
sub readColDesc($$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	unless(exists($self->{coldesc})) {
		my $concept = shift;
		
		Carp::croak('Parameter must be a DCC::Model::Concept')  unless(defined($concept) && ref($concept) && $concept->isa('DCC::Model::Concept'));
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
			
			# Mapping the columns in the file to the DCC::Model::Column instances in the DCC::Model::Concept
			my %unmappedCols = map { $_ => undef } @{$columnSet->columnNames};
			my $pos = 0;
			my @coldesc = ();
			my @colpos = ();
			foreach my $columnName (split(/\t/,$header)) {
				my $typePrep = undef;
				my $typeCheck = undef;
				my $column = undef;
				if(exists($colHash->{$columnName})) {
					Carp::croak('Column '.$columnName.' is repeated in file '.$infile)  unless(exists($unmappedCols{$columnName}));
					# As it is being mapped, remove it from the hash!
					delete($unmappedCols{$columnName});
					
					$pk_hash{$columnName}=$pos  if(exists($pk_hash{$columnName}));
					$fk_hash{$columnName}=$pos  if(exists($fk_hash{$columnName}));
					
					# We are not going to translate and store foreign keys from correlated entries
					unless($isSlave && exists($fk_hash{$columnName})) {
						$column = $colHash->{$columnName};
						$typePrep = $column->columnType->dataMangler;
						$typeCheck = $column->columnType->dataChecker;
						
						# Optimization
						$typePrep = undef  if($typePrep == \&DCC::Model::ColumnType::__string);
						$typeCheck = undef  if($typeCheck == \&DCC::Model::ColumnType::__true);
						push(@coldesc,[$columnName,$typePrep,$typeCheck,$column]);
						push(@colpos,$pos);
					}
				} else {
					Carp::cluck('Unknown column '.$columnName.' in file '.$infile);
				}
				$pos++;
			}
			
			# Is there any unmapped column?
			Carp::croak('File '.$infile.' has unmapped columns: '.join(', ',keys(%unmappedCols)).' . Aborting...')  if(scalar(keys(%unmappedCols))>0);
			
			$self->{numcols} = $pos;
			$self->{coldesc} = \@coldesc;
			# Known columns to be stored
			$self->{colpos} = \@colpos;
			
			# Mapping positions to keys (only if needed!)
			$self->{PKkeypos} = ($hasPK && $isIdentifying) ? [@pk_hash{@{$columnSet->idColumnNames}}] : undef;
			$self->{FKkeypos} = ($idConcept && $isSlave) ? [@fk_hash{@fkColumnNames}] : undef;
		} else {
			Carp::croak("Unable to open file ".$infile);
		}
	}
}


# Static internal method
# __SortCompressed parameters:
#	infile: The input file
#	p_keypos: The position of the ordering keys
# It returns a File::Temp instance with the sorted file, compressed with gzip
sub __SortCompressed($\@) {
	my($infile,$p_keypos) = @_;
	
	my $sortkeys = join(' ',map { my $p = $_ + 1 ; '-k'.$p.','.$p } @{$p_keypos});
	
	my $tmpout = File::Temp->new();
	my $tmpoutfilename = $tmpout->filename();
	
	# This command line reorders the lines
	# keeping the comments and the header at the beginning of the file
	# and sorting by the input keys
	# my $cmd = "('$GREP' '^#' '$infile' ; '$GREP' -v '^#' '$infile' | head -n 1 ; '$GREP' -v '^#' '$infile' | tail -n +2 | '$SORT' --parallel=$NUMCPUS -S 50% $sortkeys ) | gzip -9c > '$tmpoutfilename'";
	
	# This command line reorders the lines
	# pruning both the comments and header at the beginning of the file
	my $cmd = "'$GREP' -v '^#' '$infile' | tail -n +2 | '$SORT' --parallel=$NUMCPUS -S 50% $sortkeys | gzip -9c > '$tmpoutfilename'";
	
	system($cmd);
	
	return $tmpout;
}

# sortCompressed parameters:
#	(none)
# This method sorts the files by the PK and FK columns (only if it is needed)
sub sortCompressed($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	Carp::croak('readColDesc must be called before '.(caller(0))[3].'!')  unless(exists($self->{coldesc}));
	
	my $isIdentifying = shift;
	my $isSlave = shift;
	
	unless(exists($self->{__compressed})) {
		# Identifying whether we have to do two sorts
		my $distinctFK = 1;
		if($self->{FKkeypos} && $self->{PKkeypos}) {
			my $maxSteps = scalar(@{$self->{FKkeypos}});
			if(scalar(@{$self->{PKkeypos}}) >= $maxSteps) {
				$distinctFK = undef;
				foreach my $keypos (0..($maxSteps-1)) {
					if($self->{PKkeypos}[$keypos] != $self->{FKkeypos}[$keypos]) {
						$distinctFK = 1;
						Carp::croak('FATAL ERROR: Model does not correlate the identifying concept keys with the foreign keys!');
						last;
					}
				}
			}
		}

		my $infile = $self->filename;
		$self->{PKsortedConceptFile} = $self->{PKkeypos} ? __SortCompressed($infile,@{$self->{PKkeypos}}) : undef;

		if($self->{FKkeypos}) {
			$self->{FKsortedConceptFile} = $distinctFK ? __SortCompressed($infile,@{$self->{FKkeypos}}) : $self->{PKsortedConceptFile};
		} else {
			$self->{FKsortedConceptFile} = undef;
		}
		
		$self->{__compressed} = 1;
	}
}

# prepare parameters:
#	(none)
# This method prepares the correlated model files to be optimally inserted
# prefilling the internal buffers
sub prepare() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# Sorted Filehandle
	# entry
	# keyvals
	# File::Temp sorted file
	# Column description (arrayref)
	# Position of PK columns
	# Position of FK columns
	
	unless(exists($self->{__prepared})) {
		# Assuring it is as it should be
		$self->sortCompressed();
		
		my $infile = $self->filename;
		
		if($self->{PKsortedConceptFile} || $self->{FKsortedConceptFile}) {
			my $sortedFilename = $self->{PKsortedConceptFile}?$self->{PKsortedConceptFile}->filename() : $self->{FKsortedConceptFile}->filename();
			if(open(my $H,'-|','gunzip','-c',$sortedFilename)) {
				$self->{H} = $H;
			} else {
				Carp::croak('ERROR: Unable to open sorted temp file(s) from '.$infile);
			}
		} elsif(open(my $H,$infile)) {
			# First, skip initial comments and header
			$self->{H} = $H;
			while(my $line = <$H>) {
				next  if(substr($line,0,1) eq '#');
				
				# The header is here, so go out
				last;
			}
		} else {
			Carp::croak('ERROR: Unable to open file '.$infile);
		}
		$self->{entry} = undef;
		$self->{PKkeyvals} = undef;
		$self->{FKkeyvals} = undef;
		
		# As this is too expensive, we are doing it only once!
		$self->{__prepared} = 1;
		
		# First read, prefilling so internal buffers
		$self->nextLine();
	}
}

# This method closes the open filehandles, but it does not erase the sorted temp file
sub close() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(exists($self->{__prepared})) {
		close($self->{H});
		
		delete($self->{H});
		delete($self->{entry});
		delete($self->{PKkeyvals});
		delete($self->{FKkeyvals});
		delete($self->{__prepared});
		delete($self->{eof});
	}
}

# This method reads one line from the file, mapping the values
# It returns undef on eof, and sets eof flag
sub nextLine() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	unless(exists($self->{eof})) {
		my $H = $self->{H};
		my $line = <$H>;
		unless(eof($H)) {
			chomp($line);
			$self->mapValues(split(/\t/,$line));
			return 1;
		} else {
			$self->{eof} = 1;
		}
	}
	
	return undef;
}

# checkValues parameters:
#	p_cells: The values read from the file, to be mapped
sub checkValues(\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
	
	$self->{entry} = \%entry;
	$self->{PKkeyvals} = $p_PKkeyvals;
	$self->{FKkeyvals} = $p_FKkeyvals;
}



package DCC::Loader::CorrelatableConcept;

# Constructor parameters:
#	concept: a DCC::Model::Concept
#	conceptFiles: the file(name)s with the content
sub new($@) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	# my $self  = $class->SUPER::new();
	my $self = {};
	bless($self,$class);
	
	$self->{concept} = shift;
	my @conceptFiles = @_;
	@{$self->{conceptFiles}} = map { DCC::Loader::CorrelatableConcept::File->new($_) } @conceptFiles;
	$self->{correlatedConcepts} = undef;

	return $self;
}

# It returns a DCC::Model::Concept instance
sub concept {
	return $_[0]->{concept};
}

# Labelling this correlating concept as 'slave' of the identifying one, so it is going
# to be correlated
sub setSlave() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	$self->{slave}=1;
}

sub isSlave() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return exists($self->{slave});
}

# addCorrelatedConcept parameters:
#	correlatedConcept: a DCC::Loader::CorrelatableConcept instance, of a concept which is identified by this
sub addCorrelatedConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	Carp::croak('Parameter must be a DCC::Loader::CorrelatableConcept')  unless(defined($correlatedConcept) && ref($correlatedConcept) && $correlatedConcept->isa('DCC::Loader::CorrelatableConcept'));
	Carp::croak('You can only add correlated concepts')  unless(defined($correlatedConcept->concept->idConcept) && $correlatedConcept->concept->idConcept==$self->concept);
	
	$self->{correlatedConcepts} = []   unless(defined($self->{correlatedConcepts}));
	# Slaving it
	$correlatedConcept->setSlave();
	push(@{$self->{correlatedConcepts}},$correlatedConcept);
}

# This method reads the line with the column order from the inputs files
# and maps their positions to their corresponding DCC::Model::Column
# instances from the DCC::Model::Concept used to create this instance
sub readColDesc() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# Do it for the files
	foreach my $p_conceptFile (@{$self->{conceptFiles}}) {
		$p_conceptFile->readColDesc($self->{concept},defined($self->{correlatedConcepts}),$self->isSlave);
	}
	
	# And also for the slave correlated concepts
	#if(defined($self->{correlatedConcepts})) {
	#	foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
	#		$correlatedConcept->readColDesc();
	#	}
	#}
}

# This method sorts the files by the PK and FK columns (only if it is needed)
sub sortCompressed() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# Assuring we have what we need
	$self->readColDesc();

	foreach my $p_conceptFile (@{$self->{conceptFiles}}) {
		$p_conceptFile->sortCompressed();
	}
	
	# And also for the slave correlated concepts
	if(defined($self->{correlatedConcepts})) {
		foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
			$correlatedConcept->sortCompressed();
		}
	}
}

# prepare parameters:
#	(none)
# This method prepares the correlated model files to be optimally inserted
sub prepare() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	foreach my $p_conceptFile (@{$self->{conceptFiles}}) {
		$p_conceptFile->prepare();
	}
	
	# And also for the slave correlated concepts
	if(defined($self->{correlatedConcepts})) {
		foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
			$correlatedConcept->prepare();
		}
	}
}

# This method closes the open filehandles, but it does not erase the sorted temp file(s)
sub close() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	foreach my $p_conceptFile (@{$self->{conceptFiles}}) {
		$p_conceptFile->close();
	}
	
	# And also for the slave correlated concepts
	if(defined($self->{correlatedConcepts})) {
		foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
			$correlatedConcept->close();
		}
	}
}

sub readEntry(;\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_PK = shift;
	
	if($self->isSlave()) {
		
	}
	
	my $H = $self->{H};
	my $line = <$H>;
	unless(eof($H)) {
		chomp($line);
		$self->mapValues(split(/\t/,$line));
		return 1;
	}
	
	return undef;
}

1;
