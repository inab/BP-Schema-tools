#!/usr/bin/perl -W

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

package BP::Loader::CorrelatableConcept::File;

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
#	concept: a BP::Model::Concept
#	isIdentifying: 1 or undef
#	isSlave: 1 or undef
# This method reads the line with the column order from the input file
# and maps their positions to their corresponding BP::Model::Column
# instances from the BP::Model::Concept used to create this instance
sub readColDesc($$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	unless(exists($self->{coldesc})) {
		my $concept = shift;
		
		Carp::croak('Parameter must be a BP::Model::Concept')  unless(defined($concept) && ref($concept) && $concept->isa('BP::Model::Concept'));
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return exists($self->{hasUnknown});
}

# getColumnMap parameters:
#	(none)
# It returns the position -> column name mapping
sub getColumnMap() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{colMap};
}

# generatePipeSentence parameters:
#	referenceFile: a BP::Loader::CorrelatableConcept::File, whose columns are used as reference (optional)
sub generatePipeSentence(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_PK = shift;
	
	return __keyMatches(@{$p_PK},@{$self->{FKkeyvals}});
}

sub cleanup() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	delete($self->{entry});
	delete($self->{PKkeyvals});
	delete($self->{FKkeyvals});
}


package BP::Loader::CorrelatableConcept;

# Constructor parameters:
#	concept: a BP::Model::Concept
#	conceptFiles: the file(name)s with the content
sub new($@) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	# my $self  = $class->SUPER::new();
	my $self = {};
	bless($self,$class);
	
	$self->{concept} = shift;
	my @conceptFiles = @_;
	@{$self->{conceptFiles}} = map { BP::Loader::CorrelatableConcept::File->new($_) } @conceptFiles;
	$self->{correlatedConcepts} = undef;

	return $self;
}

# It returns a BP::Model::Concept instance
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
#	correlatedConcept: a BP::Loader::CorrelatableConcept instance, of a concept which is identified by this
sub addCorrelatedConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	Carp::croak('Parameter must be a BP::Loader::CorrelatableConcept')  unless(defined($correlatedConcept) && ref($correlatedConcept) && $correlatedConcept->isa('BP::Loader::CorrelatableConcept'));
	Carp::croak('You can only add correlated concepts')  unless(defined($correlatedConcept->concept->idConcept) && $correlatedConcept->concept->idConcept==$self->concept);
	
	$self->{correlatedConcepts} = []   unless(defined($self->{correlatedConcepts}));
	# Slaving it
	$correlatedConcept->setSlave();
	push(@{$self->{correlatedConcepts}},$correlatedConcept);
}

# This method reads the line with the column order from the inputs files
# and maps their positions to their corresponding BP::Model::Column
# instances from the BP::Model::Concept used to create this instance
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

sub __inpipe2Str(\@) {
	my($inpipe)=@_;
	
	# Translating the pipes structures into command-line
	my $inpipeStr = join(' && ', map {
		join(' | ', map {
			"'".join("' '",@{$_})."'";
		} @{$_});
	} @{$inpipe});
	
	$inpipeStr = '( '.$inpipeStr.' )'  if(scalar(@{$inpipe}) > 1);
	
	return $inpipeStr;
}

# Static internal method
# __SortCompressed parameters:
#	inpipe: The input streams
#	p_keypos: The position of the ordering keys
# It returns a File::Temp instance with the sorted file, compressed with gzip
sub __SortCompressed(\@\@) {
	my($inpipe,$p_keypos) = @_;
	
	my $sortkeys = join(' ',map { my $p = $_ + 1 ; '-k'.$p.','.$p } @{$p_keypos});
	
	my $tmpout = File::Temp->new();
	my $tmpoutfilename = $tmpout->filename();
	
	# This command line reorders the lines
	# keeping the comments and the header at the beginning of the file
	# and sorting by the input keys
	# my $cmd = "('$GREP' '^#' '$infile' ; '$GREP' -v '^#' '$infile' | head -n 1 ; '$GREP' -v '^#' '$infile' | tail -n +2 | '$SORT' --parallel=$NUMCPUS -S 50% $sortkeys ) | gzip -9c > '$tmpoutfilename'";
	
	my $inpipeStr = __inpipe2Str(@{$inpipe});
	
	my $cmd = "$inpipeStr | '$SORT' --parallel=$NUMCPUS -S 50% $sortkeys | gzip -9c > '$tmpoutfilename'";
	
	system($cmd);
	
	return $tmpout;
}

# sortCompressed parameters:
#	(none)
# This method sorts the files by the PK and FK columns (only if it is needed)
sub sortCompressed() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# Assuring we have what we need
	$self->readColDesc();
	
	unless(exists($self->{__compressed})) {
		my $referenceConceptFile = $self->{conceptFiles}->[0];
		$self->{referenceConceptFile} = $referenceConceptFile;
		# We only have to sort when we have slave correlated concepts
		# or when this correlatable concept is already a slave
		if(defined($self->{correlatedConcepts}) || $self->isSlave()) {
			# First, the internal sort
			my @sortPipes = ();
			push(@sortPipes,map { $_->generatePipeSentence($referenceConceptFile) } @{$self->{conceptFiles}});
			
			# Primary and foreign key positions on the reference concept file
			my $PKkeypos = $referenceConceptFile->{PKkeypos};
			my $FKkeypos = $referenceConceptFile->{FKkeypos};
			
			my $isIdentifying = shift;
			my $isSlave = shift;
	
			# Identifying whether we have to do two sorts
			my $distinctFK = 1;
			if($FKkeypos && $PKkeypos) {
				my $maxSteps = scalar(@{$FKkeypos});
				if(scalar(@{$PKkeypos}) >= $maxSteps) {
					$distinctFK = undef;
					foreach my $keypos (0..($maxSteps-1)) {
						if($PKkeypos->[$keypos] != $FKkeypos->[$keypos]) {
							$distinctFK = 1;
							Carp::croak('FATAL ERROR: Model does not correlate the identifying concept keys with the foreign keys!');
							last;
						}
					}
				}
			}

			$self->{PKsortedConceptFile} = $PKkeypos ? __SortCompressed(@sortPipes,@{$PKkeypos}) : undef;

			if($FKkeypos) {
				$self->{FKsortedConceptFile} = $distinctFK ? __SortCompressed(@sortPipes,@{$FKkeypos}) : $self->{PKsortedConceptFile};
			} else {
				$self->{FKsortedConceptFile} = undef;
			}
			
			$self->{__compressed} = 1;
			
			# And also for the slave correlated concepts
			if(defined($self->{correlatedConcepts})) {
				foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
					$correlatedConcept->sortCompressed();
				}
			}
		} else {
			$self->{__compressed} = undef;
		}
	}
}

# openFiles parameters:
#	(none)
# This method prepares and opens the correlated model files to be optimally inserted
# prefilling the internal buffers
sub openFiles() {
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
		
		my $referenceConceptFile = exists($self->{referenceConceptFile}) ? $self->{referenceConceptFile} : undef;
		if(defined($self->{__compressed})) {
			# All the content is already preprocessed
			my $sortedFilename = $self->{PKsortedConceptFile} ? $self->{PKsortedConceptFile}->filename() : $self->{FKsortedConceptFile}->filename();
			if(open(my $H,'-|','gunzip','-c',$sortedFilename)) {
				$self->{H} = $H;
			} else {
				Carp::croak('ERROR: Unable to open sorted temp file associated to concept '.$self->{concept}->_jsonId);
			}
		} elsif(scalar(@{$self->{conceptFiles}}) > 0) {
			# Content is spread over several files. Reclaim pipe expressions
			my @sortPipes = ();
			push(@sortPipes,map { $_->generatePipeSentence($referenceConceptFile) } @{$self->{conceptFiles}});
			
			my $inpipeStr = __inpipe2Str(@sortPipes);
			
			if(open(my $H,'-|',$inpipeStr)) {
				$self->{H} = $H;
			} else {
				Carp::croak('ERROR: Unable to open pipe '.$inpipeStr.' associated to concept '.$self->{concept}->_jsonId);
			}
		} elsif(open(my $H,'<',$referenceConceptFile->filename())) {
			# Single file
			# First, skip initial comments and header
			$self->{H} = $H;
			while(my $line = <$H>) {
				next  if(substr($line,0,1) eq '#');
				
				# The header is here, so go out
				last;
			}
		} else {
			Carp::croak('ERROR: Unable to open file '.$referenceConceptFile->filename());
		}
		
		# As this is too expensive, we are doing it only once!
		$self->{__prepared} = 1;
		
		# First read, prefilling so internal buffers
		$self->nextLine();
		
		# And now, the slave correlated concepts
		foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
			$correlatedConcept->openFiles();
		}
	}
}

# This method closes the open filehandles, but it does not erase the sorted temp file
sub closeFiles() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(exists($self->{__prepared})) {
		# The slave correlated concepts
		foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
			$correlatedConcept->closeFiles();
		}
		
		close($self->{H});
		
		delete($self->{H});
		$self->{referenceConceptFile}->cleanup();
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
			$self->{referenceConceptFile}->mapValues(split(/\t/,$line));
			return 1;
		} else {
			$self->{eof} = 1;
		}
	}
	
	return undef;
}

# readEntry parameters:
#	readahead: Number of entries to read ahead, when there is no correlation to apply
#	p_parentPK: reference to an array with the PK values from the parent to compare
#		for correlation. Values must appear in the same order as the declared PK (and FK)
sub readEntry(;$\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# No processing when there is nothing more to say
	return undef  if(exists($self->{eof}));
	
	# The input parameters
	my $readahead = shift;
	$readahead = 1  unless(defined($readahead) && int($readahead)>0);
	my $p_parentPK = shift;
	
	my @entries = ();
	my @orphan = ();
	
	my $assembleEntry = sub() {
		my($entry,$p_PK) = $self->{referenceConceptFile}->entry();
		
		# Now, let's read from the slaves
		if(defined($self->{correlatedConcepts})) {
			foreach my $correlatedConcept (@{$self->{correlatedConcepts}}) {
				my $entorf = $correlatedConcept->readEntry(undef,$p_PK);
				if(defined($entorf) && scalar(@{$entorf->[0]}) > 0) {
					my $key = $correlatedConcept->concept->key();
					unless(exists($entry->{$key})) {
						$entry->{$key} = $entorf->[0];
					} else {
						push(@{$entry->{$key}},@{$entorf->[0]});
					}
				}
			}
		}
		
		return $entry;
	};
	
	if($self->isSlave()) {
		my $retval = 0;
		while(($retval = $self->{referenceConceptFile}->keyMatches(@{$p_parentPK}))>=0) {
			my $entry = $assembleEntry->();
			
			# Let's save the entry in the corresponding batch
			if($retval >0) {
				push(@orphan,$entry);
			} else {
				push(@entries,$entry);
			}
			
			# We read next line, but we stop on eof
			last  unless($self->nextLine());
		}
	} else {
		# Let's read with no remorse
		foreach my $record (1..$readahead) {
			my $entry = $assembleEntry->();
			
			# Let's save the entry in the batch
			push(@entries,$entry);
			
			# We read next line, but we stop on eof
			last  unless($self->nextline());
		}
	}
	
	return [\@entries,\@orphan];
}

1;
