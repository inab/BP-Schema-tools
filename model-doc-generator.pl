#!/usr/bin/perl -W

# TODO:
#	CV uri
#	Column listing reordering (based on annotations)
#	Document allowed null values
#	Document column name used to fetch values

use strict;
use Carp;
use TeX::Encode;
use Encode;
use File::Copy;
use File::Spec;
use File::Temp;

use FindBin;
use lib "$FindBin::Bin/lib";
use DCC::Model;

sub latex_escape($);
sub latex_format($);
sub genSQL($$);

# Original code obtained from:
# http://ommammatips.blogspot.com.es/2011/01/perl-function-for-latex-escape.html
sub latex_escape_internal($) {
	my $paragraph = shift;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([\$\#&%_])/\\$1/g;
	
	# "Less than" and "greater than"
	$paragraph =~ s/>/\\textgreater/g;
	$paragraph =~ s/</\\textless/g;
	
	# Replace ^ characters with \^{} so that $^F works okay
	$paragraph =~ s/(\^)/\\$1\{\}/g;
	
	# Replace tilde (~) with \texttt{\~{}}
	$paragraph =~ s/~/\\texttt\{\\~\{\}\}/g;
	
	# Now add the dollars around each \backslash
	$paragraph =~ s/(\\backslash)/\$$1\$/g;
	$paragraph =~ s/(\\textgreater)/\$$1\$/g;
	$paragraph =~ s/(\\textless)/\$$1\$/g;
	return $paragraph;
}

sub latex_escape($) {
	my $paragraph = shift;
	
	# Replace a \ with $\backslash$
	# This is made more complicated because the dollars will be escaped
	# by the subsequent replacement. Easiest to add \backslash
	# now and then add the dollars
	$paragraph =~ s/\\/\\backslash/g;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([{}])/\\$1/g;
	
	return latex_escape_internal($paragraph);
}

sub latex_format($) {
	my $paragraph = shift;
	
	# Replace a \ with $\backslash$
	# This is made more complicated because the dollars will be escaped
	# by the subsequent replacement. Easiest to add \backslash
	# now and then add the dollars
	$paragraph =~ s/\\/\\backslash/g;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([{}])/\\$1/g;
	
	$paragraph =~ s/\<br\>/\n\n/g;
	$paragraph =~ s/\<i\>([^<]+)\<\/i\>/\\textit{$1}/msg;
	$paragraph =~ s/\<b\>([^<]+)\<\/b\>/\\textbf{$1}/msg;
	$paragraph =~ s/\<tt\>([^<]+)\<\/tt\>/\\texttt{$1}/msg;
	$paragraph =~ s/\<u\>([^<]+)\<\/u\>/\\textunderscore{$1}/msg;
	$paragraph =~ s/\<a\shref=["']([^>'"]+)['"]\>([^<]+)\<\/a\>/\\href{$1}{$2}/msg;
	
	return latex_escape_internal($paragraph);
}

my %ABSTYPE2SQL = (
	'string' => 'VARCHAR(4096)',
	'integer' => 'INTEGER',
	'decimal' => 'DOUBLE',
	'boolean' => 'BOOL',
	'timestamp' => 'DATETIME',
	'duration' => 'VARCHAR(128)',
	'compound' => 'VARCHAR(4096)',
);

my %COLKIND2ABBR = (
	DCC::Model::ColumnType::IDREF	=>	'I',
	DCC::Model::ColumnType::REQUIRED	=>	'R',
	DCC::Model::ColumnType::DESIRABLE	=>	'D',
	DCC::Model::ColumnType::OPTIONAL	=>	'O'
);

# genSQL parameters:
#	model: a DCC::Model instance, with the parsed model.
#	the path to the SQL output file
sub genSQL($$) {
	my($model,$outfileSQL) = @_;
	
	my $SQL;
	if(open($SQL,'>:utf8',$outfileSQL)) {
		print $SQL '-- Generated from '.$model->projectName.' '.$model->versionString."\n";
		print $SQL '-- '.localtime()."\n";
		# Let's iterate over all the concept domains and their concepts
		my $p_TYPES = $model->types;
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			my $conceptDomainName = $conceptDomain->name;
			foreach my $concept (@{$conceptDomain->concepts}) {
				#my $conceptName = $concept->name;
				my $basename = $conceptDomainName.'_'.$concept->name;
				print $SQL "\n-- ",$concept->fullname;
				print $SQL "\nCREATE TABLE $basename (";
				
				my $columnSet = $concept->columnSet;
				
				my @idColumnNames = @{$columnSet->idColumnNames};

				my $gottable=undef;
				# First, the idref columns
				my @idcolorder=sort(@idColumnNames);
				my @colorder=();
				
				# And then, the others
				my %idcols = map { $_ => undef } @idcolorder;
				foreach my $columnName (@{$columnSet->columnNames}) {
					push(@colorder,$columnName)  unless(exists($idcols{$columnName}));
				}
				@colorder=(@idcolorder,sort(@colorder));
				
				foreach my $column (@{$columnSet->columns}{@colorder}) {
					print $SQL ','  if(defined($gottable));
					
					my $columnType = $column->columnType;
					my $type = $ABSTYPE2SQL{$columnType->type};
					print $SQL "\n\t",$column->name,' ',$type;
					print $SQL ' NOT NULL'  if($columnType->use >= DCC::Model::ColumnType::IDREF);
					if(defined($columnType->default) && ref($columnType->default) eq '') {
						my $default = $columnType->default;
						$default = "'$default'"  if($p_TYPES->{$columnType->type}[DCC::Model::ISNOTNUMERIC]);
						print $SQL ' DEFAULT ',$default;
					}
					$gottable = 1;
				}
				
				# Declaring a primary key
				if(scalar(@idColumnNames)>0) {
					print $SQL ",\n\tPRIMARY KEY (".join(',',@idColumnNames).')'
				}
				
				print $SQL "\n);\n\n";
			}
			
			# And now, the restrictions
			foreach my $concept (@{$conceptDomain->concepts}) {
				my $basename = $conceptDomainName.'_'.$concept->name;
				if(defined($concept->parentConcept)) {
					my $parentConcept = $concept->parentConcept;
					my $refColnames = $parentConcept->columnSet->idColumnNames;
					my $parentBasename = $conceptDomainName.'_'.$parentConcept->name;
					
					print $SQL "\n-- ",$concept->fullname, " foreign keys";
					print $SQL "\nALTER TABLE $basename ADD FOREIGN KEY (",join(',',@{$refColnames}),")";
					print $SQL "\nREFERENCES $parentBasename(".join(',',@{$refColnames}).");\n";
				}
			}
		}
		
		close($SQL);
	} else {
		Carp::croak("Unable to create output file $outfileSQL");
	}
}

# assemblePDF parameters:
#	templateFile: The LaTeX template file to be used to generate the PDF
#	model: DCC::Model instance
#	bodyFile: The temporal file where the generated documentation has been written.
#	outputFile: The output PDF file.
sub assemblePDF($$$$) {
	my($templateFile,$model,$bodyFile,$outputFile) = @_;
	
	unless(-f $templateFile && -r $templateFile) {
		die "ERROR: Unable to find readable template LaTeX file $templateFile\n";
	}

	my($bodyDir,$bodyName);
	if(-f $bodyFile && -r $bodyFile) {
		my $absbody = File::Spec->rel2abs($bodyFile);
		$bodyDir = dirname($absbody);
		$bodyName = basename($absbody);
	} else {
		die "ERROR: Unable to find readable body LaTeX file $bodyFile\n";
	}
	
	my $docsDir = $model->documentationDir();
	
	my $annotationSet = $model->annotations;
	my $annotations = $annotationSet->hash;
	
	# The name of the overview file should come from here
	my $overviewFile = $annotations->{overviewDoc};
	
	unless(File::Spec->file_name_is_absolute($overviewFile)) {
		$overviewFile = File::Spec->catfile($docsDir,$overviewFile);
	}
	
	my($overviewDir,$overviewName);
	if(-f $overviewFile && -r $overviewFile) {
		my $absoverview = File::Spec->rel2abs($overviewFile);
		$overviewDir = dirname($absoverview);
		$overviewName = basename($absoverview);
	} else {
		die "ERROR: Unable to find readable overview LaTeX file $overviewFile (declared in model!)\n";
	}
	
	# Storing the document generation parameters
	my @params = map { ['ANNOT'.$_,encode('latex',$annotations->{$_})] } keys(%{$annotations});
	push(@params,['projectName',$model->projectName],['schemaVer',$model->versionString],['modelSHA',$model->modelSHA1],['CVSHA',$model->CVSHA1]);
	
	# Final slashes in directories are VERY important for subimports!!!!!!!! (i.e. LaTeX is dumb)
	push(@params,['INCLUDEoverviewdir',$overviewDir.'/'],['INCLUDEoverviewname',$overviewName]);
	push(@params,['INCLUDEbodydir',$bodyDir.'/'],['INCLUDEbodyname',$bodyName]);
	
	# Setting the jobname and the jobdir, pruning the .pdf extension from the output
	my $absjob = File::Spec->rel2abs($outputFile);
	my $jobDir = dirname($absjob);
	my $jobName = basename($absjob,'.pdf');

	
	# And now, let's prepare the command line
	my @pdflatexParams = (
		'-interaction=batchmode',
		'-jobname',$jobName,
		'-output-directory',$jobDir,
		join(' ',map { '\gdef\\'.$_->[0].'{'.$_->[1].'}' } @params).' \input{'.$templateFile.'}'
	);
	
	print STDERR "[DOCGEN] => ",join(' ','pdflatex',@pdflatexParams),"\n";
	
	# exit 0;
	
	my $follow = 1;
	foreach my $it (1..5) {
		if(system('pdflatex','-draftmode',@pdflatexParams)!=0) {
			$follow = undef;
			last;
		}
	}
	if(defined($follow)) {
		system('pdflatex',@pdflatexParams);
	}
}

my $TOCFilename = 'toc.latex';
my $NotesFilename = 'notes.latex';

my %COMMANDS = (
	'file' => 'subsection',
	'featureType' => undef,
	'fileType' => undef
);

my %CVCOMMANDS = (
	'file' => 'section',
	'header' => undef,
	'disposition' => undef,
	'version' => undef,
);

sub printDoc($$;$) {
	my($O,$text,$command)=@_;
	
	if(defined($command) && length($command)>0) {
		my $latex;
		if(exists($COMMANDS{$command})) {
			$latex = $COMMANDS{$command};
		} else {
			$latex = $command;
		}
		print $O "\\$latex\{",latex_format($text),"\}\n"  if(defined($latex));
	} else {
		print $O latex_format($text),"\n\n";
	}
}

# processInlineCVTable parameters:
#	CV: a DCC::Model::CV instance
# it returns a LaTeX formatted table
sub processInlineCVTable($) {
	my($CV)=@_;
	
	my $output='';
	
	# First, the embedded documentation
	foreach my $documentation (@{$CV->description}) {
		$output .= latex_format($documentation)."\n";
	}
	# TODO: process the annotations
	
	$output .= "\n";
	if($CV->isLocal) {
		$output .= '\begin{tabular}{r@{ = }p{0.4\textwidth}}'."\n";
		#$output .= '\begin{tabular}{r@{ = }l}'."\n";

		my $CVhash = $CV->CV;
		foreach my $key (@{$CV->order}) {
			$output .= join(' & ','\textbf{'.latex_escape($key).'}',latex_escape($CVhash->{$key}))."\\\\\n";
		}
		$output .= '\end{tabular}';
	} else {
		# Is it an external CV
		$output .= '\textit{(See ';
		
		my @extCVs = @{$CV->filename};
		my $extpos = 1;
		foreach my $extCV (@extCVs) {
			if($extpos > 1) {
				$output .= ($extpos == scalar(@extCVs))?' and ':', ';
			}
			my $uri = $extCV->uri;
			my $docURI = $extCV->docURI;
			my $cvuri = undef;
			# Do we have external documentation?
			if(defined($docURI)) {
				$cvuri = $docURI->as_string;
			} else {
				$cvuri = $uri->as_string;
			}
			
			$output .= '\url{'.$cvuri.'}';
			$extpos++;
		}
		$output .= ')}';
	}
	$output .= "\n";
	
	return $output;
}

# printCVTable parameters:
#	CV: A named DCC::Model::CV instance (the controlled vocabulary)
#	O: The output filehandle where the documentation about the
#		controlled vocabulary is written.
sub printCVTable($$) {
	my($CV,$O)=@_;
	
	my $cvname = $CV->name;
	
	my $annotationSet = $CV->annotations;
	my $annotations = $annotationSet->hash;
	
	# The caption
	my $caption = undef;
	if(exists($annotations->{'file'})) {
		$caption = $annotations->{'file'};
	} else {
		$caption = "CV Table $cvname";
	}
	print $O "\\section{",latex_escape($caption),"} \\label{cvsec:$cvname}\n";
	
	my @header = ();
	
	# The header names used by the LaTeX table
	if(exists($annotations->{'header'})) {
		@header = split(/\t/,$annotations->{'header'},2);
	} else {
		@header = ('Key','Description');
	}
	$header[0] = latex_format($header[0]);
	$header[1] = latex_format($header[1]);
	
	# Printing embedded documentation
	foreach my $documentation (@{$CV->description}) {
		print $O latex_format($documentation),"\n";
	}
	
	# And annotations
	foreach my $annotName (@{$annotationSet->order}) {
		next  if($annotName eq 'file' || $annotName eq 'header');
		
		my $latex = undef;
		if(exists($CVCOMMANDS{$annotName})) {
			$latex = $CVCOMMANDS{$annotName};
		} else {
			$latex = $annotName;
		}
		
		print $O "\\$latex\{",latex_format($annotations->{$annotName}),"\}\n"  if(defined($latex));
	}

	# Table header
	print $O <<EOF ;
\\renewcommand{\\cvKey}{$header[0]}
\\renewcommand{\\cvDesc}{$header[1]}
EOF

	if($CV->isLocal) {
		print $O "\\topcaption{",latex_format($caption),"} \\label{cv:$cvname}\n";
		print $O <<'EOF' ;
\tablefirsthead{\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline\hline }

\tablehead{\multicolumn{2}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline }

\tablelasthead{\multicolumn{2}{c}{{\bfseries \tablename\ \thetable{} -- concluded from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline }

\tabletail{\hline
\multicolumn{2}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline}

\tablelasttail{\hline\hline}

\begin{center}
	\arrayrulecolor{DarkOrange}
	\begin{xtabular}{|r|p{0.5\textwidth}|}
EOF
	
		my $CVhash = $CV->CV;
		foreach my $cvKey (@{$CV->order}) {
			print $O join(' & ',latex_escape($cvKey),latex_escape($CVhash->{$cvKey})),'\\\\ \hline',"\n";
		}
		
		# Table footer
		print $O <<EOF ;
	\\end{xtabular}
\\end{center}
EOF
	} else {
		# Remote CVs
		# Is it an external CV
		
		my @extCVs = @{$CV->filename};
		print $O "\n",'\textit{(See '.((scalar(@extCVs)>1)?'them':'it').' at ';
		my $extpos = 1;
		foreach my $extCV (@extCVs) {
			if($extpos > 1) {
				print $O ($extpos == scalar(@extCVs))?' and ':', ';
			}
			my $uri = $extCV->uri;
			my $docURI = $extCV->docURI;
			my $cvuri = undef;
			# Do we have external documentation?
			if(defined($docURI)) {
				$cvuri = $docURI->as_string;
			} else {
				$cvuri = $uri->as_string;
			}
			
			print $O '\url{'.$cvuri.'}';
			$extpos++;
		}
		print $O ')}';
	}
	
	# And now, the aliases
	if(scalar(@{$CV->aliasOrder}) > 0) {
		print $O "\\topcaption{",latex_format($caption)," aliases} \\label{cv:$cvname:alias}\n";
		print $O <<'EOF' ;
\tablefirsthead{\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Alias}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline\hline }

\tablehead{\multicolumn{3}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Alias}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline }

\tablelasthead{\multicolumn{3}{c}{{\bfseries \tablename\ \thetable{} -- concluded from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Alias}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline }

\tabletail{\hline \multicolumn{3}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline}

\tablelasttail{\hline\hline}

\begin{center}
	\arrayrulecolor{DarkOrange}
	\begin{xtabular}{|r|c|p{0.5\textwidth}|}
EOF
		
		my $aliashash = $CV->alias;
		foreach my $aliasKey (@{$CV->aliasOrder}) {
			my $alias = $aliashash->{$aliasKey};
			my $termStr = undef;
			
			if(scalar(@{$alias->order}) > 1) {
				$termStr = '\begin{tabular}{l}'.join(' \\\\ ',map { latex_escape($_) } @{$alias->order}).'\end{tabular}';
			} else {
				$termStr = $alias->order->[0];
			}
			
			my $descStr = '';
			if(scalar(@{$alias->description}) > 1) {
				$descStr = '{'.join(' \\\\ ',map { latex_escape($_) } @{$alias->description}).'}';
			} elsif(scalar(@{$alias->description}) == 1) {
				$descStr = $alias->description->[0];
			}
			print $O join(' & ', latex_escape($aliasKey), $termStr, $descStr),'\\\\ \hline',"\n";
		}

		# Table footer
		print $O <<EOF ;
		\\end{xtabular}
\\end{center}
EOF
	}
}

# printConceptDomain parameters:
#	model: A DCC::Model instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, from the model
#	O: The filehandle where to print the documentation about the concept domain
sub printConceptDomain($$$) {
	my($model,$conceptDomain,$O)=@_;
	
	# The title
	# TODO: consider using encode('latex',...) for the content
	print $O '\\section{'.latex_format($conceptDomain->fullname).'}\\label{fea:'.$conceptDomain->name."}\n";
	
	# The additional documentation
	# The concept domain holds its additional documentation in a subdirectory with
	# the same name as the concept domain
	my $domainDocDir = File::Spec->catfile($model->documentationDir,$conceptDomain->name);
	my $docfile = File::Spec->catfile($domainDocDir,$TOCFilename);
	
	if(-f $docfile && -r $docfile){
		# Full paths, so import instead of subimport
		print $O "\\import*\{$domainDocDir/\}\{$TOCFilename\}\n";
	}
	
	# Let's iterate over the concepts of this concept domain
	foreach my $concept (@{$conceptDomain->concepts}) {
		my $columnSet = $concept->columnSet;
		
		# The embedded documentation of each concept
		my $caption = $concept->fullname;
		print $O '\\subsection{'.latex_format($caption)."}\n";
		foreach my $documentation (@{$concept->description}) {
			printDoc($O,$documentation);
		}
		my $annotationSet = $concept->annotations;
		my $annotations = $annotationSet->hash;
		foreach my $annotKey (@{$annotationSet->order}) {
			my $tcap = printDoc($O,$annotations->{$annotKey},$annotKey);
			$caption = $tcap  if(defined($tcap));
		}
		
		# The table header
		$caption = latex_format($caption);
		my $entry = $conceptDomain->name.'_'.$concept->name;
		print $O "\\topcaption{$caption} \\label{tab:$entry}\n";
		print $O <<'EOF' ;
\tablefirsthead{\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
\hline\hline }

\tablehead{\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
\hline }

\tablelasthead{\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- concluded from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
\hline }

\tabletail{\hline
\multicolumn{4}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline}

\tablelasttail{\hline\hline}

\begin{center}
	\arrayrulecolor{DarkOrange}
	\begin{xtabular}{|l|c|c|p{0.5\textwidth}|}
EOF
		
		# Determining the order of the columns
		# First, the idref columns
		my @idcolorder=sort(@{$columnSet->idColumnNames});
		my @colorder=();
		
		# And then, the others
		my %idcols = map { $_ => undef } @idcolorder;
		foreach my $columnName (@{$columnSet->columnNames}) {
			push(@colorder,$columnName)  unless(exists($idcols{$columnName}));
		}
		@colorder=(@idcolorder,sort(@colorder));

		# Now, let's print the documentation of each column
		foreach my $column (@{$columnSet->columns}{@colorder}) {
			# Preparing the documentation
			my $description='';
			foreach my $documentation (@{$column->description}) {
				$description = "\n\n" if(length($description)>0);
				$description .= latex_format($documentation);
			}
			
			# The comment about the values
			my $values='';
			if(exists($column->annotations->hash->{values})) {
				$values = '\\textit{'.latex_format($column->annotations->hash->{values}).'}';
			}
			
			# Now the possible CV
			my $columnType = $column->columnType;
			my $restriction = $columnType->restriction;
			if(ref($restriction) eq 'DCC::Model::CV') {
				# Is it an anonymous CV?
				if(!defined($restriction->name) || (exists($restriction->annotations->hash->{'disposition'}) && $restriction->annotations->hash->{disposition} eq 'inline')) {
					$values .= processInlineCVTable($restriction);
				} else {
					my $cv = $restriction->name;
					$values .= "\n".'\textit{(See \hyperref[cvsec:'.$cv.']{'.($restriction->isLocal?'CV':'external CV description').' \ref*{cvsec:'.$cv.'}})}';
				}
			}
			
			# What it goes to the column type column
			my @colTypeLines = ('\textbf{'.latex_escape($columnType->type.('[]' x length($columnType->arraySeps))).'}');
			
			push(@colTypeLines,'\textit{'.latex_escape($restriction->template).'}')  if(ref($restriction) eq 'DCC::Model::CompoundType');
			
			push(@colTypeLines,'\textcolor{gray}{(array seps \textbf{\color{black}'.latex_escape($columnType->arraySeps).'})}')  if(defined($columnType->arraySeps));
			
			# Stringify it!
			my $colTypeStr = '\\texttt{'.(
				(scalar(@colTypeLines)>1)?
#					'\begin{tabular}{l}'.join(' \\\\ ',map { latex_escape($_) } @colTypeLines).'\end{tabular}'
#					'\begin{minipage}[t]{8em}'.join(' \\\\ ',@colTypeLines).'\end{minipage}'
					'\pbox[t]{10cm}{\relax\ifvmode\centering\fi'."\n".join(' \\\\ ',@colTypeLines).'}'
					:
					$colTypeLines[0]
				).'}';
			
			print $O join(' & ',latex_escape($column->name),$colTypeStr,'\\texttt{'.$COLKIND2ABBR{$columnType->use}.'}',$description."\n\n".$values),'\\\\ \hline',"\n";
		}
		# End of the table!
		print $O <<EOF ;
	\\end{xtabular}
\\end{center}
EOF
	}
	
	# And at last, optional notes about this!
	my $notesfile = File::Spec->catfile($domainDocDir,$NotesFilename);
	
	if(-f $notesfile && -r $notesfile){
		print $O '\\subsection{Further notes on '.latex_format($conceptDomain->fullname)."}\n";
		# Full paths, so import instead of subimport
		print $O "\\import*\{$domainDocDir/\}\{$NotesFilename\}\n";
	}
	
}

if(scalar(@ARGV)>=3) {
	my($modelFile,$templateDocFile,$out)=@ARGV;
	
	binmode(STDERR,':utf8');
	binmode(STDOUT,':utf8');
	my $model = undef;
	
	eval {
		$model = DCC::Model->new($modelFile);
	};
	
	if($@) {
		print STDERR "ERROR: Model loading and validation failed. Reason: ".$@,"\n";
		exit 2;
	}
	
	# In case $out is a directory, then fill-in the other variables
	my $outfilePDF = undef;
	my $outfileSQL = undef;
	my $outfileXML = undef;
	if(-d $out) {
		my (undef,undef,undef,$day,$month,$year) = localtime();
		# Doing numerical adjustments
		$year += 1900;
		$month ++;
		my $thisdate = sprintf("%d%.2d%.2d",$year,$month,$day);
		
		my $outfileRoot = File::Spec->catfile($out,join('-',$model->projectName,'data_model',$model->versionString,$thisdate));
		$outfilePDF = $outfileRoot . '.pdf';
		$outfileSQL = $outfileRoot . '.sql';
		$outfileXML = $outfileRoot . '.xml';
	} else {
		$outfilePDF = $out;
		$outfileSQL = $out.'.sql';
	}
	
	# Copying the original XML (if it is reasonable)
	copy($modelFile,$outfileXML)  if(defined($outfileXML));
	
	# Generating the SQL file for BioMart
	genSQL($model,$outfileSQL);
	
	# Generating the document to be included in the template
	my $TO = File::Temp->new();
	
	print $TO '\chapter{DCC Submission Tabular Formats}\label{ch:tabFormat}',"\n";
	
	# Let's iterate over all the concept domains and their concepts
	foreach my $conceptDomain (@{$model->conceptDomains}) {
		printConceptDomain($model,$conceptDomain,$TO);
	}

	print $TO "\\appendix\n";
	print $TO "\\chapter{Controlled Vocabularies}\n";
	
	foreach my $CV (@{$model->namedCVs}) {
		unless(exists($CV->annotations->hash->{disposition}) && $CV->annotations->hash->{disposition} eq 'inline') {
			printCVTable($CV,$TO);
		}
	}
	
	# Now, let's generate the documentation!
	assemblePDF($templateDocFile,$model,$TO->filename,$outfilePDF);
} else {
	print STDERR "This program takes as input the model (in XML), the LaTeX template and the output file\n";
	exit 1;
}
