#!/usr/bin/perl -W

# TODO:
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


use constant RELEASE => 1;


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
	my $par = shift;
	
	my $paragraph = $par;
	# Let's serialize this nodeset
	if(ref($par) eq 'ARRAY') {
		$paragraph = join('',map { ($_->can('toString'))?$_->toString(0):$_ } @{$par});
	}
	
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
	my $par = shift;
	
	my $paragraph = $par;
	# Let's serialize this nodeset
	if(ref($par) eq 'ARRAY') {
		$paragraph = join('',map { $_->toString(0)} @{$par});
	}
	
	# Replace a \ with $\backslash$
	# This is made more complicated because the dollars will be escaped
	# by the subsequent replacement. Easiest to add \backslash
	# now and then add the dollars
	$paragraph =~ s/\\/\\backslash/g;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([{}])/\\$1/g;
	
	# HTML content
	$paragraph =~ s/\<br *\/?\>/\n\n/g;
	$paragraph =~ s/\<i\>([^<]+)\<\/i\>/\\textit{$1}/msg;
	$paragraph =~ s/\<b\>([^<]+)\<\/b\>/\\textbf{$1}/msg;
	$paragraph =~ s/\<tt\>([^<]+)\<\/tt\>/\\texttt{$1}/msg;
	$paragraph =~ s/\<u\>([^<]+)\<\/u\>/\\textunderscore{$1}/msg;
	$paragraph =~ s/\<a\s+href=["']([^>'"]+)['"]\>([^<]+)\<\/a\>/\\href{$1}{$2}/msg;
	
	# XHTML content
	$paragraph =~ s/\<[^:]+:br *\/\>/\n\n/g;
	$paragraph =~ s/\<[^:]+:i\>([^<]+)\<\/[^:]+:i\>/\\textit{$1}/msg;
	$paragraph =~ s/\<[^:]+:b\>([^<]+)\<\/[^:]+:b\>/\\textbf{$1}/msg;
	$paragraph =~ s/\<[^:]+:tt\>([^<]+)\<\/[^:]+:tt\>/\\texttt{$1}/msg;
	$paragraph =~ s/\<[^:]+:u\>([^<]+)\<\/[^:]+:u\>/\\textunderscore{$1}/msg;
	$paragraph =~ s/\<[^:]+:a\s+href=["']([^>'"]+)['"]\>([^<]+)\<\/[^:]+:a\>/\\href{$1}{$2}/msg;
	
	return latex_escape_internal($paragraph);
}

my %ABSTYPE2SQL = (
	'string' => 'VARCHAR(4096)',
	'integer' => 'INTEGER',
	'decimal' => 'DOUBLE PRECISION',
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

# fancyColumnOrdering parameters:
#	concept: a DCC::Concept instance
# It returns an array with the column names from the concept in a fancy
# order, based on several criteria, like annotations
sub fancyColumnOrdering($) {
	my($concept)=@_;
	
	my @colorder = ();
	
	my $columnSet = $concept->columnSet;

	# First, the idref columns, alphabetically ordered
	my @idcolorder=sort(@{$columnSet->idColumnNames});

	# And then, the others
	my %idcols = map { $_ => undef } @idcolorder;
	foreach my $columnName (@{$columnSet->columnNames}) {
		push(@colorder,$columnName)  unless(exists($idcols{$columnName}));
	}
	
	return (@idcolorder,sort(@colorder));
}

# genSQL parameters:
#	model: a DCC::Model instance, with the parsed model.
#	the path to the SQL output file
sub genSQL($$) {
	my($model,$outfileSQL) = @_;
	
	my $SQL;
	if(open($SQL,'>:utf8',$outfileSQL)) {
		print $SQL '-- Generated from '.$model->projectName.' '.$model->versionString."\n";
		print $SQL '-- '.localtime()."\n";
		
		# Needed later for foreign keys
		my @fks = ();

		# Let's iterate over all the concept domains and their concepts
		my $p_TYPES = $model->types;
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			# Skipping abstract concept domains
			next  if(RELEASE && $conceptDomain->isAbstract);
			
			my $conceptDomainName = $conceptDomain->name;
			
			my %pcon = ();
			foreach my $concept (@{$conceptDomain->concepts}) {
				#my $conceptName = $concept->name;
				my $basename = $conceptDomainName.'_'.$concept->name;

				my %fkselemrefs = ();
				my @fkselem = ($basename,$concept,\%fkselemrefs);
				my $fksinit = undef;

				print $SQL "\n-- ",$concept->fullname;
				print $SQL "\nCREATE TABLE $basename (";
				
				
				my @colorder = fancyColumnOrdering($concept);
				my $columnSet = $concept->columnSet;
				my $gottable=undef;
				
				foreach my $column (@{$columnSet->columns}{@colorder}) {
					# Is it involved in a foreign key outside the relatedConcept system?
					if(defined($column->refColumn) && !defined($column->relatedConcept)) {
						$fksinit = 1;
						
						my $refConcept = $column->refConcept;
						my $refBasename = $refConcept->conceptDomain->name . '_' . $refConcept->name;
						
						$fkselemrefs{$refBasename} = [$refConcept,[]]  unless(exists($fkselemrefs{$refBasename}));
						
						push(@{$fkselemrefs{$refBasename}[1]}, $column);
					}
					
					# Let's print
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
				
				push(@fks,\@fkselem)  if(defined($fksinit));
				
				# Declaring a primary key (if any!)
				my @idColumnNames = @{$columnSet->idColumnNames};
				
				if(scalar(@idColumnNames)>0) {
					print $SQL ",\n\tPRIMARY KEY (".join(',',@idColumnNames).')';
					$pcon{$basename} = undef;
				}
				
				print $SQL "\n);\n\n";
			}
			
			## Now, the FK restrictions from inheritance
			#foreach my $concept (@{$conceptDomain->concepts}) {
			#	my $basename = $conceptDomainName.'_'.$concept->name;
			#	if(defined($concept->idConcept)) {
			#		my $idConcept = $concept->idConcept;
			#		my $refColnames = $idConcept->columnSet->idColumnNames;
			#		my $idBasename = $conceptDomainName.'_'.$idConcept->name;
			#		
			#		# Referencing only concepts with keys
			#		if(exists($pcon{$idBasename})) {
			#			print $SQL "\n-- ",$concept->fullname, " foreign keys";
			#			print $SQL "\nALTER TABLE $basename ADD FOREIGN KEY (",join(',',@{$refColnames}),")";
			#			print $SQL "\nREFERENCES $idBasename(".join(',',@{$refColnames}).");\n";
			#		}
			#	}
			#}
		}
			
		# Now, the FK restrictions from identification relations
		foreach my $p_fks (@fks) {
			my($basename,$concept,$p_fkconcept) = @{$p_fks};
			
			print $SQL "\n-- ",$concept->fullname, " foreign keys from inheritance";
			foreach my $relatedBasename (keys(%{$p_fkconcept})) {
				my $p_columns = $p_fkconcept->{$relatedBasename}[1];
				
				print $SQL "\nALTER TABLE $basename ADD FOREIGN KEY (",join(',',map { $_->name } @{$p_columns}),")";
				print $SQL "\nREFERENCES $relatedBasename(".join(',',map { $_->refColumn->name } @{$p_columns}).");\n";
			}
		}

		# And now, the FK restrictions from related concepts
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			next  if(RELEASE && $conceptDomain->isAbstract);
			
			my $conceptDomainName = $conceptDomain->name;
			
			foreach my $concept (@{$conceptDomain->concepts}) {
				#my $conceptName = $concept->name;
				my $basename = $conceptDomainName.'_'.$concept->name;
				
				# Let's visit each concept!
				if(scalar(@{$concept->relatedConcepts})>0) {
					print $SQL "\n-- ",$concept->fullname, " foreign keys from related-to";
					foreach my $relatedConcept (@{$concept->relatedConcepts}) {
						# Skipping foreign keys to abstract concepts
						next  if(RELEASE && $relatedConcept->concept->conceptDomain->isAbstract);
						
						my $refBasename = (defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName).'_'.$relatedConcept->concept->name;
						my @refColumns = values(%{$relatedConcept->columnSet->columns});
						print $SQL "\nALTER TABLE $basename ADD FOREIGN KEY (",join(',',map { $_->name } @refColumns),")";
						print $SQL "\nREFERENCES $refBasename(".join(',',map { $_->refColumn->name } @refColumns).");\n";
					}
				}
			}
		}
		
		close($SQL);
	} else {
		Carp::croak("Unable to create output file $outfileSQL");
	}
}

use constant {
	REL_TEMPLATES_DIR	=>	'doc-templates',
	PACKAGES_TEMPLATE_FILE	=>	'packages.latex',
	COVER_TEMPLATE_FILE	=>	'cover.latex',
	FRONTMATTER_TEMPLATE_FILE	=>	'frontmatter.latex',
	ICONS_DIR	=>	'icons'
};

# assemblePDF parameters:
#	templateDir: The LaTeX template dir to be used to generate the PDF
#	model: DCC::Model instance
#	bpmodelFile: The model in bpmodel format
#	bodyFile: The temporal file where the generated documentation has been written.
#	outputFile: The output PDF file.
sub assemblePDF($$$$$) {
	my($templateDir,$model,$bpmodelFile,$bodyFile,$outputFile) = @_;
	
	my $masterTemplate = File::Spec->catfile($FindBin::Bin,basename($0,'.pl').'.latex');
	unless(-f $masterTemplate && -r $masterTemplate) {
		die "ERROR: Unable to find master template LaTeX file $masterTemplate\n";
	}
	
	foreach my $relfile (PACKAGES_TEMPLATE_FILE,COVER_TEMPLATE_FILE,FRONTMATTER_TEMPLATE_FILE) {
		my $templateFile = File::Spec->catfile($templateDir,$relfile);
		
		unless(-f $templateFile && -r $templateFile) {
			die "ERROR: Unable to find readable template LaTeX file $relfile in dir $templateDir\n";
		}
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
	my @params = map { my $str=encode('latex',$annotations->{$_}); $str =~ s/\{\}//g; ['ANNOT'.$_,$str] } keys(%{$annotations});
	push(@params,['projectName',$model->projectName],['schemaVer',$model->versionString],['modelSHA',$model->modelSHA1],['CVSHA',$model->CVSHA1],['schemaSHA',$model->schemaSHA1]);
	
	# Final slashes in directories are VERY important for subimports!!!!!!!! (i.e. LaTeX is dumb)
	push(@params,['INCLUDEtemplatedir',$templateDir.'/']);
	push(@params,['INCLUDEoverviewdir',$overviewDir.'/'],['INCLUDEoverviewname',$overviewName]);
	push(@params,['INCLUDEbodydir',$bodyDir.'/'],['INCLUDEbodyname',$bodyName]);
	
	my(undef,undef,$bpmodelFilename) = File::Spec->splitpath($bpmodelFile);
	push(@params,['BPMODELfilename',latex_escape($bpmodelFilename)]);
	push(@params,['BPMODELpath',$bpmodelFile]);
	
	# Setting the jobname and the jobdir, pruning the .pdf extension from the output
	my $absjob = File::Spec->rel2abs($outputFile);
	my $jobDir = dirname($absjob);
	my $jobName = basename($absjob,'.pdf');

	
	# And now, let's prepare the command line
	my @pdflatexParams = (
		'-interaction=batchmode',
#		'-synctex=1',
#		'-shell-escape',
		'-jobname',$jobName,
		'-output-directory',$jobDir,
		join(' ',map { '\gdef\\'.$_->[0].'{'.$_->[1].'}' } @params).' \input{'.$masterTemplate.'}'
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
	'fileType' => undef,
	'altname' => undef
);

my %CVCOMMANDS = (
	'file' => 'section',
	'header' => undef,
	'disposition' => undef,
	'version' => undef,
);

sub printDescription($$;$) {
	my($O,$text,$command)=@_;
	
	if(defined($command) && length($command)>0) {
		my $latex;
		if(exists($COMMANDS{$command})) {
			$latex = $COMMANDS{$command};
		} elsif(substr($command,0,5) eq 'LATEX') {
			$latex = substr($command,5);
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
	my $inline = $CV->kind ne DCC::Model::CV::URIFETCHED || (exists($CV->annotations->hash->{'disposition'}) && $CV->annotations->hash->{disposition} eq 'inline');
	
	$output .= "\n";
	# We have the values. Do we have to print them?
	if($CV->isLocal && $inline) {
		$output .= '\begin{tabular}{r@{ = }p{0.4\textwidth}}'."\n";
		#$output .= '\begin{tabular}{r@{ = }l}'."\n";

		my $CVhash = $CV->CV;
		foreach my $key (@{$CV->order}) {
			$output .= join(' & ','\textbf{'.latex_escape($key).'}',latex_escape($CVhash->{$key}->name))."\\\\\n";
		}
		$output .= '\end{tabular}';
	}
	
	if($CV->kind eq DCC::Model::CV::URIFETCHED) {
		# Is it an external CV
		$output .= '\textit{(See ';
		
		my @extCVs = @{$CV->uri};
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
		} elsif(substr($annotName,0,5) eq 'LATEX') {
			$latex = substr($annotName,5);
		}
		
		print $O "\\$latex\{",latex_format($annotations->{$annotName}),"\}\n"  if(defined($latex));
	}
	
	my $doPrintCV =  $CV->isLocal && ($CV->kind() ne DCC::Model::CV::URIFETCHED || (exists($CV->annotations->hash->{'showFetched'}) && $CV->annotations->hash->{showFetched} eq 'true'));
	
	if($doPrintCV || scalar(@{$CV->aliasOrder}) > 0) {
		# Table header
		print $O <<EOF ;
\\renewcommand{\\cvKey}{$header[0]}
\\renewcommand{\\cvDesc}{$header[1]}
EOF
	}
	
	# We have the values. Do we have to print them?
	if($doPrintCV) {
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
			print $O join(' & ',latex_escape($cvKey),latex_escape($CVhash->{$cvKey}->name)),'\\\\ \hline',"\n";
		}
		
		# Table footer
		print $O <<EOF ;
	\\end{xtabular}
\\end{center}
EOF
	}
	
	if($CV->kind() eq DCC::Model::CV::URIFETCHED){
		# Remote CVs
		# Is it an external CV
		
		my @extCVs = @{$CV->uri};
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
		
		my $aliashash = $CV->CV;
		foreach my $aliasKey (@{$CV->aliasOrder}) {
			my $alias = $aliashash->{$aliasKey};
			my $termStr = undef;
			
			if(scalar(@{$alias->parents}) > 1) {
				$termStr = '\begin{tabular}{l}'.join(' \\\\ ',map { latex_escape($_) } @{$alias->parents}).'\end{tabular}';
			} else {
				$termStr = $alias->parents->[0];
			}
			
			my $descStr = latex_escape($alias->name);
			print $O join(' & ', latex_escape($aliasKey), $termStr, $descStr),'\\\\ \hline',"\n";
		}

		# Table footer
		print $O <<EOF ;
		\\end{xtabular}
\\end{center}
EOF
	}
}

# parseColor parameters:
#	color: a XML::LibXML::Element instance, from the model, with an embedded color
# it returns an array of 2 or 4 elements, where the first one is the LaTeX color model
# (rgb, RGB or HTML), and the next elements are the components (1 compound for HTML, 3 for the others)
sub parseColor($) {
	my($color) = @_;
	
	my $colorText = $color->textContent();
	my $colorModel = undef;
	my @colorComponents = ();
	if($colorText =~ /^#([0-9a-fA-F]{3,6})$/) {
		my $component = $1;
		# Let's give it an upgrade
		if(length($component)==3) {
			$component = (substr($component,0,1) x 2) . (substr($component,1,1) x 2) . (substr($component,2,1) x 2);
		}
		$colorModel = 'HTML';
		push(@colorComponents,$component);
	} elsif($colorText =~ /^rgb\(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]),([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]),([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\)$/) {
		my $r = $1;
		my $g = $2;
		my $b = $3;
		
		$colorModel = 'RGB';
		push(@colorComponents,$r,$g,$b);
	} elsif($colorText =~ /^rgb\(([0-9]|[1-9][0-9]|100)%,([0-9]|[1-9][0-9]|100)%,([0-9]|[1-9][0-9]|100)%\)$/) {
		my $r = $1;
		my $g = $2;
		my $b = $3;
		
		$colorModel = 'rgb';
		push(@colorComponents,$r/100.0,$g/100.0,$b/100.0);
	}
	
	return ($colorModel,@colorComponents);
}

# genConceptGraphNode parameters:
#	concept: a DCC::Model::Concept instance
#	p_defaultColorDef: a reference to the default color
#	templateAbsDocDir: The absolute path to the template directory
# It returns the DOT line of the node and the color of this concept
sub genConceptGraphNode($\@$) {
	my($concept,$p_defaultColorDef,$templateAbsDocDir)=@_;
	
	my $conceptDomain = $concept->conceptDomain;
	
	my $entry = $conceptDomain->name.'_'.$concept->name;
	my $color = $entry;
	
	my @colorDef = ();
	my $p_colorDef = $p_defaultColorDef;
	if(exists($concept->annotations->hash->{color})) {
		my @colorDef = parseColor($concept->annotations->hash->{color});
		$p_colorDef = \@colorDef;
	}
	
	my $columnSet = $concept->columnSet;
	my %idColumnNames = map { $_ => undef } @{$concept->columnSet->idColumnNames};
	
	my $latexAttribs = '\graphicspath{{'.File::Spec->catfile($templateAbsDocDir,ICONS_DIR).'/}} \arrayrulecolor{Black} \begin{tabular}{ c l }  \multicolumn{2}{c}{\textbf{\hyperref[tab:'.$entry.']{\Large{}'.latex_escape(exists($concept->annotations->hash->{'altkey'})?$concept->annotations->hash->{'altkey'}:$concept->fullname).'}}} \\\\ \hline ';
	
	my @colOrder = fancyColumnOrdering($concept);
	my $labelPrefix = $conceptDomain->name.'.'.$concept->name.'.';
	
	my %partialFKS = ();
	$latexAttribs .= join(' \\\\ ',map {
		my $column = $_;
		if(defined($column->refColumn) && !defined($column->relatedConcept) && $column->refConcept->conceptDomain eq $conceptDomain) {
			my $refEntry = $column->refConcept->conceptDomain->name . '_' . $column->refConcept->name;
			$partialFKS{$refEntry} = (defined($concept->idConcept) && $column->refConcept eq $concept->idConcept)?1:undef  unless(exists($partialFKS{$refEntry}));
		}
		my $formattedColumnName = latex_escape($column->name);
		
		my $colType = $column->columnType->use;
		my $isId = exists($idColumnNames{$column->name});
		my $icon = undef;
		if($colType eq DCC::Model::ColumnType::DESIRABLE || $colType eq DCC::Model::ColumnType::OPTIONAL) {
			$formattedColumnName = '\textcolor{gray}{'.$formattedColumnName.'}';
		}
		if($colType eq DCC::Model::ColumnType::DESIRABLE || $isId) {
			$formattedColumnName = '\textbf{'.$formattedColumnName.'}';
		}
		# Hyperlinking only to concrete concepts
		my $refConcreteConcept = defined($column->refConcept) && (!RELEASE || !$column->refConcept->conceptDomain->isAbstract);
		if($refConcreteConcept) {
#			if(defined($column->refConcept) && defined($concept->idConcept) && $column->refConcept eq $concept->idConcept) {
			$formattedColumnName = '\textit{'.$formattedColumnName.'}';
		}
		
		if($isId) {
			$icon = ($refConcreteConcept)?'fkpk':'pk';
		} elsif($refConcreteConcept) {
			$icon = 'fk';
		}
		
		my $image = defined($icon)?('\includegraphics[height=1.6ex]{'.$icon.'.pdf}'):'';
		if($refConcreteConcept) {
			$image = '\hyperref[column:'.join('.',$column->refConcept->conceptDomain->name,$column->refConcept->name,$column->refColumn->name).']{'.$image.'}';
		}
		
		$image.' & \hyperref[column:'.($labelPrefix.$column->name).']{'.$formattedColumnName.'}'
	} @{$columnSet->columns}{@colOrder});
	
	$latexAttribs .= ' \end{tabular}';
	
	my $doubleBorder = defined($concept->idConcept)?',double distance=2pt':'';
	my $dotline = <<DEOF;
$entry [texlbl="$latexAttribs",style="top color=$color,rounded corners,drop shadow$doubleBorder",margin="0,0"];
DEOF

	return ($dotline, $entry, \%partialFKS, $color => $p_colorDef);
}

# genConceptDomainGraph parameters:
#	model: A DCC::Model instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, from the model
#	figurePrefix: path prefix for the generated figures in .dot and in .latex
#	templateAbsDocDir: Directory of the template being used
sub genConceptDomainGraph($$$$) {
	my($model,$conceptDomain,$figurePrefix,$templateAbsDocDir)=@_;
	
	my @defaultColorDef = ('rgb',0,1,0);
	if(exists($model->annotations->hash->{defaultColor})) {
		@defaultColorDef = parseColor($model->annotations->hash->{defaultColor});
	}
#	node [shape=box,style="rounded corners,drop shadow"];

	my $dotfile = $figurePrefix . '-domain-'. $conceptDomain->name.'.dot';
	my $latexfile = $figurePrefix . '-domain-'.$conceptDomain->name .'.latex';
	my $standalonelatexfile = $figurePrefix . '-domain-'.$conceptDomain->name .'-standalone.latex';
	my $DOT;
	if(open($DOT,'>:utf8',$dotfile)) {
		print $DOT <<DEOF;
digraph G {
	rankdir=LR;
	node [shape=box];
	edge [arrowhead=none];
	
DEOF
	}
	
	my %fks = ();
	my %colors = ();
	my @firstRank = ();
	# First, the concepts
	foreach my $concept (@{$conceptDomain->concepts}) {
		my($dotline,$entry,$p_partialFKS,$color,$p_colorDef) = genConceptGraphNode($concept,@defaultColorDef,$templateAbsDocDir);
		
		print $DOT $dotline;
		foreach my $refEntry (keys(%{$p_partialFKS})) {
			$fks{$refEntry}{$entry} = $p_partialFKS->{$refEntry};
		}
		$colors{$color} = $p_colorDef;
		
		if(!defined($concept->idConcept) || $concept->idConcept->conceptDomain ne $conceptDomain) {
			push(@firstRank,$entry);
		}
	}
	
	# Let's print the rank
	print $DOT <<DEOF;
	{ rank=same; @firstRank }
DEOF
	
	# Then, their relationships
	print $DOT <<DEOF;
	
	node [shape=diamond, texlbl="Identifies"];
	
DEOF
	# The FK restrictions from identification relations
	my $relnode = 1;
	my $doubleBorder = 'double distance=2pt';
	foreach my $idEntry (keys(%fks)) {
		my $dEntry = 'ID_'.$idEntry;
		print $DOT <<DEOF;
	
	${dEntry}_$relnode [style="top color=$idEntry,drop shadow,$doubleBorder"];
	$idEntry -> ${dEntry}_$relnode  [label="1"];
DEOF
		foreach my $entry (keys(%{$fks{$idEntry}})) {
			print $DOT <<DEOF;
	${dEntry}_$relnode -> $entry [label="N",style="$doubleBorder"];
DEOF
		}
		$relnode++;
	}
	
	print $DOT <<DEOF;
	
	node [shape=diamond];
	
DEOF
	# The FK restrictions from related concepts
	foreach my $concept (@{$conceptDomain->concepts}) {
		my $conceptDomainName = $conceptDomain->name;
		my $entry = $conceptDomainName.'_'.$concept->name;
		# Let's visit each concept!
		if(scalar(@{$concept->relatedConcepts})>0) {
			foreach my $relatedConcept (@{$concept->relatedConcepts}) {
				# Not showing the graphs to abstract concept domains
				next  if(RELEASE && $relatedConcept->concept->conceptDomain->isAbstract);
				
				my $refEntry = (defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName).'_'.$relatedConcept->concept->name;
				
				my $refEntryLine = '';
				
				my $dEntry = $entry.'_'.$refEntry;
				my $port = '';
				my $eport = '';
				my $wport = '';
				if(defined($relatedConcept->conceptDomainName) && $relatedConcept->conceptDomainName ne $conceptDomainName) {
					$port = ':n';
					$eport = ':e';
					$wport = ':w';
					$refEntryLine = $refEntry.' [shape="box",style="top color='.$refEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$refEntry.']{\Large{}'.latex_escape(exists($relatedConcept->concept->annotations->hash->{'altkey'})?$relatedConcept->concept->annotations->hash->{'altkey'}:$relatedConcept->concept->fullname).'}}"];';
				} elsif($relatedConcept->concept eq $concept) {
					$eport = ':n';
					$wport = ':s';
				}
				
				my $arity = $relatedConcept->arity;
				my $doubleBorder = $relatedConcept->isPartial()?'':'double distance=2pt';
				
				my $texlbl = 'Relationship';
				if(defined($relatedConcept->keyPrefix)) {
					$texlbl = '\parbox{3cm}{\centering '.$texlbl.' \linebreak \textit{\small('.latex_escape($relatedConcept->keyPrefix).')}}';
				}
				
				print $DOT <<DEOF;
	
	${dEntry}_$relnode [style="top color=$refEntry,drop shadow",texlbl="$texlbl"];
	$refEntryLine
	$refEntry$port -> ${dEntry}_$relnode$wport [label="$arity"];
	${dEntry}_$relnode$eport -> $entry$port [label="N",style="$doubleBorder"];
DEOF
				$relnode++;
			}
		}
	}
	
	# And now, let's close the graph
	print $DOT <<DEOF;
}
DEOF
	close($DOT);
	
	# Prepare the colors
	my $figpreamble = join('',map {
		my $color = $_;
		my($colorModel,@components) = @{$colors{$color}};
		'\definecolor{'.$color.'}{'.$colorModel.'}{'.join(',',@components).'}';
	} keys(%colors));
	
	# The moment to call dot2tex
	my @params = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble=\usepackage{hyperref}',
		'--figpreamble='.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
		'--figonly',
# This backend kills relationships
#		'-ftikz',
		'-o',$latexfile,
		$dotfile
	);
	my @standAloneParams = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble=\usepackage{hyperref} \usetikzlibrary{shapes,automata,backgrounds,arrows,shadows} \providecommand{\arrayrulecolor}[1] {}'.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
#		'--prog=circo',
# This backend kills relationships
#		'-ftikz',
		'-o',$standalonelatexfile,
		$dotfile
	);
	system(@params);
	system(@standAloneParams);

	return ($latexfile,$figpreamble);
}

# genConceptDomainGraph parameters:
#	model: A DCC::Model instance
#	figurePrefix: path prefix for the generated figures in .dot and in .latex
#	templateAbsDocDir: Directory of the template being used
sub genModelGraph($$$) {
	my($model,$figurePrefix,$templateAbsDocDir)=@_;
	
	my @defaultColorDef = ('rgb',0,1,0);
	my @blackColor = ('HTML','000000');
	if(exists($model->annotations->hash->{defaultColor})) {
		@defaultColorDef = parseColor($model->annotations->hash->{defaultColor});
	}
#	node [shape=box,style="rounded corners,drop shadow"];

	my $dotfile = $figurePrefix . '-model.dot';
	my $latexfile = $figurePrefix . '-model.latex';
	my $standalonelatexfile = $figurePrefix . '-model-standalone.latex';
	my $DOT;
	if(open($DOT,'>:utf8',$dotfile)) {
		print $DOT <<DEOF;
digraph G {
	rankdir=LR;
	node [shape=box];
	edge [arrowhead=none];
	
DEOF
	}
	
	my %colors = ();
	# With this, we do not need xcolor
	#$colors{Black} = \@blackColor;
	my $relnode = 1;
	# First, the concepts
	foreach my $conceptDomain (@{$model->conceptDomains}) {
		# Skipping abstract concept domains
		next  if(RELEASE && $conceptDomain->isAbstract);
		
		my $conceptDomainName = $conceptDomain->name;
		my $conceptDomainFullName = $conceptDomain->fullname;
		print $DOT <<DEOF;
	subgraph cluster_$conceptDomainName {
		label="$conceptDomainFullName"
DEOF
		my %fks = ();
		my @firstRank = ();
		foreach my $concept (@{$conceptDomain->concepts}) {
			my($dotline,$entry,$p_partialFKS,$color,$p_colorDef) = genConceptGraphNode($concept,@defaultColorDef,$templateAbsDocDir);
			
			print $DOT "\t",$dotline;
			foreach my $refEntry (keys(%{$p_partialFKS})) {
				$fks{$refEntry}{$entry} = $p_partialFKS->{$refEntry};
			}
			$colors{$color} = $p_colorDef;
		
			if(!defined($concept->idConcept) || $concept->idConcept->conceptDomain ne $conceptDomain) {
				push(@firstRank,$entry);
			}
		}
		
	
		# Let's print the rank
		print $DOT <<DEOF;
		{ rank=same; @firstRank }
DEOF
		# Then, their relationships
		print $DOT <<DEOF;
		
		node [shape=diamond, texlbl="Identifies"];
		
DEOF
		# The FK restrictions from identification relations
		my $doubleBorder = 'double distance=2pt';
		foreach my $idEntry (keys(%fks)) {
			my $dEntry = 'ID_'.$idEntry;
			print $DOT <<DEOF;
			
		${dEntry}_$relnode [style="top color=$idEntry,drop shadow,$doubleBorder"];
		$idEntry -> ${dEntry}_$relnode  [label="1"];
DEOF
			foreach my $entry (keys(%{$fks{$idEntry}})) {
				print $DOT <<DEOF;
		${dEntry}_$relnode -> $entry [label="N",style="$doubleBorder"];
DEOF
			}
			$relnode++;
		}
		
		print $DOT <<DEOF;
		
		node [shape=diamond];
		
DEOF
		# The FK restrictions from related concepts
		foreach my $concept (@{$conceptDomain->concepts}) {
			my $conceptDomainName = $conceptDomain->name;
			my $entry = $conceptDomainName.'_'.$concept->name;
			# Let's visit each concept!
			if(scalar(@{$concept->relatedConcepts})>0) {
				foreach my $relatedConcept (@{$concept->relatedConcepts}) {
					# Skipping relationships to abstract concept domains
					next  if(RELEASE && $relatedConcept->concept->conceptDomain->isAbstract);
					
					my $refEntry = (defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName).'_'.$relatedConcept->concept->name;
					
					my $refEntryLine = '';
					
					my $dEntry = $entry.'_'.$refEntry;
#					my $port = '';
#					my $eport = '';
#					my $wport = '';
#					if(defined($relatedConcept->conceptDomainName) && $relatedConcept->conceptDomainName ne $conceptDomainName) {
#						$port = ':n';
#						$eport = ':e';
#						$wport = ':w';
#						$refEntryLine = $refEntry.' [shape="box",style="top color='.$refEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$refEntry.']{\Large{}'.latex_escape(exists($relatedConcept->concept->annotations->hash->{'altkey'})?$relatedConcept->concept->annotations->hash->{'altkey'}:$relatedConcept->concept->fullname).'}}"];';
#					} elsif($relatedConcept->concept eq $concept) {
#						$eport = ':n';
#						$wport = ':s';
#					}
					
					my $arity = $relatedConcept->arity;
					my $doubleBorder = $relatedConcept->isPartial()?'':'double distance=2pt';
					
					my $texlbl = 'Relationship';
					if(defined($relatedConcept->keyPrefix)) {
						$texlbl = '\parbox{3cm}{\centering '.$texlbl.' \linebreak \textit{\small('.latex_escape($relatedConcept->keyPrefix).')}}';
					}
					
					print $DOT <<DEOF;
		
		${dEntry}_$relnode [style="top color=$refEntry,drop shadow",texlbl="$texlbl"];
		$refEntry -> ${dEntry}_$relnode [label="$arity"];
		${dEntry}_$relnode -> $entry [label="N",style="$doubleBorder"];
DEOF
					$relnode++;
				}
			}
		}
		# And now, let's close the subgraph
		print $DOT <<DEOF;
	}
DEOF
	}
	
	# And now, let's close the graph
	print $DOT <<DEOF;
}
DEOF
	close($DOT);
	
	# Prepare the colors
	my $figpreamble = join('',map {
		my $color = $_;
		my($colorModel,@components) = @{$colors{$color}};
		'\definecolor{'.$color.'}{'.$colorModel.'}{'.join(',',@components).'}';
	} keys(%colors));
	
	# The moment to call dot2tex
	my @params = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble=\usepackage{hyperref}',
		'--figpreamble='.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
		'--figonly',
# This backend kills relationships
#		'-ftikz',
		'-o',$latexfile,
		$dotfile
	);
	my @standAloneParams = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble=\usepackage{hyperref} \usetikzlibrary{shapes,automata,backgrounds,arrows,shadows} \providecommand{\arrayrulecolor}[1] {}'.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
#		'--prog=circo',
# This backend kills relationships
#		'-ftikz',
		'-o',$standalonelatexfile,
		$dotfile
	);
	system(@params);
	system(@standAloneParams);
	
	return ($latexfile,$figpreamble);
}

# printConceptDomain parameters:
#	model: A DCC::Model instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, from the model
#	figurePrefix: The prefix path for the generated figures (.dot, .latex, etc...)
#	templateAbsDocDir: Directory of the template being used
#	O: The filehandle where to print the documentation about the concept domain
sub printConceptDomain($$$$$) {
	my($model,$conceptDomain,$figurePrefix,$templateAbsDocDir,$O)=@_;
	
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
	
	# generate dot graph representing the concept domain
	my($conceptDomainLatexFile,$figDomainPreamble) = genConceptDomainGraph($model,$conceptDomain,$figurePrefix,$templateAbsDocDir);
	
	my(undef,$absLaTeXDir,$relLaTeXFile) = File::Spec->splitpath($conceptDomainLatexFile);
	my $conceptDomainFullname = $conceptDomain->fullname;
	
	#my $gdef = '\graphicspath{{'.File::Spec->catfile($templateAbsDocDir,ICONS_DIR).'/}}';
	print $O <<GEOF;
$figDomainPreamble
\\begin{figure*}[!h]
\\centering
\\resizebox{.95\\linewidth}{!}{
\\hypersetup{
	linkcolor=Black
}
%\\input{$conceptDomainLatexFile}
\\import*{$absLaTeXDir/}{$relLaTeXFile}
}
\\caption{$conceptDomainFullname Sub-Schema}
\\end{figure*}
GEOF
	
	# Let's iterate over the concepts of this concept domain
	foreach my $concept (@{$conceptDomain->concepts}) {
		my $columnSet = $concept->columnSet;
		my %idColumnNames = map { $_ => undef } @{$concept->columnSet->idColumnNames};
		
		# The embedded documentation of each concept
		my $caption = latex_format($concept->fullname);
		print $O '\\subsection{'.$caption."}\n";
		foreach my $documentation (@{$concept->description}) {
			printDescription($O,$documentation);
		}
		my $annotationSet = $concept->annotations;
		my $annotations = $annotationSet->hash;
		foreach my $annotKey (@{$annotationSet->order}) {
			printDescription($O,$annotations->{$annotKey},$annotKey);
		}
		
		# The table header
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
		my @colorder = fancyColumnOrdering($concept);
		
		# Now, let's print the documentation of each column
		foreach my $column (@{$columnSet->columns}{@colorder}) {
			# Preparing the documentation
			my $description='';
			foreach my $documentation (@{$column->description}) {
				$description = "\n\n" if(length($description)>0);
				$description .= latex_format($documentation);
			}
			
			my $related='';
			# Only references to concepts is non abstract concept domains
			if(defined($column->refColumn) && (!RELEASE || !$column->refConcept->conceptDomain->isAbstract)) {
				$related = '\\textcolor{gray}{Relates to \\textit{\\hyperref[column:'.($column->refConcept->conceptDomain->name.'.'.$column->refConcept->name.'.'.$column->refColumn->name).']{'.latex_escape($column->refConcept->fullname.' ('.$column->refColumn->name.')').'}}}';
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
					$values .= "\n".'\textit{(See \hyperref[cvsec:'.$cv.']{'.(($restriction->kind() ne DCC::Model::CV::URIFETCHED)?'CV':'external CV description').' \ref*{cvsec:'.$cv.'}})}';
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
			
			print $O join(' & ','\label{column:'.($conceptDomain->name.'.'.$concept->name.'.'.$column->name).'}'.latex_escape($column->name),$colTypeStr,'\\texttt{'.$COLKIND2ABBR{($columnType->use!=DCC::Model::ColumnType::IDREF || exists($idColumnNames{$column->name}))?$columnType->use:DCC::Model::ColumnType::REQUIRED}.'}',join("\n\n",$description,$related,$values)),'\\\\ \hline',"\n";
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
	my($modelFile,$templateDocDir,$out)=@ARGV;
	
	binmode(STDERR,':utf8');
	binmode(STDOUT,':utf8');
	my $model = undef;
	
	# Preparing the absolute template documentation directory
	# and checking whether it exists
	my $templateAbsDocDir = $templateDocDir;
	unless(File::Spec->file_name_is_absolute($templateDocDir)) {
		$templateAbsDocDir = File::Spec->catfile($FindBin::Bin,REL_TEMPLATES_DIR,$templateDocDir);
	}
	
	unless(-d $templateAbsDocDir) {
		print STDERR "ERROR: Template directory $templateDocDir (treated as $templateAbsDocDir) does not exist!\n";
		exit 2;
	}
	
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
	my $outfileBPMODEL = undef;
	my $outfileLaTeX = undef;
	my $figurePrefix = undef;
	if(-d $out) {
		my (undef,undef,undef,$day,$month,$year) = localtime();
		# Doing numerical adjustments
		$year += 1900;
		$month ++;
		my $thisdate = sprintf("%d%.2d%.2d",$year,$month,$day);
		
		my $outfileRoot = File::Spec->catfile($out,join('-',$model->projectName,'data_model',$model->versionString,$thisdate));
		$outfilePDF = $outfileRoot . '.pdf';
		$outfileSQL = $outfileRoot . '.sql';
		$outfileBPMODEL = $outfileRoot . '.bpmodel';
		$figurePrefix = File::Spec->file_name_is_absolute($outfileRoot)?$outfileRoot:File::Spec->rel2abs($outfileRoot);
	} else {
		$outfilePDF = $out;
		$outfileSQL = $out.'.sql';
		$outfileLaTeX = $out.'.latex';
		$outfileBPMODEL = $out . '.bpmodel';
		$figurePrefix = $out;
	}
	
	# Generating the bpmodel bundle (if it is reasonable)
	$model->saveBPModel($outfileBPMODEL)  if(defined($outfileBPMODEL));
	
	# Generating the SQL file for BioMart
	genSQL($model,$outfileSQL);
	
	# Generating the graph model
	my($modelgraphfile,$modelpreamble) = genModelGraph($model,$figurePrefix,$templateAbsDocDir);

	# Generating the document to be included in the template
	my $TO = undef;
	
	if(defined($outfileLaTeX)) {
		open($TO,'>:utf8',$outfileLaTeX) || die "ERROR: Unable to create output LaTeX file";
	} else {
		$TO = File::Temp->new();
		binmode( $TO, ":utf8" );
		$outfileLaTeX = $TO->filename;
	}
	
	# Model graph
	my $modelName = latex_escape($model->projectName.' '.$model->schemaVer);
	my(undef,$absModelDir,$relModelFile) = File::Spec->splitpath($modelgraphfile);

	print $TO <<TEOF;
\\newpage
\\newlength{\\modelheight}
\\newlength{\\modelwidth}
\\newsavebox{\\modelbox}

\\savebox{\\modelbox}{%
\\hypersetup{
	linkcolor=Black
}
\\import*{$absModelDir/}{$relModelFile}
}
\\setlength{\\modelheight}{\\ht\\modelbox}
% Total height
\\addtolength{\\modelheight}{\\dp\\modelbox}
\\setlength{\\modelwidth}{\\wd\\modelbox}

\\ifthenelse{\\modelheight<\\modelwidth}{%then
% This is what must be done to center landscape figures
\\begin{landscape}
\\parbox[c][\\textwidth][s]{\\linewidth}{%
\\vfill
\\centering
\\resizebox{\\linewidth}{!}{
\\usebox{\\modelbox}
}
\\captionof{figure}{Overview of $modelName data model}
\\vfill
}
\\end{landscape}
}{%else
{
\\vfill
\\centering
\\resizebox{\\linewidth}{!}{
\\usebox{\\modelbox}
}
\\captionof{figure}{Overview of $modelName data model}
\\vfill
}
}
TEOF
	
	print $TO '\chapter{DCC Submission Tabular Formats}\label{ch:tabFormat}',"\n";
	
	# Let's iterate over all the concept domains and their concepts
	foreach my $conceptDomain (@{$model->conceptDomains}) {
		# Skipping abstract concept domains on documentation generation
		next  if(RELEASE && $conceptDomain->isAbstract);
		
		printConceptDomain($model,$conceptDomain,$figurePrefix,$templateAbsDocDir,$TO);
	}

	print $TO "\\appendix\n";
	print $TO "\\chapter{Controlled Vocabularies}\n";
	
	foreach my $CV (@{$model->namedCVs}) {
		unless(exists($CV->annotations->hash->{disposition}) && $CV->annotations->hash->{disposition} eq 'inline') {
			printCVTable($CV,$TO);
		}
	}
	# Flushing the temp file
	$TO->flush();
	
	# Now, let's generate the documentation!
	assemblePDF($templateAbsDocDir,$model,$outfileBPMODEL,$outfileLaTeX,$outfilePDF);
} else {
	print STDERR "This program takes as input the model (in XML or BPModel formats), the documentation template directory and the output file\n";
	exit 1;
}
