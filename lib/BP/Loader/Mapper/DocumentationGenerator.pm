#!/usr/bin/perl -W

use strict;
use Carp;

use BP::Model;

use Config::IniFiles;

use Cwd;
use TeX::Encode;
use Encode;
use File::Basename;
use File::Copy;
use File::Spec;
use File::Temp;

use Image::ExifTool;

package BP::Loader::Mapper::DocumentationGenerator;

use base qw(BP::Loader::Mapper);

our $SECTION;
BEGIN {
	$SECTION = 'gendoc';
	$BP::Loader::Mapper::storage_names{$SECTION}=__PACKAGE__;
};

my @DEFAULTS = (
	['template-dir' => undef],
	['pdflatex' => 'xelatex'],
	['inline-terms-limit' => 20],
	['terms-limit' => 200],
	['release' => 'true'],
);

use constant {
	REL_TEMPLATES_DIR	=>	'doc-templates',
	PACKAGES_TEMPLATE_FILE	=>	'packages.latex',
	FONTS_TEMPLATE_FILE	=>	'fonts.latex',
	COVER_TEMPLATE_FILE	=>	'cover.latex',
	FRONTMATTER_TEMPLATE_FILE	=>	'frontmatter.latex',
	ICONS_DIR	=>	'icons',
	FIGURE_PREAMBLE_FILE	=>	'figure-preamble.latex',
	MASTER_TEMPLATE_FILE	=>	'model-doc-generator.latex'
};


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
	'data-version' => undef,
);

my %COLKIND2ABBR = (
	BP::Model::ColumnType::IDREF	=>	'I',
	BP::Model::ColumnType::REQUIRED	=>	'R',
	BP::Model::ColumnType::DESIRABLE	=>	'D',
	BP::Model::ColumnType::OPTIONAL	=>	'O'
);


# Constructor parameters:
#	model: a BP::Model instance
#	config: a Config::IniFiles instance
sub new($$) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	my $model = shift;
	my $config = shift;
	
	my $self  = $class->SUPER::new($model,$config);
	bless($self,$class);
	
	if(scalar(@DEFAULTS)>0) {
		if($config->SectionExists($SECTION)) {
			foreach my $param (@DEFAULTS) {
				my($key,$defval) = @{$param};
				
				if(defined($defval)) {
					$self->{$key} = $config->val($SECTION,$key,$defval);
				} elsif($config->exists($SECTION,$key)) {
					$self->{$key} = $config->val($SECTION,$key);
				} else {
					Carp::croak("ERROR: required parameter $key not found in section $SECTION");
				}
			}
		} else {
			Carp::croak("ERROR: Unable to read section $SECTION");
		}
	}
	
	$self->{relTemplateBaseDir} = defined($config->GetFileName)?File::Spec->rel2abs(File::Basename::dirname($config->GetFileName)):Cwd::cwd();
	
	return $self;
}

sub nestedCorrelatedConcepts {
	return undef;
}

# recordGeneratedFiles parameters:
#	filename: one or more filenames
# It records the paths, so at the end generateNativeModel returns them.
sub recordGeneratedFiles(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	$self->{generatedFiles} = []  unless(exists($self->{generatedFiles}));
	
	push(@{$self->{generatedFiles}},@_);
}

sub _Label__Prefix($) {
	my($concept)=@_;
	
	return $concept->conceptDomain->name.'.'.$concept->name.'.';
}

sub _Label__Column($$) {
	my($conceptOrLabelPrefix,$column)=@_;
	
	$conceptOrLabelPrefix = _Label__Prefix($conceptOrLabelPrefix)  if(ref($conceptOrLabelPrefix));
	
	return $conceptOrLabelPrefix.$column->name;
}

sub _EntryName($;$) {
	my($concept,$conceptDomainName)=@_;
	
	$conceptDomainName = $concept->conceptDomain->name  unless(defined($conceptDomainName));
	
	return $conceptDomainName.'_'.$concept->name;
}

# Original code obtained from:
# http://ommammatips.blogspot.com.es/2011/01/perl-function-for-latex-escape.html
sub _LaTeX__escape_internal($) {
	my $paragraph = shift;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([\$\#&%])/\\$1/g;
	
	# This one helps in hyphenation
	$paragraph =~ s/_/\\-\\_\\-/g;
	
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

sub _LaTeX__escape($) {
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
	
	return _LaTeX__escape_internal($paragraph);
}


sub _LaTeX__format($) {
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
	# (?:(?!PATTERN).)*
	$paragraph =~ s/\<i\>((?:(?!\<\/i\>).)*)\<\/i\>/\\textit{$1}/msg;
	$paragraph =~ s/\<b\>((?:(?!\<\/b\>).)*)\<\/b\>/\\textbf{$1}/msg;
	$paragraph =~ s/\<tt\>((?:(?!\<\/tt\>).)*)\<\/tt\>/\\texttt{$1}/msg;
	$paragraph =~ s/\<u\>((?:(?!\<\/u\>).)*)\<\/u\>/\\textunderscore{$1}/msg;
	$paragraph =~ s/\<a\s+href=["']([^>'"]+)['"]\>((?:(?!\<\/a\>).)*)\<\/a\>/\\href{$1}{$2}/msg;
	
	# XHTML content
	$paragraph =~ s/\<[^:]+:br *\/\>/\n\n/g;
	$paragraph =~ s/\<[^:]+:i\>((?:(?!\<\/[^:]+:i\>).)*)\<\/[^:]+:i\>/\\textit{$1}/msg;
	$paragraph =~ s/\<[^:]+:b\>((?:(?!\<\/[^:]+:b\>).)*)\<\/[^:]+:b\>/\\textbf{$1}/msg;
	$paragraph =~ s/\<[^:]+:tt\>((?:(?!\<\/[^:]+:tt\>).)*)\<\/[^:]+:tt\>/\\texttt{$1}/msg;
	$paragraph =~ s/\<[^:]+:u\>((?:(?!\<\/[^:]+:u\>).)*)\<\/[^:]+:u\>/\\textunderscore{$1}/msg;
	$paragraph =~ s/\<[^:]+:a\s+href=["']([^>'"]+)['"]\>((?:(?!\<\/[^:]+:a\>).)*)\<\/[^:]+:a\>/\\href{$1}{$2}/msg;
	
	# Encoded entities &lt; and &gt;
	$paragraph =~ s/&lt;/\</g;
	$paragraph =~ s/&gt;/\>/g;
	
	return _LaTeX__escape_internal($paragraph);
}

# This method reads LaTeX files into strings, removing comment lines
# So it could harm any side-effect related to newlines and zero-length comments
sub _LaTeX__readTemplate($$) {
	my($templateAbsDocDir,$templateFile) = @_;
	
	my $template = '';
	if(open(my $LT,'<',File::Spec->catfile($templateAbsDocDir,$templateFile))) {
		while(my $line=<$LT>) {
			chomp($line);
			# Removing comments
			my $ipos = index($line,'%');
			$line = substr($line,0,$ipos)  if($ipos!=-1);
			$template .= $line."\n"  if(length($line) > 0);
		}
		
		close($LT);
	} else {
		Carp::croak("ERROR: Unable to read template LaTeX file $templateFile\n");
	}
	
	return $template;
}

sub _LaTeX__printDescription($$;$) {
	my($O,$text,$command)=@_;
	
	if(defined($command) && length($command)>0) {
		my $latex;
		if(exists($COMMANDS{$command})) {
			$latex = $COMMANDS{$command};
		} elsif(substr($command,0,5) eq 'LATEX') {
			$latex = substr($command,5);
		}
		print $O "\\$latex\{",_LaTeX__format($text),"\}\n"  if(defined($latex));
	} else {
		print $O _LaTeX__format($text),"\n\n";
	}
}

# _LaTeX__inlineCVTable parameters:
#	CV: a BP::Model::CV instance
# it returns a string with a LaTeX-formatted table
sub _LaTeX__inlineCVTable($) {
	my($CV)=@_;
	
	my $output='';
	
	# First, the embedded documentation
	foreach my $documentation (@{$CV->description}) {
		$output .= _LaTeX__format($documentation)."\n";
	}
	# TODO: process the annotations
	my $inline = $CV->kind ne BP::Model::CV::URIFETCHED || (exists($CV->annotations->hash->{'disposition'}) && $CV->annotations->hash->{disposition} eq 'inline');
	
	$output .= "\n";
	# We have the values. Do we have to print them?
	if($CV->isLocal && $inline) {
		$output .= '\begin{tabularx}{0.5\columnwidth}{>{\textbf\bgroup\texttt\bgroup}r<{\egroup\egroup}@{ $\mapsto$ }>{\raggedright\arraybackslash}X}'."\n";
		#$output .= '\begin{tabular}{r@{ = }l}'."\n";

		my $CVhash = $CV->CV;
		foreach my $key (@{$CV->order}) {
			$output .= join(' & ',_LaTeX__escape($key),_LaTeX__escape($CVhash->{$key}->name))."\\\\\n";
		}
		$output .= '\end{tabularx}';
	}
	
	if($CV->kind eq BP::Model::CV::URIFETCHED) {
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

# _LaTeX__CVCaption parameters:
#	CV: A named BP::Model::CV instance (the controlled vocabulary)
#	It returns the LaTeX caption string
sub _LaTeX__CVCaption($) {
	my($CV)=@_;
	
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
	
	return $caption;
}


# _LaTeX__CVTable parameters:
#	O: The output filehandle where the documentation about the
#		controlled vocabulary is written.
#	CV: A named BP::Model::CV instance (the controlled vocabulary)
#	termslimit: Max number of terms to be printed
sub _LaTeX__CVTable($$$) {
	my($O,$CV,$termslimit)=@_;
	
	my $cvname = $CV->name;
	
	my $annotationSet = $CV->annotations;
	my $annotations = $annotationSet->hash;
	
	# The caption
	my $caption = _LaTeX__CVCaption($CV);
	print $O "\\section{",_LaTeX__escape($caption),"} \\label{cvsec:$cvname}\n";
	
	my $dataVersion = $CV->version;
	
	print $O "\\textit{This controlled vocabulary ".(defined($dataVersion)?'(version '._LaTeX__escape($dataVersion).') ':'')."has ".scalar(@{$CV->order})." terms".((scalar(@{$CV->aliasOrder})>0)?(" and ".scalar(@{$CV->aliasOrder})." aliases"):"")."}\\\\[2ex]\n"  if($CV->isLocal);
	
	my @header = ();
	
	# The header names used by the LaTeX table
	if(exists($annotations->{'header'})) {
		@header = split(/\t/,$annotations->{'header'},2);
	} else {
		@header = ('Key','Description');
	}
	$header[0] = _LaTeX__format($header[0]);
	$header[1] = _LaTeX__format($header[1]);
	
	# Printing embedded documentation
	foreach my $documentation (@{$CV->description}) {
		print $O _LaTeX__format($documentation),"\n";
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
		
		print $O "\\$latex\{",_LaTeX__format($annotations->{$annotName}),"\}\n"  if(defined($latex));
	}
	
	my $doPrintCV =  $CV->isLocal && (scalar(@{$CV->order}) <= $termslimit || (exists($CV->annotations->hash->{'showFetched'}) && $CV->annotations->hash->{showFetched} eq 'true'));
	
	if($doPrintCV || (scalar(@{$CV->aliasOrder}) > 0 && scalar(@{$CV->aliasOrder}) <= $termslimit)) {
		# Table header
		print $O <<EOF ;
\\renewcommand{\\cvKey}{$header[0]}
\\renewcommand{\\cvDesc}{$header[1]}
EOF
	}
	
	# We have the values. Do we have to print them?
	if($doPrintCV) {
		my $latexcaption = _LaTeX__format($caption);
		print $O <<'EOF';
{
	\arrayrulecolor{DarkOrange}
	\begin{longtable}[c]{|>{\tt}r|p{0.5\textwidth}|}
EOF
		print $O '\caption{'.$latexcaption.'} \label{cv:'.$cvname.'} \\\\'."\n";
		print $O <<'EOF';
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline\hline
\endfirsthead
\multicolumn{2}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline
\endhead
\hline
\multicolumn{2}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline
\endfoot
\endlastfoot
EOF
	
		my $CVhash = $CV->CV;
		foreach my $cvKey (@{$CV->order}) {
			print $O join(' & ',_LaTeX__escape($cvKey),_LaTeX__escape($CVhash->{$cvKey}->name)),'\\\\ \hline',"\n";
		}
		
		# Table footer
		print $O <<'EOF';
	\end{longtable}
}
EOF
	}
	
	if($CV->kind() eq BP::Model::CV::URIFETCHED){
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
	if(scalar(@{$CV->aliasOrder}) > 0 && scalar(@{$CV->aliasOrder}) <= $termslimit) {
		my $latexcaption = _LaTeX__format($caption);
		print $O <<'EOF';
{
	\arrayrulecolor{DarkOrange}
	\begin{longtable}[c]{|>{\tt}r|p{0.3\textwidth}|p{0.5\textwidth}|}
EOF
		print $O '\caption{'.$latexcaption.' aliases} \label{cv:'.$cvname.':alias} \\\\'."\n";
		print $O <<'EOF';
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Alias}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline\hline
\endfirsthead
\multicolumn{3}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Alias}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline
\endhead
\hline \multicolumn{3}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline
\endfoot
\endlastfoot
EOF
		
		my $aliashash = $CV->CV;
		foreach my $aliasKey (@{$CV->aliasOrder}) {
			my $alias = $aliashash->{$aliasKey};
			my $termStr = undef;
			
			if(scalar(@{$alias->parents}) > 1) {
#				$termStr = '\begin{tabular}{l}'.join(' \\\\ ',map { _LaTeX__escape($_) } @{$alias->parents}).'\end{tabular}';
				$termStr = join(', ',map { _LaTeX__escape($_) } @{$alias->parents});
			} else {
				$termStr = $alias->parents->[0];
			}
			
			my $descStr = _LaTeX__escape($alias->name);
			print $O join(' & ', _LaTeX__escape($aliasKey), $termStr, $descStr),'\\\\ \hline',"\n";
		}

		# Table footer
		print $O <<'EOF';
	\end{longtable}
}
EOF
	}
}

# _ParseModelColor parameters:
#	color: a XML::LibXML::Element instance, from the model, with an embedded color
# it returns an array of 2 or 4 elements, where the first one is the LaTeX color model
# (rgb, RGB or HTML), and the next elements are the components (1 compound for HTML, 3 for the others)
sub _ParseModelColor($) {
	my($color) = @_;
	
	my $colorModel = undef;
	my @colorComponents = ();
	
	if(ref($color) && $color->isa('XML::LibXML::Element')
		&& $color->namespaceURI eq BP::Model::dccNamespace
		&& $color->localname eq 'color'
	) {
		my $colorText = $color->textContent();
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
	}
	
	return ($colorModel,@colorComponents);
}

# _ParseOrderingHints parameters:
#	a XML::LibXML::Element, type 'dcc:ordering-hints'
# It returns the ordering hints (at this moment, undef or the block where it appears)
sub _ParseOrderingHints($) {
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

# _FancyColumnOrdering parameters:
#	concept: a BP::Concept instance
# It returns an array with the column names from the concept in a fancy
# order, based on several criteria, like annotations
sub _FancyColumnOrdering($) {
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
			my($block) = _ParseOrderingHints($column->annotations->hash->{ordering});
			
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
			my($block) = _ParseOrderingHints($column->annotations->hash->{ordering});
			
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

# _GenConceptGraphNode parameters:
#	concept: a BP::Model::Concept instance
#	p_defaultColorDef: a reference to the default color
#	templateAbsDocDir: The absolute path to the template directory
#	isRelease: Is a release version?
# It returns the DOT line of the node and the color of this concept
sub _GenConceptGraphNode($\@$$) {
	my($concept,$p_defaultColorDef,$templateAbsDocDir,$isRelease)=@_;
	
	my $conceptDomain = $concept->conceptDomain;
	
	my $entry = _EntryName($concept);
	my $color = $entry;
	
	my $p_colorDef = $p_defaultColorDef;
	if(exists($concept->annotations->hash->{color})) {
		my @colorDef = _ParseModelColor($concept->annotations->hash->{color});
		$p_colorDef = \@colorDef;
	}
	
	my $columnSet = $concept->columnSet;
	my %idColumnNames = map { $_ => undef } @{$concept->columnSet->idColumnNames};
	
	my $latexAttribs = '\graphicspath{{'.File::Spec->catfile($templateAbsDocDir,ICONS_DIR).'/}} \arrayrulecolor{Black} \begin{tabular}{ c l }  \multicolumn{2}{c}{\textbf{\hyperref[tab:'.$entry.']{\Large{}'._LaTeX__escape(exists($concept->annotations->hash->{'altkey'})?$concept->annotations->hash->{'altkey'}:$concept->fullname).'}}} \\\\ \hline ';
	
	my @colOrder = _FancyColumnOrdering($concept);
	my $labelPrefix = _Label__Prefix($concept);
	
	my %partialFKS = ();
	$latexAttribs .= join(' \\\\ ',map {
		my $column = $_;
		if(defined($column->refColumn) && !defined($column->relatedConcept) && $column->refConcept->conceptDomain eq $conceptDomain) {
			my $refEntry = _EntryName($column->refConcept);
			$partialFKS{$refEntry} = (defined($concept->idConcept) && $column->refConcept eq $concept->idConcept)?1:undef  unless(exists($partialFKS{$refEntry}));
		}
		my $formattedColumnName = _LaTeX__escape($column->name);
		
		my $colType = $column->columnType->use;
		my $isId = exists($idColumnNames{$column->name});
		my $icon = undef;
		if($colType eq BP::Model::ColumnType::DESIRABLE || $colType eq BP::Model::ColumnType::OPTIONAL) {
			$formattedColumnName = '\textcolor{gray}{'.$formattedColumnName.'}';
		}
		if($colType eq BP::Model::ColumnType::DESIRABLE || $isId) {
			$formattedColumnName = '\textbf{'.$formattedColumnName.'}';
		}
		# Hyperlinking only to concrete concepts
		my $refConcreteConcept = defined($column->refConcept) && (!$isRelease || !$column->refConcept->conceptDomain->isAbstract);
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
			$image = '\hyperref[column:'._Label__Column($column->refConcept,$column->refColumn).']{'.$image.'}';
		}
		
		$image.' & \hyperref[column:'._Label__Column($labelPrefix,$column).']{'.$formattedColumnName.'}'
	} @{$columnSet->columns}{@colOrder});
	
	$latexAttribs .= ' \end{tabular}';
	
	my $doubleBorder = defined($concept->idConcept)?',double distance=2pt':'';
	my $dotline = <<DEOF;
$entry [texlbl="$latexAttribs",style="top color=$color,rounded corners,drop shadow$doubleBorder",margin="0,0"];
DEOF

	return ($dotline, $entry, \%partialFKS, $color => $p_colorDef);
}

# _genModelGraph parameters:
#	figurePrefix: path prefix for the generated figures in .dot and in .latex
#	templateAbsDocDir: Directory of the template being used
#	p_colors: Hash of colors for each concept (to be filled)
# It generates a drawed graph from the model
sub _genModelGraph($$\%) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my($figurePrefix,$templateAbsDocDir,$p_colors)=@_;
	
	my $model = $self->{model};
	
	my @defaultColorDef = ('rgb',0,1,0);
	my @blackColor = ('HTML','000000');
	if(exists($model->annotations->hash->{defaultColor})) {
		@defaultColorDef = _ParseModelColor($model->annotations->hash->{defaultColor});
	}
#	node [shape=box,style="rounded corners,drop shadow"];

	my $dotfile = $figurePrefix . '-model.dot';
	my $latexfile = $figurePrefix . '-model.latex';
	my $standalonelatexfile = $figurePrefix . '-model-standalone.latex';
	if(open(my $DOT,'>:utf8',$dotfile)) {
		print $DOT <<DEOF;
digraph G {
	rankdir=LR;
	node [shape=box];
	edge [arrowhead=none];
	
DEOF
		
		# With this, we do not need xcolor
		#$colors{Black} = \@blackColor;
		my $relnode = 1;
		# First, the concepts
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			# Skipping abstract concept domains
			next  if($self->{release} && $conceptDomain->isAbstract);
			
			my @defaultConceptDomainColorDef = @defaultColorDef;
			if(exists($conceptDomain->annotations->hash->{defaultColor})) {
				@defaultConceptDomainColorDef = _ParseModelColor($conceptDomain->annotations->hash->{defaultColor});
			}

			my $conceptDomainName = $conceptDomain->name;
			my $conceptDomainFullName = $conceptDomain->fullname;
			print $DOT <<DEOF;
	subgraph cluster_$conceptDomainName {
		label="$conceptDomainFullName"
DEOF
			my %fks = ();
			my @firstRank = ();
			foreach my $concept (@{$conceptDomain->concepts}) {
				my($dotline,$entry,$p_partialFKS,$color,$p_colorDef) = _GenConceptGraphNode($concept,@defaultConceptDomainColorDef,$templateAbsDocDir,$self->{release});
				
				print $DOT "\t",$dotline;
				foreach my $refEntry (keys(%{$p_partialFKS})) {
					$fks{$refEntry}{$entry} = $p_partialFKS->{$refEntry};
				}
				$p_colors->{$color} = $p_colorDef;
			
				if(!defined($concept->idConcept) || $concept->idConcept->conceptDomain != $conceptDomain) {
					if(!defined($concept->parentConcept) || $concept->parentConcept->conceptDomain != $conceptDomain) {
						push(@firstRank,$entry);
					}
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
				my $entry = _EntryName($concept,$conceptDomainName);
				# Let's visit each concept!
				if(scalar(@{$concept->relatedConcepts})>0) {
					foreach my $relatedConcept (@{$concept->relatedConcepts}) {
						# Skipping relationships to abstract concept domains
						next  if($self->{release} && $relatedConcept->concept->conceptDomain->isAbstract);
						
						my $refEntry = _EntryName($relatedConcept->concept,(defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName));
						
						my $refEntryLine = '';
						
						my $dEntry = $entry.'_'.$refEntry;
#						my $port = '';
#						my $eport = '';
#						my $wport = '';
#						if(defined($relatedConcept->conceptDomainName) && $relatedConcept->conceptDomainName ne $conceptDomainName) {
#							$port = ':n';
#							$eport = ':e';
#							$wport = ':w';
#							$refEntryLine = $refEntry.' [shape="box",style="top color='.$refEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$refEntry.']{\Large{}'._LaTeX__escape(exists($relatedConcept->concept->annotations->hash->{'altkey'})?$relatedConcept->concept->annotations->hash->{'altkey'}:$relatedConcept->concept->fullname).'}}"];';
#						} elsif($relatedConcept->concept eq $concept) {
#							$eport = ':n';
#							$wport = ':s';
#						}
						
						my $arity = $relatedConcept->arity;
						my $doubleBorder = $relatedConcept->isPartial()?'':'double distance=2pt';
						
						my $texlbl = 'Relationship';
						if(defined($relatedConcept->keyPrefix)) {
							$texlbl = '\parbox{3cm}{\centering '.$texlbl.' \linebreak \textit{\small('._LaTeX__escape($relatedConcept->keyPrefix).')}}';
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
		
		# And the specialization relations!!!
		my %parentConceptNodes = ();
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			# Skipping abstract concept domains
			next  if($self->{release} && $conceptDomain->isAbstract);
			
			my $conceptDomainName = $conceptDomain->name;
			my $parentEntryLines = '';
			foreach my $concept (@{$conceptDomain->concepts}) {
				# There is one, so..... let's go!
				if($concept->parentConcept) {
					# Not showing the graphs to abstract concept domains
					my $parentConcept = $concept->parentConcept;
					next  if($self->{release} && $parentConcept->conceptDomain->isAbstract);
					
					my $entry = _EntryName($concept,$conceptDomainName);
					my $parentEntry = _EntryName($parentConcept);
					my $extendsEntry = $parentEntry.'__extends';
					if(!exists($parentConceptNodes{$parentEntry})) {
						# Let's create the (d) node, along with its main arc
						$parentEntryLines .= $extendsEntry.' [shape="triangle",margin="0",style="top color='.$parentEntry.',drop shadow",texlbl="\texttt{d}"];'."\n";
						$parentEntryLines .= $extendsEntry.' -> '.$parentEntry.' [style="double distance=2pt"];'."\n";
						
						$parentConceptNodes{$parentEntry} = undef;
					}
					
					# Let's create the arc to the (d) node
					$parentEntryLines .= $entry.' -> '.$extendsEntry."\n\n";
				}
			}
			print $DOT $parentEntryLines  if(length($parentEntryLines)>0);
		}

		# And now, let's close the graph
		print $DOT <<DEOF;
}
DEOF
		close($DOT);
	}
	
	# Prepare the colors
	my $figpreamble = join('',map {
		my $color = $_;
		my($colorModel,@components) = @{$p_colors->{$color}};
		'\definecolor{'.$color.'}{'.$colorModel.'}{'.join(',',@components).'}';
	} keys(%{$p_colors}));
	
	# The moment to call dot2tex
	my $docpreamble = _LaTeX__readTemplate(File::Spec->catdir($FindBin::Bin,REL_TEMPLATES_DIR),FIGURE_PREAMBLE_FILE);
	$docpreamble =~ tr/\n/ /;
	my $fontpreamble = _LaTeX__readTemplate($templateAbsDocDir,FONTS_TEMPLATE_FILE);
	$fontpreamble =~ tr/\n/ /;
	my @params = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble='.$docpreamble.' '.$fontpreamble,
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
		'--docpreamble='.$docpreamble.' '.$fontpreamble.' '.$figpreamble,
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
	
	#$self->recordGeneratedFiles($latexfile,$standalonelatexfile);
	
	return ($latexfile,$figpreamble);
}

# _genConceptDomainGraph parameters:
#	conceptDomain: A BP::Model::ConceptDomain instance, from the model
#	figurePrefix: path prefix for the generated figures in .dot and in .latex
#	templateAbsDocDir: Directory of the template being used
#	p_colors: Hash of colors for each concept (to be filled)
sub _genConceptDomainGraph($$$\%) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my($conceptDomain,$figurePrefix,$templateAbsDocDir,$p_colors)=@_;
	
	my $model = $self->{model};
	
	my @defaultColorDef = ('rgb',0,1,0);
	if(exists($conceptDomain->annotations->hash->{defaultColor})) {
		@defaultColorDef = _ParseModelColor($conceptDomain->annotations->hash->{defaultColor});
	} elsif(exists($model->annotations->hash->{defaultColor})) {
		@defaultColorDef = _ParseModelColor($model->annotations->hash->{defaultColor});
	}

#	node [shape=box,style="rounded corners,drop shadow"];

	my $conceptDomainName = $conceptDomain->name;
	my $dotfile = $figurePrefix . '-domain-'.$conceptDomainName.'.dot';
	my $latexfile = $figurePrefix . '-domain-'.$conceptDomainName.'.latex';
	my $standalonelatexfile = $figurePrefix . '-domain-'.$conceptDomainName.'-standalone.latex';
	if(open(my $DOT,'>:utf8',$dotfile)) {
		print $DOT <<DEOF;
digraph G {
	rankdir=LR;
	node [shape=box];
	edge [arrowhead=none];
	
DEOF
		
		my %fks = ();
		my @firstRank = ();
		# First, the concepts
		foreach my $concept (@{$conceptDomain->concepts}) {
			my($dotline,$entry,$p_partialFKS,$color,$p_colorDef) = _GenConceptGraphNode($concept,@defaultColorDef,$templateAbsDocDir,$self->{release});
			
			print $DOT $dotline;
			foreach my $refEntry (keys(%{$p_partialFKS})) {
				$fks{$refEntry}{$entry} = $p_partialFKS->{$refEntry};
			}
			$p_colors->{$color} = $p_colorDef;
			
			if(!defined($concept->idConcept) || $concept->idConcept->conceptDomain != $conceptDomain) {
				if(!defined($concept->parentConcept) || $concept->parentConcept->conceptDomain != $conceptDomain) {
					push(@firstRank,$entry);
				}
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
			# Let's visit each concept!
			if(scalar(@{$concept->relatedConcepts})>0) {
				my $entry = _EntryName($concept,$conceptDomainName);
				foreach my $relatedConcept (@{$concept->relatedConcepts}) {
					# Not showing the graphs to abstract concept domains
					next  if($self->{release} && $relatedConcept->concept->conceptDomain->isAbstract);
					
					my $refEntry = _EntryName($relatedConcept->concept,(defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName));
					
					my $refEntryLine = '';
					
					my $dEntry = $entry.'_'.$refEntry;
					my $port = '';
					my $eport = '';
					my $wport = '';
					if(defined($relatedConcept->conceptDomainName) && $relatedConcept->conceptDomainName ne $conceptDomainName) {
						$port = ':n';
						$eport = ':e';
						$wport = ':w';
						$refEntryLine = $refEntry.' [shape="box",style="top color='.$refEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$refEntry.']{\Large{}'._LaTeX__escape(exists($relatedConcept->concept->annotations->hash->{'altkey'})?$relatedConcept->concept->annotations->hash->{'altkey'}:$relatedConcept->concept->fullname).'}}"];';
					} elsif($relatedConcept->concept eq $concept) {
						$eport = ':n';
						$wport = ':s';
					}
					
					my $arity = $relatedConcept->arity;
					my $doubleBorder = $relatedConcept->isPartial()?'':'double distance=2pt';
					
					my $texlbl = 'Relationship';
					if(defined($relatedConcept->keyPrefix)) {
						$texlbl = '\parbox{3cm}{\centering '.$texlbl.' \linebreak \textit{\small('._LaTeX__escape($relatedConcept->keyPrefix).')}}';
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
		
		# And the specialization relations
		my %parentConceptNodes = ();
		my $parentEntryLines = '';
		foreach my $concept (@{$conceptDomain->concepts}) {
			# There is one, so..... let's go!
			if($concept->parentConcept) {
				# Not showing the graphs to abstract concept domains
				my $parentConcept = $concept->parentConcept;
				next  if($self->{release} && $parentConcept->conceptDomain->isAbstract);
				
				my $entry = _EntryName($concept,$conceptDomainName);
				my $parentEntry = _EntryName($parentConcept);
				my $extendsEntry = $parentEntry.'__extends';
				if(!exists($parentConceptNodes{$parentEntry})) {
					# Let's create the node, if external
					if($parentConcept->conceptDomain!=$conceptDomain) {
						$parentEntryLines .= $parentEntry.' [shape="box",style="top color='.$parentEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$parentEntry.']{\Large{}'._LaTeX__escape(exists($parentConcept->annotations->hash->{'altkey'})?$parentConcept->annotations->hash->{'altkey'}:$parentConcept->fullname).'}}"];'."\n";
					}
					
					# Let's create the (d) node, along with its main arc
					$parentEntryLines .= $extendsEntry.' [shape="triangle",margin="0",style="top color='.$parentEntry.',drop shadow",texlbl="\texttt{d}"];'."\n";
					$parentEntryLines .= $extendsEntry.' -> '.$parentEntry.' [style="double distance=2pt"];'."\n";
					
					$parentConceptNodes{$parentEntry} = undef;
				}
				
				# Let's create the arc to the (d) node
				$parentEntryLines .= $entry.' -> '.$extendsEntry."\n\n";
			}
		}
		print $DOT $parentEntryLines  if(length($parentEntryLines)>0);
		
		# And now, let's close the graph
		print $DOT <<DEOF;
}
DEOF
		close($DOT);
	}
	
	# Prepare the colors
	my $figpreamble = join('',map {
		my $color = $_;
		my($colorModel,@components) = @{$p_colors->{$color}};
		'\definecolor{'.$color.'}{'.$colorModel.'}{'.join(',',@components).'}';
	} keys(%{$p_colors}));
	
	# The moment to call dot2tex
	my $docpreamble = _LaTeX__readTemplate(File::Spec->catdir($FindBin::Bin,REL_TEMPLATES_DIR),FIGURE_PREAMBLE_FILE);
	$docpreamble =~ tr/\n/ /;
	my $fontpreamble = _LaTeX__readTemplate($templateAbsDocDir,FONTS_TEMPLATE_FILE);
	$fontpreamble =~ tr/\n/ /;
	my @params = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble='.$docpreamble.' '.$fontpreamble,
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
		'--docpreamble='.$docpreamble.' '.$fontpreamble.' '.$figpreamble,
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

# _printConceptDomain parameters:
#	conceptDomain: A BP::Model::ConceptDomain instance, from the model
#	figurePrefix: The prefix path for the generated figures (.dot, .latex, etc...)
#	templateAbsDocDir: Directory of the template being used
#	O: The filehandle where to print the documentation about the concept domain
#	p_colors: The colors to be taken into account when the graphs are generated
sub _printConceptDomain($$$$\%) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my($conceptDomain,$figurePrefix,$templateAbsDocDir,$O,$p_colors)=@_;
	
	my $model = $self->{model};
	
	my $latexDefaultValue = exists($model->nullCV->annotations->hash->{default})?'\textbf{\textit{\color{black}'._LaTeX__escape($model->nullCV->annotations->hash->{default}).'}}':'the default value';
	
	# The title
	# TODO: consider using encode('latex',...) for the content
	my $conceptDomainName = $conceptDomain->name;
	my $conceptDomainFullname = $conceptDomain->fullname;
	print $O '\\section{'._LaTeX__format($conceptDomainFullname).'}\\label{fea:'.$conceptDomainName."}\n";
	
	# Printing embedded documentation in the model
	foreach my $documentation (@{$conceptDomain->description}) {
		_LaTeX__printDescription($O,$documentation);
	}
	my $cDomainAnnotationSet = $conceptDomain->annotations;
	my $cDomainAnnotations = $cDomainAnnotationSet->hash;
	foreach my $annotKey (@{$cDomainAnnotationSet->order}) {
		_LaTeX__printDescription($O,$cDomainAnnotations->{$annotKey},$annotKey);
	}
	# The additional documentation
	# The concept domain holds its additional documentation in a subdirectory with
	# the same name as the concept domain
	my $domainDocDir = File::Spec->catfile($model->documentationDir,$conceptDomainName);
	my $docfile = File::Spec->catfile($domainDocDir,$TOCFilename);
	
	if(-f $docfile && -r $docfile){
		# Full paths, so import instead of subimport
		print $O "\\import*\{$domainDocDir/\}\{$TOCFilename\}\n";
	}
	
	# generate dot graph representing the concept domain
	my($conceptDomainLatexFile,$figDomainPreamble) = $self->_genConceptDomainGraph($conceptDomain,$figurePrefix,$templateAbsDocDir,%{$p_colors});
	
	my(undef,$absLaTeXDir,$relLaTeXFile) = File::Spec->splitpath($conceptDomainLatexFile);
	
	#my $gdef = '\graphicspath{{'.File::Spec->catfile($templateAbsDocDir,ICONS_DIR).'/}}';
	print $O <<GEOF;
\\par
$figDomainPreamble
{%
%\\begin{figure*}[!h]
\\centering
%\\resizebox{.95\\linewidth}{!}{
\\maxsizebox{.95\\textwidth}{.4\\textheight}{
\\hypersetup{
	linkcolor=Black
}
%\\input{$conceptDomainLatexFile}
\\import*{$absLaTeXDir/}{$relLaTeXFile}
}
\\captionof{figure}{$conceptDomainFullname Sub-Schema}
%\\end{figure*}
}
GEOF
	
	# Let's iterate over the concepts of this concept domain
	foreach my $concept (@{$conceptDomain->concepts}) {
		my $columnSet = $concept->columnSet;
		my %idColumnNames = map { $_ => undef } @{$concept->columnSet->idColumnNames};
		
		# The embedded documentation of each concept
		my $caption = _LaTeX__format($concept->fullname);
		print $O '\\subsection{'.$caption."}\n";
		foreach my $documentation (@{$concept->description}) {
			_LaTeX__printDescription($O,$documentation);
		}
		my $annotationSet = $concept->annotations;
		my $annotations = $annotationSet->hash;
		foreach my $annotKey (@{$annotationSet->order}) {
			_LaTeX__printDescription($O,$annotations->{$annotKey},$annotKey);
		}
		
		# The relation to the extended concept
		if($concept->parentConcept && (!$concept->parentConcept->conceptDomain->isAbstract || !$self->{release})) {
			print $O '\textit{This concept extends \hyperref[tab:'._EntryName($concept->parentConcept).']{'.$concept->parentConcept->fullname.'}}'."\n";
		}
		
		# The table header
		my $entry = _EntryName($concept,$conceptDomainName);
#	\begin{tabularx}{\linewidth}{|>{\setlength{\hsize}{.5\hsize}\raggedright\arraybackslash}X|c|c|p{0.5\textwidth}|}
		print $O <<'EOF';
{
	\arrayrulecolor{DarkOrange}
EOF
#		print $O '\topcaption{'.$caption.'\label{tab:'.$entry.'}}'."\n";
#		print $O <<'EOF';
#\tablefirsthead{%
#\hline
#\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
#\hline\hline}
#\tablehead{%
#\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
#\hline
#\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
#\hline}
#\tabletail{%
#\hline
#\multicolumn{4}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
#\hline}
#
#	\begin{supertabular}{|l|c|c|p{0.5\textwidth}|}
#EOF
		print $O <<'EOF';
	\begin{longtable}[c]{|>{\maxsizebox{3.5cm}{!}\bgroup}l<{\egroup}|>{\texttt\bgroup}c<{\egroup}|>{\texttt\bgroup}c<{\egroup}|>{\raggedright\arraybackslash}p{0.5\textwidth}|}
EOF
		print $O '\caption{'.$caption.'\label{tab:'.$entry.'}} \\\\'."\n";
		print $O <<'EOF';
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
\hline\hline
\endfirsthead
\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
\hline
\endhead
\hline
\multicolumn{4}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline
\endfoot
\hline
\endlastfoot
EOF
		
		# Determining the order of the columns
		my @colorder = _FancyColumnOrdering($concept);
		
		# Now, let's print the documentation of each column
		foreach my $column (@{$columnSet->columns}{@colorder}) {
			my @descriptionItems = ();
			# Preparing the documentation
			my $description='';
			foreach my $documentation (@{$column->description}) {
				$description = "\n\n" if(length($description)>0);
				$description .= _LaTeX__format($documentation);
			}
			push(@descriptionItems,$description);
			
			# Only references to concepts is non abstract concept domains
			if(defined($column->refColumn) && (!$self->{release} || !$column->refConcept->conceptDomain->isAbstract)) {
				my $related = '\\textcolor{gray}{Relates to \\textit{\\hyperref[column:'._Label__Column($column->refConcept,$column->refColumn).']{'._LaTeX__escape($column->refConcept->fullname.' ('.$column->refColumn->name.')').'}}}';
				push(@descriptionItems,$related);
			}
			
			# The comment about the values
			my $values='';
			if(exists($column->annotations->hash->{values})) {
				$values = '\\textit{'._LaTeX__format($column->annotations->hash->{values}).'}';
			}
			
			my $columnType = $column->columnType;
			
			# The default value
			if(defined($columnType->default)) {
				my $related = '\textcolor{gray}{If it is set to '.$latexDefaultValue.', the default value for this column is '.(ref($columnType->default)?'from ':'').'\textbf{\texttt{\color{black}'.(ref($columnType->default)?('\hyperref[column:'._Label__Column($concept,$columnType->default).']{'._LaTeX__escape($columnType->default->name).'}'):_LaTeX__escape($columnType->default)).'}}}';
				push(@descriptionItems,$related);
			}
			
			# Now the possible CV(s)
			my $restriction = $columnType->restriction;
			if(ref($restriction) && $restriction->isa('BP::Model::CV::Abstract')) {
				foreach my $CV (@{$restriction->getEnclosedCVs}) {
					# Is it an anonymous CV?
					my $numterms = scalar(@{$CV->order});
					if($numterms < $self->{'inline-terms-limit'} && (!defined($CV->name) || (exists($CV->annotations->hash->{'disposition'}) && $CV->annotations->hash->{disposition} eq 'inline'))) {
						$values .= _LaTeX__inlineCVTable($CV);
					} else {
						my $cv = $CV->name;
						$values .= "\n\n".'\textit{(See \hyperref[cvsec:'.$cv.']{'._LaTeX__CVCaption($CV).', CV \ref*{cvsec:'.$cv.'}})}';
					}
				}
			}
			
			### HACK ###
			if(ref($restriction) && $restriction->isa('BP::Model::CompoundType')) {
				my $rColumnSet = $restriction->columnSet;
				foreach my $rColumnName (@{$rColumnSet->columnNames}) {
					my $rColumn = $rColumnSet->columns->{$rColumnName};
					my $rRestr = $rColumn->columnType->restriction;
					
					if(defined($rRestr) && ref($rRestr) && $rRestr->isa('BP::Model::CV::Abstract')) {
						$values .= "\n".'\textit{\texttt{\textbf{'.$rColumnName.'}}}';
						foreach my $rCV (@{$rRestr->getEnclosedCVs}) {
							# Is it an anonymous CV?
							my $numterms = scalar(@{$rCV->order});
							if($numterms < $self->{'inline-terms-limit'} && (!defined($rCV->name) || (exists($rCV->annotations->hash->{'disposition'}) && $rCV->annotations->hash->{disposition} eq 'inline'))) {
								$values .= "\n"._LaTeX__inlineCVTable($rCV);
							} else {
								my $cv = $rCV->name;
								$values .= '$\mapsto$ \textit{(See \hyperref[cvsec:'.$cv.']{'._LaTeX__CVCaption($rCV).', CV \ref*{cvsec:'.$cv.'}})}';
							}
						}
					}
				}
			}
			
			push(@descriptionItems,$values)  if(length($values)>0);
			
			# What it goes to the column type column
			my $arrayDecorators = defined($columnType->arraySeps)?('[]' x length($columnType->arraySeps)):'';
			my @colTypeLines = ('\textbf{'._LaTeX__escape($columnType->type.$arrayDecorators).'}');
			
			push(@colTypeLines,'\textit{\maxsizebox{2cm}{!}{'._LaTeX__escape($restriction->template).'}}')  if(ref($restriction) eq 'BP::Model::CompoundType');
			
			push(@colTypeLines,'\textcolor{gray}{\maxsizebox{2cm}{!}{(array seps \textbf{\color{black}'._LaTeX__escape($columnType->arraySeps).'})}}')  if(defined($columnType->arraySeps));
			
			#push(@colTypeLines,'\textcolor{gray}{\maxsizebox{2cm}{!}{(default \textbf{\color{black}'.(ref($columnType->default)?('\hyperref[column:'._Label__Column($concept,$columnType->default).']{'._LaTeX__escape($columnType->default->name).'}'):_LaTeX__escape($columnType->default)).'})}}')  if(defined($columnType->default));
			
			# Stringify it!
			my $colTypeStr = (scalar(@colTypeLines)>1)?
#					'\begin{tabular}{l}'.join(' \\\\ ',map { _LaTeX__escape($_) } @colTypeLines).'\end{tabular}'
#					'\begin{minipage}[t]{8em}'.join(' \\\\ ',@colTypeLines).'\end{minipage}'
					'\pbox[t]{10cm}{\relax\ifvmode\centering\fi'."\n".join(' \\\\ ',@colTypeLines).'}'
					:
					$colTypeLines[0]
			;
			
			print $O join(' & ',
				'\label{column:'._Label__Column($concept,$column).'}'._LaTeX__escape($column->name),
				$colTypeStr,
				$COLKIND2ABBR{($columnType->use!=BP::Model::ColumnType::IDREF || exists($idColumnNames{$column->name}))?$columnType->use:BP::Model::ColumnType::REQUIRED},
				join("\n\n",@descriptionItems)
			),'\\\\ \hline',"\n";
		}
		# End of the table!
		print $O <<'EOF';
	\end{longtable}
}
EOF
	}
	
	# And at last, optional notes about this!
	my $notesfile = File::Spec->catfile($domainDocDir,$NotesFilename);
	
	if(-f $notesfile && -r $notesfile){
		print $O '\\subsection{Further notes on '._LaTeX__format($conceptDomain->fullname)."}\n";
		# Full paths, so import instead of subimport
		print $O "\\import*\{$domainDocDir/\}\{$NotesFilename\}\n";
	}
	
}

# _assemblePDF parameters:
#	templateDir: The LaTeX template dir to be used to generate the PDF
#	bpmodelFile: The model in bpmodel format
#	bodyFile: The temporal file where the generated documentation has been written.
#	outputFile: The output PDF file.
#	outputSH: Optional shell script to rebuild documentation
sub _assemblePDF($$$$$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my($templateDir,$bpmodelFile,$bodyFile,$outputFile,$outputSH) = @_;
	
	my $model = $self->{model};
	
	my $masterTemplate = File::Spec->catfile($FindBin::Bin,REL_TEMPLATES_DIR,MASTER_TEMPLATE_FILE);
	unless(-f $masterTemplate && -r $masterTemplate) {
		Carp::croak("ERROR: Unable to find master template LaTeX file $masterTemplate\n");
	}
	
	foreach my $relfile (PACKAGES_TEMPLATE_FILE,FONTS_TEMPLATE_FILE,COVER_TEMPLATE_FILE,FRONTMATTER_TEMPLATE_FILE) {
		my $templateFile = File::Spec->catfile($templateDir,$relfile);
		
		unless(-f $templateFile && -r $templateFile) {
			Carp::croak("ERROR: Unable to find readable template LaTeX file $relfile in dir $templateDir\n");
		}
	}

	my($bodyDir,$bodyName);
	if(-f $bodyFile && -r $bodyFile) {
		my $absbody = File::Spec->rel2abs($bodyFile);
		$bodyDir = File::Basename::dirname($absbody);
		$bodyName = File::Basename::basename($absbody);
	} else {
		Carp::croak("ERROR: Unable to find readable body LaTeX file $bodyFile\n");
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
		$overviewDir = File::Basename::dirname($absoverview);
		$overviewName = File::Basename::basename($absoverview);
	} else {
		Carp::croak("ERROR: Unable to find readable overview LaTeX file $overviewFile (declared in model!)\n");
	}
	
	# Storing the document generation parameters
	my @params = map {
		my $str = Encode::encode('latex',$annotations->{$_});
		$str =~ s/\{\}//g;
		['ANNOT'.$_,$str]
	} keys(%{$annotations});
	push(@params,['projectName',$model->projectName],['schemaVer',$model->versionString],['modelSHA',$model->modelSHA1],['CVSHA',$model->CVSHA1],['schemaSHA',$model->schemaSHA1]);
	
	# Final slashes in directories are VERY important for subimports!!!!!!!! (i.e. LaTeX is dumb)
	push(@params,['INCLUDEtemplatedir',$templateDir.'/']);
	push(@params,['INCLUDEoverviewdir',$overviewDir.'/'],['INCLUDEoverviewname',$overviewName]);
	push(@params,['INCLUDEbodydir',$bodyDir.'/'],['INCLUDEbodyname',$bodyName]);
	push(@params,
		['INCLUDEpackagesFile',PACKAGES_TEMPLATE_FILE],
		['INCLUDEfontsFile',FONTS_TEMPLATE_FILE],
		['INCLUDEcoverFile',COVER_TEMPLATE_FILE],
		['INCLUDEfrontmatterFile',FRONTMATTER_TEMPLATE_FILE]
	);
	
	my(undef,undef,$bpmodelFilename) = File::Spec->splitpath($bpmodelFile);
	push(@params,['BPMODELfilename',_LaTeX__escape($bpmodelFilename)]);
	push(@params,['BPMODELpath',$bpmodelFile]);
	
	# Setting the jobname and the jobdir, pruning the .pdf extension from the output
	my $absjob = File::Spec->rel2abs($outputFile);
	my $jobDir = File::Basename::dirname($absjob);
	my $jobName = File::Basename::basename($absjob,'.pdf');

	
	# And now, let's prepare the command line
	my $gdefs = join(' ',map { '\gdef\\'.$_->[0].'{'.$_->[1].'}' } @params);
	
	# Preparing gdefs for latexmk
	$gdefs =~ s/'/'"'"'/g;
	my @latexmkParams = (
		'-recorder',
		'-quiet',
		'-jobname='.$jobName,
		'-output-directory='.$jobDir,
		'-pdf',
		'-pdflatex='.$self->{pdflatex}.' %O '."'".$gdefs.' \input{%S}'."'",
		$masterTemplate
	);

	# print STDERR "[DOCGEN] => ",join(' ','latexmk',@latexmkParams),"\n";
	
	if(defined($outputSH)) {
		if(open(my $SH,'>',$outputSH)) {
			my $workingDir = cwd();
			if($workingDir =~ /['" ()\$\\]/) {
				$workingDir =~ s/'/'"'"'/g;
				$workingDir = "'".$workingDir."'";
			}
			print $SH <<EOFSH;
#!/bin/bash

read -r LATEXGDEF <<'EOF'
$gdefs \\input{%S}
EOF

cd $workingDir

latexmk -recorder -quiet -jobname=$jobName -output-directory=$jobDir -pdf -pdflatex="$self->{pdflatex} %O '\$LATEXGDEF'" $masterTemplate
EOFSH
			close($SH);
		} else {
			warn "ERROR: Unable to create shell script $outputSH\n";
		}
	}
	
	# exit 0;
	
	system('latexmk',@latexmkParams);
	if(-f $outputFile) {
		my $annotations = $model->annotations->hash;
		
		# Annotating the PDF (if there is an available XMP sidecar file)
		if(exists($annotations->{XMPsidecarDoc})) {
			my $xmp = $annotations->{XMPsidecarDoc};
			my $docsDir = $model->documentationDir();
			unless(File::Spec->file_name_is_absolute($xmp)) {
				$xmp = File::Spec->catfile($docsDir,$xmp);
			}
			my $exiftool = Image::ExifTool->new;
			$exiftool->SetNewValuesFromFile($xmp,'*:*');
			$exiftool->WriteInfo($outputFile);
		}
		
		# TODO: Signing the PDF with portablesigner
		# http://portablesigner.sourceforge.net/
		
		$self->recordGeneratedFiles($outputFile);
	}
}

# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
# It returns a reference to an array of absolute paths to the generated files, based on workingDir
sub generateNativeModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $workingDir = shift;
	my $filePrefix = $self->{BP::Loader::Mapper::FILE_PREFIX_KEY};
	my $fullFilePrefix = File::Spec->catfile($workingDir,$filePrefix);
	
	binmode(STDERR,':utf8');
	binmode(STDOUT,':utf8');
	
	# Resetting previously recorded generated files
	delete($self->{generatedFiles});
	
	my $model = $self->{model};
	my $templateDocDir = $self->{'template-dir'};
	
	# Preparing the absolute template documentation directory
	# and checking whether it exists
	my $templateAbsDocDir = $templateDocDir;
	unless(File::Spec->file_name_is_absolute($templateDocDir)) {
		$templateAbsDocDir = File::Spec->catfile($self->{relTemplateBaseDir},$templateDocDir);
	}
	
	unless(-d $templateAbsDocDir) {
		Carp::croak("ERROR: Template directory $templateDocDir (treated as $templateAbsDocDir) does not exist!\n");
	}
	
	# In case $out is a directory, then fill-in the other variables
	my $relOutfilePrefix = undef;
	my $outfilePDF = undef;
	my $outfileBPMODEL = undef;
	my $outfileLaTeX = undef;
	my $outfileSH = undef;
	my $figurePrefix = undef;
	my $out = $workingDir;
	if(-d $out) {
		$relOutfilePrefix = $self->{BP::Loader::Mapper::FILE_PREFIX_KEY};
		my $outfileRoot = File::Spec->catfile($out,$relOutfilePrefix);
		$outfilePDF = $outfileRoot . '.pdf';
		$outfileBPMODEL = $outfileRoot . '.bpmodel';
		$figurePrefix = File::Spec->file_name_is_absolute($outfileRoot)?$outfileRoot:File::Spec->rel2abs($outfileRoot);
	} else {
		# Working 'dir' was a file!?!
		$relOutfilePrefix = File::Basename::basename($out);
		$workingDir = File::Basename::dirname($out);
		$outfilePDF = $out;
		$outfileLaTeX = $out.'.latex';
		$outfileSH = $outfileLaTeX.'.sh';
		$outfileBPMODEL = $out . '.bpmodel';
		$figurePrefix = $out;
	}
	
	# Generating the bpmodel bundle (if it is reasonable)
	if(defined($outfileBPMODEL)) {
		$model->saveBPModel($outfileBPMODEL);
		$self->recordGeneratedFiles($outfileBPMODEL);
	}
	
	# Generating the graph model
	my %colors = ();
	my($modelgraphfile,$modelpreamble) = $self->_genModelGraph($figurePrefix,$templateAbsDocDir,%colors);

	# Generating the document to be included in the template
	my $TO = undef;
	
	if(defined($outfileLaTeX)) {
		open($TO,'>:utf8',$outfileLaTeX) || Carp::croak("ERROR: Unable to create output LaTeX file $outfileLaTeX\n");
		$self->recordGeneratedFiles($outfileLaTeX);
	} else {
		$TO = File::Temp->new();
		binmode($TO,':utf8');
		$outfileLaTeX = $TO->filename;
	}
	
	# Model graph
	my $modelName = _LaTeX__escape($model->projectName.' '.$model->schemaVer);
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
%\\parbox[c][\\textwidth][s]{\\linewidth}{%
\\vfill
\\centering
%\\resizebox{\\linewidth}{!}{
%\\usebox{\\modelbox}
%}
\\maxsizebox{.95\\textwidth}{.95\\textheight}{
\\usebox{\\modelbox}
}
\\captionof{figure}{Overview of $modelName data model}
\\vfill
%}
\\end{landscape}
}{%else
{
\\vfill
\\centering
%\\resizebox{\\linewidth}{!}{
%\\usebox{\\modelbox}
%}
\\maxsizebox{.95\\textwidth}{.95\\textheight}{
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
		next  if($self->{release} && $conceptDomain->isAbstract);
		
		$self->_printConceptDomain($conceptDomain,$figurePrefix,$templateAbsDocDir,$TO,%colors);
	}

	print $TO "\\appendix\n";
	print $TO "\\chapter{Controlled Vocabularies}\n";
	
	foreach my $CV (@{$model->namedCVs}) {
		unless(exists($CV->annotations->hash->{disposition}) && $CV->annotations->hash->{disposition} eq 'inline') {
			_LaTeX__CVTable($TO,$CV,$self->{'terms-limit'});
		}
	}
	# Flushing the temp file
	$TO->flush();
	
	# Now, let's generate the documentation!
	$self->_assemblePDF($templateAbsDocDir,$outfileBPMODEL,$outfileLaTeX,$outfilePDF,$outfileSH);
	
	return $self->{generatedFiles};
}

1;
