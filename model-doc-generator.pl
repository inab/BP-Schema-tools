#!/usr/bin/perl -W

use strict;
use Carp;
use File::Spec;

use FindBin;
use lib "$FindBin::Bin/lib";
use DCC::Model;

if(scalar(@ARGV)>=2) {
	my($modelFile,$outfile)=@ARGV[0..1];
	
	DCC::Model->new($modelFile);
} else {
	print STDERR "This program takes as input the model (in XML) and the output file\n";
} 

__END__

my %COMMANDS = (
	'file' => 'subsection',
	'featureType' => undef,
	'fileType' => undef
);

my %CVCOMMANDS = (
	'file' => 'section',
	'header' => undef
);

my $CVTablePathPrefix = '{codec.dir}/';
my $TOCFilename = 'toc.latex';
my $TSVExt = '.tsv';
my $TSVExtLength = length($TSVExt);

sub latex_escape($);
sub latex_format($);

sub genSQL($$);
sub scanTSVDir($\%\@);
sub processTSVDir($$\%$);
sub processCVTable($$$);

sub inlineCVTable($$);

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

sub genSQL($$) {
	my($dirname,$SQL)=@_;
	
	my $docfile = File::Spec->catfile($dirname,$TOCFilename);
	
	my $D;
	if(! -f $docfile) {
	#	Carp::carp("Skipping directory $dirname");
	} elsif(-r $docfile && opendir($D,$dirname)){
		my @entries = readdir($D);
		@entries = sort @entries;
		
		foreach my $entry (@entries) {
			if(substr($entry,-$TSVExtLength) eq $TSVExt) {
				my $F;
				my $file = File::Spec->catfile($dirname,$entry);
				if(-f $file && open($F,'<',$file)) {
					my $line;
					
					my $basename = substr($entry,0,-$TSVExtLength);
					
					print $SQL "\nCREATE TABLE $basename (";
					
					# Processing the tabular file
					my $gottable=undef;
					while($line=<$F>) {
						next  if(substr($line,0,1) eq '#');
						
						print $SQL ','  if(defined($gottable));
						
						chomp($line);
						my @fields = split(/\t/,$line);
						
						print $SQL "\n\t",$fields[0],' ',$fields[1];
						print $SQL ' NOT NULL'  if($fields[2] eq 'true');
						$gottable = 1;
					}
					close($F);
					
					print $SQL "\n);\n\n";
				}
			}
		}
		
		closedir($D);
	} else {
		Carp::carp("Unable to generate SQL for directory $dirname");
	}
}

sub scanTSVDir($\%\@) {
	my($dirname,$p_tabhash,$p_tables)=@_;
	
	my $docfile = File::Spec->catfile($dirname,$TOCFilename);
	
	my $D;
	if(! -f $docfile) {
	#	Carp::carp("Skipping directory $dirname");
	} elsif(-r $docfile && opendir($D,$dirname)){
		my @entries = readdir($D);
		@entries = sort @entries;
		
		foreach my $entry (@entries) {
			if(substr($entry,-$TSVExtLength) eq $TSVExt) {
				my $F;
				my $file = File::Spec->catfile($dirname,$entry);
				if(-f $file && open($F,'<',$file)) {
					my $line;
					
					# Processing the tabular file
					my $gottable=undef;
					while($line=<$F>) {
						next  if(substr($line,0,1) eq '#');
						
						chomp($line);
						my @fields = split(/\t/,$line);
						
						my $cv = $fields[5];
						if(defined($cv) && $cv ne '') {
							if(index($cv,$CVTablePathPrefix)==0) {
								$cv = substr($cv,length($CVTablePathPrefix));
							}
							
							unless(exists($p_tabhash->{$cv})) {
								$p_tabhash->{$cv} = [];
								push(@{$p_tables},$cv);
							}
							push(@{$p_tabhash->{$cv}},$file);
						}
					}
					close($F);
				}
			}
		}
		
		closedir($D);
	} else {
		Carp::carp("Unable to process directory $dirname");
	}
	
}

sub processTSVDir($$\%$) {
	my($dirname,$codecDir,$p_tabhash,$O)=@_;
	
	# We are only dealing with tsv files
	my $docfile = File::Spec->catfile($dirname,$TOCFilename);
	my $D;
	if(! -f $docfile) {
		#Carp::carp("Skipping directory $dirname");
	} elsif(-r $docfile && opendir($D,$dirname)){
		print $O "\\subimport*\{$dirname/\}\{$TOCFilename\}\n";
		
		my @entries = readdir($D);
		@entries = sort(@entries);
		
		foreach my $entry (@entries) {
			if(substr($entry,-$TSVExtLength) eq $TSVExt) {
				print STDERR "\tTSV $entry\n";
				my $F;
				my $file = File::Spec->catfile($dirname,$entry);
				if(-f $file && open($F,'<',$file)) {
					my $line;
					
					# Processing the tabular file
					my $gottable=undef;
					my $caption = 'TBD';
					while($line=<$F>) {
						chomp($line);
						if(substr($line,0,1) eq '#') {
							# Is it embedded documentation?
							if(!defined($gottable) && substr($line,1,1) eq '#') {
								my $docline = substr($line,2);
								my($command,$text) = split(/[ \t]/,$docline,2);
								$text =~ s/[ \t]+$//;
								my $latex = undef;
								if(defined($command) && length($command)>0) {
									if(exists($COMMANDS{$command})) {
										$latex = $COMMANDS{$command};
									} else {
										$latex = $command;
									}
									$caption=$text  if($command eq 'file');
									print $O "\\$latex\{",latex_format($text),"\}\n"  if(defined($latex));
								} else {
									print $O latex_format($text),"\n\n";
								}
							}
						} else {
							my @fields = split(/\t/,$line);
							
							unless(defined($gottable)) {
								$caption = latex_format($caption);
								print $O "\\topcaption{$caption} \\label{tab:$entry}\n";
								print $O <<'EOF' ;
\tablefirsthead{\hline \multicolumn{1}{|c|}{\textbf{Name}} & \multicolumn{1}{c|}{\textbf{Type}} & \multicolumn{1}{c|}{\textbf{R/O}} &
                       \multicolumn{1}{c|}{\textbf{Description / Values}} \\ \hline\hline }
\tablehead{\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
           \hline \multicolumn{1}{|c|}{\textbf{Name}} & \multicolumn{1}{c|}{\textbf{Type}} & \multicolumn{1}{c|}{\textbf{R/O}} &
                       \multicolumn{1}{c|}{\textbf{Description / Values}} \\ \hline }
\tablelasthead{\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- concluded from previous page}} \\
           \hline \multicolumn{1}{|c|}{\textbf{Name}} & \multicolumn{1}{c|}{\textbf{Type}} & \multicolumn{1}{c|}{\textbf{R/O}} &
                       \multicolumn{1}{c|}{\textbf{Description / Values}} \\ \hline }
\tabletail{\hline \multicolumn{4}{|r|}{{Continued on next page}} \\ \hline}
\tablelasttail{\hline \hline}

\begin{center}
%\begin{tabularx}{\linewidth}{ | l | c | c | X |}
\begin{xtabular}{|l|c|c|p{0.5\textwidth}|}
%\hline
%Name & Type & R/O & Description / Values \\
%\hline\hline
EOF
								$gottable = 1;
							}
							
							my $description='';
							my $values='';
							if(scalar(@fields)>=7) {
								$description = latex_format($fields[6]);
								if(scalar(@fields)>=8) {
									$values = '\\textit{'.latex_format($fields[7]).'}';
								}
							}
							my $cv = $fields[5];
							if(defined($cv) && $cv ne '') {
								if(index($cv,$CVTablePathPrefix)==0) {
									$cv = substr($cv,length($CVTablePathPrefix));
								}
								if(scalar(@{$p_tabhash->{$cv}})==1) {
									my $abscv = $cv;
									unless(File::Spec->file_name_is_absolute($abscv)) {
										$abscv = File::Spec->catfile($codecDir,$cv);
									}
									
									$values .= inlineCVTable($abscv,$entry);
								} else {
									$values .= "\n".'\textit{(See \hyperref[cv:'.$cv.']{CV Table \ref*{cv:'.$cv.'}})}';
								}
							}
							print $O join(' & ',latex_escape($fields[0]),'\\texttt{'.latex_escape($fields[1]).'}','\\texttt{'.($fields[2] eq 'true'?'R':'O').'}',$description."\n\n".$values),'\\\\ \hline',"\n";
						}
					}
					if(defined($gottable)) {
						print $O <<EOF ;
\\end{xtabular}
%\\end{tabularx}
%\\label{tab:$entry}
\\end{center}
EOF
					}
					close($F);
				}
			}
		}
		closedir($D);
	} else {
		Carp::carp("Unable to process directory $dirname");
	}
}

sub processCVTable($$$) {
	my($relcv,$cv,$O)=@_;
	
	my $retval = undef;
	
	my $CV;
	if(open($CV,'<',$cv)) {
		my $caption = undef;
		my $gottable = undef;
		my @header = ('Key','Description');
		while(my $line = <$CV>) {
			chomp($line);
			
			if(substr($line,0,1) eq '#') {
				# Is it embedded documentation?
				if(!defined($gottable) && substr($line,1,2) eq '#') {
					my $docline = substr($line,2);
					print $O $docline,"\n";
				}
				# Is it embedded documentation?
				if(!defined($gottable) && substr($line,1,1) eq '#') {
					
					my $docline = substr($line,2);
					my($command,$text) = split(/[ \t]/,$docline,2);
					$text =~ s/[ \t]+$//;
					my $latex = undef;
					if(defined($command) && length($command)>0) {
						if(exists($CVCOMMANDS{$command})) {
							$latex = $CVCOMMANDS{$command};
						} else {
							$latex = $command;
						}
						
						if($command eq 'file') {
							$caption=$text;
						} elsif($command eq 'header') {
							my @tmpheader = split(/\t/,$text);
							
							my $idx = 0;
							foreach my $tmpcell (@tmpheader) {
								last if($idx >= scalar(@header));
								
								$header[$idx] = latex_format($tmpcell);
								
								$idx++;
							}
						}
						unless(defined($caption)) {
							print $O "\\section{CV Table ",latex_escape($relcv),"}\n";
							$caption='';
						}
						print $O "\\$latex\{",latex_format($text),"\}\n"  if(defined($latex));
					} else {
						unless(defined($caption)) {
							print $O "\\section{CV Table ",latex_escape($relcv),"}\n";
							$caption='';
						}
						print $O latex_format($text),"\n\n";
					}
				}
			} else {
				unless(defined($gottable)) {
					unless(defined($caption)) {
						print $O "\\section{CV Table ",latex_escape($relcv),"}\n";
						$caption='';
					}
					
					$caption='Test'  if($caption eq '');
					
					print $O "\\topcaption{",latex_format($caption),"} \\label{cv:$relcv}\n";
					print $O <<EOF ;
\\renewcommand{\\cvKey}{$header[0]}
\\renewcommand{\\cvDesc}{$header[1]}
EOF

					print $O <<'EOF' ;
\tablefirsthead{\hline \multicolumn{1}{|c|}{\textbf{\cvKey}} &
                       \multicolumn{1}{c|}{\textbf{\cvDesc}} \\ \hline\hline }
\tablehead{\multicolumn{2}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
           \hline \multicolumn{1}{|c|}{\textbf{\cvKey}} & \multicolumn{1}{c|}{\textbf{\cvDesc}} \\ \hline }
\tablelasthead{\multicolumn{2}{c}{{\bfseries \tablename\ \thetable{} -- concluded from previous page}} \\
           \hline \multicolumn{1}{|c|}{\textbf{\cvKey}} & \multicolumn{1}{c|}{\textbf{\cvDesc}} \\ \hline }
\tabletail{\hline \multicolumn{2}{|r|}{{Continued on next page}} \\ \hline}
\tablelasttail{\hline \hline}

\begin{center}
	\begin{xtabular}{|r|p{0.5\textwidth}|}
%	\hline
%	Key & Description \\
%	\hline\hline
EOF
					$gottable = 1;
				}
				my @fields = split(/\t/,$line);
				print $O join(' & ',latex_escape($fields[0]),latex_escape($fields[1])),'\\\\ \hline',"\n";
			}
			
		}
		
		if(defined($gottable)) {
			print $O <<EOF ;
	\\end{xtabular}
\\end{center}
%\\end{table}
EOF
		}
		close($CV);
		
		$retval = 1;
	}
	
	return $retval;
}

sub inlineCVTable($$) {
	my($cv,$table)=@_;
	
	my $output='';
	
	my $CV;
	if(open($CV,'<',$cv)) {
		my $gottable = undef;
		while(my $line = <$CV>) {
			chomp($line);
			
			if(substr($line,0,1) eq '#') {
				# Is it embedded documentation?
				if(!defined($gottable) && substr($line,1,2) eq '#') {
					my $docline = substr($line,2);
					$output .= latex_format($docline)."\n";
				}
			} else {
				unless(defined($gottable)) {
					$output .= "\n";
					$output .= '\begin{tabular}{r@{ = }p{0.4\textwidth}}'."\n";
					#$output .= '\begin{tabular}{r@{ = }l}'."\n";
				}
				my @fields = split(/\t/,$line);
				$output .= join(' & ','\textbf{'.latex_escape($fields[0]).'}',latex_escape($fields[1]))."\\\\\n";

				$gottable = 1;
			}
		}
		
		if(defined($gottable)) {
			$output .= '\end{tabular}'."\n";
		}
		
		close($CV);
	} else {
		Carp::croak("Unable to open CV file $cv, used by $table");
	}
	
	return $output;
}

if(scalar(@ARGV)>=3) {
	my($dataModelDir,$codecDir,$outfile)=@ARGV[0..2];
	
	my $sourceModelDir = File::Spec->catfile($dataModelDir,'source');
	unless(-d $sourceModelDir) {
		Carp::croak("Data models directory ($dataModelDir) is wrong or not well set");
	}
	
	unless(-d $codecDir) {
		Carp::croak("Codecs directory ($codecDir) is not well set");
	}
	
	my $O;
	my $SQL;
	if(open($O,'>',$outfile) && open($SQL,'>',$outfile.'.sql')) {
		my $S;
		if(opendir($S,$sourceModelDir)) {
			my @tables = ();
			my %tabhash = ();
			
			my @entries = readdir($S);
			closedir($S);
			
			@entries = sort @entries;
			
			# First pass: scan
			foreach my $entry (@entries) {
				next  if(substr($entry,0,1) eq '.');
				
				my $file = File::Spec->catfile($sourceModelDir,$entry);
				if(-d $file) {
					scanTSVDir($file,%tabhash,@tables);
					genSQL($file,$SQL);
				}
				
			}
			close($SQL);
			
			# Second pass
			print $O '\chapter{DCC Submission Tabular Formats}\label{ch:tabFormat}',"\n";
			foreach my $entry (@entries) {
				next  if(substr($entry,0,1) eq '.');
				
				my $file = File::Spec->catfile($sourceModelDir,$entry);
				if(-d $file) {
					print STDERR "PDIR $file\n";
					processTSVDir($file,$codecDir,%tabhash,$O);
				}
				
			}
			
			print $O "\\appendix\n";
			print $O "\\chapter{Controlled Vocabulary Tables}\n";
			# Now it is time to process the gathered CV tables
			my @cvtables = sort @tables;
			foreach my $relcv (@cvtables) {
				# These tables were already processed
				next  if(scalar(@{$tabhash{$relcv}})==1);
				
				my $cv = $relcv;
				unless(File::Spec->file_name_is_absolute($relcv)) {
					$cv = File::Spec->catfile($codecDir,$relcv);
				}
				unless(processCVTable($relcv,$cv,$O)) {
					Carp::croak("Unable to open CV file $cv, used by ".join(', ',@{$tabhash{$relcv}}));
				}
			}
		} else {
			Carp::croak("Unable to open source models directory $sourceModelDir");
		}
		
		close($O);
	} else {
		Carp::croak("Unable to create output file $outfile");
	}
} else {
	print STDERR "This program takes as input the schemas dir, the CVs dir and the output file\n";
}