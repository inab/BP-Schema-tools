#!/usr/bin/perl -W

use strict;
use File::Spec;
use File::Basename;

if(scalar(@ARGV)==5) {
	my $templateFile = $ARGV[0];
	my $paramFile = $ARGV[1];
	my $overviewFile = $ARGV[2];
	my $bodyFile = $ARGV[3];
	my $jobname = $ARGV[4];
	
	my @params = ();
	
	unless(-f $templateFile && -r $templateFile) {
		die "ERROR: Unable to find readable template LaTeX file\n";
	}
	
	my($bodyDir,$bodyName);
	if(-f $bodyFile && -r $bodyFile) {
		my $absbody = File::Spec->rel2abs($bodyFile);
		$bodyDir = dirname($absbody);
		$bodyName = basename($absbody);
	} else {
		die "ERROR: Unable to find readable body LaTeX file\n";
	}
	
	my($overviewDir,$overviewName);
	if(-f $overviewFile && -r $overviewFile) {
		my $absoverview = File::Spec->rel2abs($overviewFile);
		$overviewDir = dirname($absoverview);
		$overviewName = basename($absoverview);
	} else {
		die "ERROR: Unable to find readable overview LaTeX file\n";
	}
	
	my $P;
	if(open($P,'<',$paramFile)) {
		while(my $line=<$P>) {
			next  if(substr($line,0,1) eq '#');
			chomp($line);
			
			my($param,$value)=split(/\t/,$line,2);
			push(@params,[$param,$value]);
		}
		close($P);
	} else {
		die "ERROR: Unable to open input param file $paramFile\n";
	}
	
	# Final slashes in directories are VERY important for subimports!!!!!!!! (i.e. LaTeX is dumb)
	push(@params,['latexoverviewdir',$overviewDir.'/'],['latexoverviewname',$overviewName]);
	push(@params,['latexbodydir',$bodyDir.'/'],['latexbodyname',$bodyName]);
	
	# And now, let's prepare the command line
	my @pdflatex = (
		'pdflatex',
		'-jobname',$jobname,
		join(' ',map { '\def\\'.$_->[0].'{'.$_->[1].'}' } @params).' \input{'.$templateFile.'}'
	);
	
	print "COMMAND LINE => ",join(' ',@pdflatex),"\n";
	
	# exit 0;
	
	foreach my $it (1..5) {
		last  if(system(@pdflatex)!=0);
	}
} else {
	print STDERR <<EOF;
ERROR: This program takes four parameters:
	the template LaTeX file (which understand all the parameters and takes as input the body LaTeX file)
	the input param file (where several parameters, like the version, are set)
	the overview LaTeX file
	the body LaTeX file
	the jobname, which is indeed the prefix of PDFLaTeX job
EOF
}