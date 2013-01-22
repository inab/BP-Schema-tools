#!/usr/bin/perl -W

use strict;
use Carp;
use File::Spec;

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

my %ABSTYPE2SQL= (
	'string' => 'VARCHAR(4096)',
	'integer' => 'INTEGER',
	'decimal' => 'DOUBLE',
	'boolean' => 'BOOL',
	'timestamp' => 'DATETIME',
	'complex' => 'VARCHAR(4096)',
);

# genSQL parameters:
#	model: a DCC::Model instance, with the parsed model.
#	the path to the SQL output file
sub genSQL($$) {
	my($model,$outfileSQL) = @_;
	
	my $SQL;
	if(open($SQL,'>',$outfileSQL)) {
		# Let's iterate over all the concept domains and their concepts
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			my $conceptDomainName = $conceptDomain->name;
			foreach my $concept (@{$conceptDomain->concepts}) {
				#my $conceptName = $concept->name;
				my $basename = $conceptDomainName.'_'.$concept->name;
				print $SQL "\n-- ",$concept->fullname;
				print $SQL "\nCREATE TABLE $basename (";
				
				my $columnSet = $concept->columnSet;
				
				my $gottable=undef;
				# First, the idref columns
				my @colorder=@{$columnSet->idColumnNames};
				
				# And then, the others
				my %idcols = map { $_ => undef } @colorder;
				foreach my $columnName (@{$columnSet->columnNames}) {
					push(@colorder,$columnName)  unless(exists($idcols{$columnName}));
				}
				
				foreach my $column (@{$columnSet->columns}{@colorder}) {
					print $SQL ','  if(defined($gottable));
					
					my $columnType = $column->columnType;
					my $type = $ABSTYPE2SQL{$columnType->type};
					print $SQL "\n\t",$column->name,' ',$type;
					print $SQL ' NOT NULL'  if($columnType->use >= DCC::Model::ColumnType::IDREF);
					print $SQL ' DEFAULT ',$columnType->default  if(defined($columnType->default));
					$gottable = 1;
				}
				
				print $SQL "\n);\n\n";
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
#	
sub assemblePDF($$$$) {
	my($templateFile,$model,$bodyFile,$overview, = @_;
	
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
	
	my $annotations = $model->annotations;
	
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
	
	my($bodyDir,$bodyName);
	if(-f $bodyFile && -r $bodyFile) {
		my $absbody = File::Spec->rel2abs($bodyFile);
		$bodyDir = dirname($absbody);
		$bodyName = basename($absbody);
	} else {
		die "ERROR: Unable to find readable body LaTeX file $bodyFile\n";
	}
	
	my $jobname = $ARGV[4];
	
	# Storing the document generation parameters
	my @params = map { [$_,$annotations->{$_}] } keys(%{$annotations});
	push(@params,['project',$model->projectName],['schemaVer',$model->schemaVer]);
	
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
}

if(scalar(@ARGV)>=3) {
	my($modelFile,$templateDocFile,$outfile)=@ARGV[0..1];
	
	my $model = undef;
	
	eval {
		$model = DCC::Model->new($modelFile);
	};
	
	if($@) {
		print STDERR "ERROR: Model loading and validation failed. Reason: ".$@,"\n";
		exit 2;
	}
	
	# Generating the SQL file for BioMart
	genSQL($model,$outfile.'.sql');
	
	# Generating the document to be included in the template
	
	
	# Now, let's generate the documentation!

	close($O);
	} else {
		Carp::croak("Unable to create output file $outfile");
	}
} else {
	print STDERR "This program takes as input the model (in XML) and the output file\n";
	exit 1;
} 
