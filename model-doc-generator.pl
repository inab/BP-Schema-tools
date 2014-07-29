#!/usr/bin/perl -W

# TODO:
#	Column listing reordering (based on annotations)
#	Document allowed null values
#	Document column name used to fetch values

use strict;
use Carp;
use Config::IniFiles;
use Cwd;
use TeX::Encode;
use Encode;
use File::Basename;
use File::Copy;
use File::Spec;
use File::Temp;

use FindBin;
use lib "$FindBin::Bin/lib";
use BP::Model;
use BP::Loader::Mapper;
use BP::Loader::Mapper::DocumentationGenerator;
use BP::Loader::Mapper::Relational;

use constant PDFLATEX => 'xelatex';
#use constant TERMSLIMIT => 200;
use constant TERMSLIMIT => 10000;

# Global variable (using my because our could have too much scope)
my $RELEASE = 1;

# Flag validation
my $onlySQL = undef;
while(scalar(@ARGV)>0 && substr($ARGV[0],0,2) eq '--') {
	my $flag = shift(@ARGV);
	if($flag eq '--sql') {
		$onlySQL = 1;
	} elsif($flag eq '--showAbstract') {
		$RELEASE = undef;
	} else {
		print STDERR "Unknown flag $flag. This program takes as input: optional --sql or --showAbstract flags, the model (in XML or BPModel formats), the documentation template directory and the output file\n";
		exit 1;
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
		$templateAbsDocDir = File::Spec->catfile(dirname(File::Spec->rel2abs($modelFile)),$templateDocDir);
	}
	
	unless(-d $templateAbsDocDir) {
		print STDERR "ERROR: Template directory $templateDocDir (treated as $templateAbsDocDir) does not exist!\n";
		exit 2;
	}
	
	eval {
		$model = BP::Model->new($modelFile);
	};
	
	if($@) {
		print STDERR "ERROR: Model loading and validation failed. Reason: ".$@,"\n";
		exit 2;
	}
	
	# In case $out is a directory, then fill-in the other variables
	my $relOutfilePrefix = undef;
	my $workingDir = undef;
	my $SQLworkingDir = undef;
	if(-d $out) {
		my (undef,undef,undef,$day,$month,$year) = localtime();
		# Doing numerical adjustments
		$year += 1900;
		$month ++;
		my $thisdate = sprintf("%d%.2d%.2d",$year,$month,$day);
		
		$relOutfilePrefix=join('-',$model->projectName,'data_model',$model->versionString,$thisdate);
		$SQLworkingDir = $workingDir = $out;
		my $outfileRoot = File::Spec->catfile($out,$relOutfilePrefix);
	} else {
		$relOutfilePrefix = File::Basename::basename($out);
		$SQLworkingDir = File::Basename::dirname($out);
		$workingDir = $out;
	}
	
	# Generating the SQL file for BioMart
	my $conf = Config::IniFiles->new(-default=>$BP::Loader::Mapper::DEFAULTSECTION);
	# Setting up the fake configuration file
	$conf->newval($BP::Loader::Mapper::DEFAULTSECTION,BP::Loader::Mapper::FILE_PREFIX_KEY,$relOutfilePrefix);
	$conf->newval($BP::Loader::Mapper::SECTION,'release',$RELEASE);
	$conf->AddSection($BP::Loader::Mapper::Relational::SECTION);
	$conf->newval($BP::Loader::Mapper::Relational::SECTION,'db','');
	$conf->newval($BP::Loader::Mapper::DocumentationGenerator::SECTION,'template-dir',$templateDocDir);
	$conf->newval($BP::Loader::Mapper::DocumentationGenerator::SECTION,'pdflatex',PDFLATEX);
	$conf->newval($BP::Loader::Mapper::DocumentationGenerator::SECTION,'terms-limit',TERMSLIMIT);
	my $relStor = BP::Loader::Mapper::Relational->new($model,$conf);
	$relStor->generateNativeModel($SQLworkingDir);
	
	# We finish here if only SQL is required
	exit 0  if($onlySQL);
	
	my $docStor = BP::Loader::Mapper::DocumentationGenerator->new($model,$conf);
	$docStor->generateNativeModel($workingDir);
} else {
	print STDERR "This program takes as input: optional --sql or --showAbstract flags, the model (in XML or BPModel formats), the documentation template directory and the output file\n";
	exit 1;
}
