#!/usr/bin/perl -W

use strict;

use FindBin;
use lib "$FindBin::Bin/lib";
use BP::Model;


if(scalar(@ARGV)>=2) {
	my($modelFile,$newModelFile)=@ARGV;
	
	binmode(STDERR,':utf8');
	binmode(STDOUT,':utf8');
	my $model = undef;
	
	eval {
		$model = BP::Model->new($modelFile);
	};
	
	if($@) {
		print STDERR "ERROR: Model loading and validation failed. Reason: ".$@,"\n";
		exit 2;
	}
	
	# Generating the CV bundle
	my $retval = $model->saveBPModel($newModelFile);
	print STDERR "RETVAL: ".$retval."\n";
} else {
	print STDERR "This program takes as input the model (in XML or bpmodel format) and the output model file (in bpmodel format)\n";
	exit 1;
}
