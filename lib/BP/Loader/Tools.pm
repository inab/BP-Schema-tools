#!/usr/bin/perl -W

use strict;
use File::Which;

package BP::Loader::Tools;

use File::Which qw();
use Carp;

# These closures are to hide the variables which held the detected executables
{
	my $gzip=undef;
	
	sub GZIP() {
		unless(defined($gzip)) {
			foreach my $exe ('pigz','gzip') {
				$gzip = File::Which::which($exe);
				last  if(defined($gzip));
			}
			Carp::croak("FATAL ERROR: Unable to find gzip or pigz in PATH")  unless(defined($gzip));
		}
		
		return $gzip;
	}
}

{
	my $gunzip=undef;
	
	sub GUNZIP() {
		unless(defined($gunzip)) {
			foreach my $exe ('unpigz','gunzip') {
				$gunzip = File::Which::which($exe);
				last  if(defined($gunzip));
			}
			Carp::croak("FATAL ERROR: Unable to find gunzip or unpigz in PATH")  unless(defined($gunzip));
		}
		
		return $gunzip;
	}
}

use constant SORT => 'sort';

1;
