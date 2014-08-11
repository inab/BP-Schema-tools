#!/usr/bin/perl -W

use strict;
use warnings;
use Carp;
use IO::File;	# For getline

package TabParser;

use constant {
	TAG_COMMENT	=>	'comment',	# Symbol use for comments in the tabular file
	TAG_SEP		=>	'sep',		# Regular expression separator used for the columns in the tabular file
	TAG_DELIMITER	=>	'delim',	# Symbol used for the columns in the tabular file
	TAG_SKIPLINES	=>	'skip-lines',	# Number of lines to skip at the beginning
	TAG_HAS_HEADER	=>	'read-header',	# Do we expect an embedded header line?
	TAG_HEADER	=>	'header',	# The array of elements in the header
	TAG_NUM_COLS	=>	'num-cols',	# Number of columns, fixed instead of calculated
	TAG_POS_FILTER	=>	'pos-filter',	# Positive filter by these values
	TAG_NEG_FILTER	=>	'neg-filter',	# Negative filter by these values
	TAG_FETCH_COLS	=>	'fetch-cols',	# The columns we are interested in
	TAG_CALLBACK	=>	'cb',		# callback to send tokens to
	TAG_ERR_CALLBACK	=>	'ecb',	# error callback to call
};

my %DEFCONFIG = (
#	TabParser::TAG_COMMENT	=>	'#',
	TabParser::TAG_SEP		=>	qr/\t/,
);

sub parseTab($;\%);

# Function to map the filters
sub mapFilters($\@){
	my($p_header,$p_filters) = @_;
	
	my $numcols = undef;
	if(ref($p_header) eq 'HASH') {
		$numcols = scalar(keys(%{$p_header}));
	} else {
		$numcols = $p_header;
		$p_header = undef;
	}
	
	my @retval = ();
	foreach my $filter (@{$p_filters}) {
		if($filter->[0] =~ /^(?:0|[1-9][0-9]*)$/) {
			Carp::croak("Condition out of range: ".$filter->[0].' '.$filter->[1])  if($filter->[0] >= $numcols);
			push(@retval,$filter);
		} elsif(defined($p_header)) {
			Carp::croak("Condition on unknown column: ".$filter->[0].' '.$filter->[1])  unless(exists($p_header->{$filter->[0]}));
			push(@retval,[$p_header->{$filter->[0]},$filter->[1]]);
		} else {
			Carp::croak("Filter with a named column on an unnamed context: ".$filter->[0].' '.$filter->[1]);
		}
	}
	
	return @retval;
}

# parseTab parameters:
#	T: the tabular file handle, which is being read
#	config: the configuration hash used to teach the parser
#		how to work
#	callback: the function to call with the read data on each
#	err_callback: the function to call when an error happens
sub parseTab($;\%) {
	my($T,$p_config)=@_;
	
	# Setting up the configuration
	my %config = %DEFCONFIG;
	@config{keys(%{$p_config})} = values(%{$p_config})  if(defined($p_config));
	
	# Number of columns of the tabular file
	# At this point we know nothing...
	my $numcols = undef;
	
	my @header = ();
	my %header = ();
	
	my @posfilter = ();
	my $doPosFilter = exists($config{TabParser::TAG_POS_FILTER});
	
	my @negfilter = ();
	my $doNegFilter = exists($config{TabParser::TAG_NEG_FILTER});
	
	my @columns = ();
	my $doColumns = exists($config{TabParser::TAG_FETCH_COLS});
	
	my @fetchColumnFilters = ();
	@fetchColumnFilters = map { [$_ => undef] } @{$config{TabParser::TAG_FETCH_COLS}}  if($doColumns);
	
	my $callback = (exists($config{TabParser::TAG_CALLBACK}))?$config{TabParser::TAG_CALLBACK}:undef;
	my $err_callback = (exists($config{TabParser::TAG_ERR_CALLBACK}))?$config{TabParser::TAG_ERR_CALLBACK}:undef;
	
	# If we have a predefined header
	# we can know the number of columns
	if(exists($config{TabParser::TAG_HEADER})) {
		@header = @{$config{TabParser::TAG_HEADER}};
		$numcols = scalar(@header);
		%header = map { $header[$_] => $_ } (0..($numcols-1));
		
		# And we try mapping the filters
		@posfilter = mapFilters(\%header,@{$config{TabParser::TAG_POS_FILTER}})  if($doPosFilter);
		@negfilter = mapFilters(\%header,@{$config{TabParser::TAG_NEG_FILTER}})  if($doNegFilter);
		@columns = map { $_->[0] } mapFilters(\%header,@fetchColumnFilters)  if($doColumns);
	}
	
	# Is number of columns forced?
	# But only if we don't have already a predefined header
	if(exists($config{TabParser::TAG_NUM_COLS}) && !defined($numcols)) {
		$numcols = $config{TabParser::TAG_NUM_COLS};
		@posfilter = mapFilters($numcols,@{$config{TabParser::TAG_POS_FILTER}})  if($doPosFilter);
		@negfilter = mapFilters($numcols,@{$config{TabParser::TAG_NEG_FILTER}})  if($doNegFilter);
		@columns = map { $_->[0] } mapFilters($numcols,@fetchColumnFilters)  if($doColumns);
	}
	
	# Do we have to read/skip a header?
	# But only if we don't know the number of columns
	my $doReadHeader = undef;
	if(exists($config{TabParser::TAG_HAS_HEADER})) {
		$doReadHeader = defined($numcols)?-1:1;
	}
	
	# This is the comment separator
	my $commentSep = undef;
	if(exists($config{TabParser::TAG_COMMENT})) {
		$commentSep = $config{TabParser::TAG_COMMENT};
	}
		
	my $eof = undef;
	# Skipping lines
	if(exists($config{TabParser::TAG_SKIPLINES}) && $config{TabParser::TAG_SKIPLINES} > 0) {
		foreach my $counter (1..($config{TabParser::TAG_SKIPLINES})) {
			my $cvline = $T->getline();
			unless(defined($cvline)) {
				$eof = 1;
				last;
			}
		}
	}
	
	# Let's read!
	unless(defined($eof)) {
		# Value delimiters
		my $delim = undef;
		my $delimLength = undef;
		if(exists($config{TabParser::TAG_DELIMITER})) {
			$delim = $config{TabParser::TAG_DELIMITER};
			$delimLength = length($delim);
		}
		
		# Separator is translated into a regexp
		my $sep = $config{TabParser::TAG_SEP};
		# With delimiters, it is wiser to add them to the separation pattern
		if(defined($delim)) {
			$sep = $delim . $sep . $delim;
		}
		unless(ref($sep) eq 'Regexp') {
			$sep = qr/$sep/;
		}
		
		# The columns we are interested in
		my @datacols = ();
		
		# Step 1: getting what we need
		my $cvline = undef;
		HEADERGET:
		while(!defined($numcols) && ($cvline=$T->getline())) {
			chomp($cvline);
			
			# Trimming comments
			if(defined($commentSep)) {
				my $commentIdx = index($cvline,$commentSep);
				$cvline = substr($cvline,0,$commentIdx)  if($commentIdx!=-1);
			}
			
			# And trimming external delimiters
			if(defined($delim)) {
				if(index($cvline,$delim)==0) {
					$cvline = substr($cvline,$delimLength);
				}
				my $rdel = rindex($cvline,$delim);
				if($rdel!=-1) {
					$cvline = substr($cvline,0,$rdel);
				}
			}
			
			next  if(length($cvline)==0);
			
			# Now, let's split the line
			my @tok = split($sep,$cvline);

			# Reading/skipping the header
			if(defined($doReadHeader)) {
				# We record it instead of discarding it
				if($doReadHeader == 1) {
					@header = @tok;
					$numcols = scalar(@tok);
					%header = map { $header[$_] => $_ } (0..($numcols-1));
					
					@posfilter = mapFilters(\%header,@{$config{TabParser::TAG_POS_FILTER}})  if($doPosFilter);
					@negfilter = mapFilters(\%header,@{$config{TabParser::TAG_NEG_FILTER}})  if($doNegFilter);
					@columns = map { $_->[0] } mapFilters(\%header,@fetchColumnFilters)  if($doColumns);
				}
				last;
			}
			
			# Recording/checking the number of columns
			$numcols = scalar(@tok);
			
			@posfilter = mapFilters($numcols,@{$config{TabParser::TAG_POS_FILTER}})  if($doPosFilter);
			@negfilter = mapFilters($numcols,@{$config{TabParser::TAG_NEG_FILTER}})  if($doNegFilter);
			@columns = map { $_->[0] } mapFilters($numcols,@fetchColumnFilters)  if($doColumns);
			
			# And now, let's filter!
			if($doPosFilter) {
				foreach my $filter (@posfilter) {
					last HEADERGET  if($tok[$filter->[0]] ne $filter->[1]);
				}
			}
			
			if($doNegFilter) {
				foreach my $filter (@negfilter) {
					last HEADERGET  if($tok[$filter->[0]] eq $filter->[1]);
				}
			}
			
			# And let's give it to the callback
			if(defined($callback)) {
				if($doColumns) {
					$callback->(@tok[@columns]);
				} else {
					$callback->(@tok);
				}
			}
			last;
		}

		# Step 2: run as the hell hounds!
		GETLINE:
		while(my $cvline=$T->getline()) {
			chomp($cvline);
			
			# Trimming comments
			if(defined($commentSep)) {
				my $commentIdx = index($cvline,$commentSep);
				$cvline = substr($cvline,0,$commentIdx)  if($commentIdx!=-1);
			}
			
			# And trimming external delimiters
			if(defined($delim)) {
				if(index($cvline,$delim)==0) {
					$cvline = substr($cvline,$delimLength);
				}
				my $rdel = rindex($cvline,$delim);
				if($rdel!=-1) {
					$cvline = substr($cvline,0,$rdel);
				}
			}
			
			next  if(length($cvline)==0);
			
			# Now, let's split the line
			my @tok = split($sep,$cvline);

			if(scalar(@tok)!=$numcols) {
				Carp::croak("ERROR: Expected $numcols columns, got ".scalar(@tok));
			}
			
			# And now, let's filter!
			if($doPosFilter) {
				foreach my $filter (@posfilter) {
					next GETLINE  if($tok[$filter->[0]] ne $filter->[1]);
				}
			}
			
			if($doNegFilter) {
				foreach my $filter (@negfilter) {
					next GETLINE  if($tok[$filter->[0]] eq $filter->[1]);
				}
			}
			
			# And let's give it to the callback
			if(defined($callback)) {
				if($doColumns) {
					$callback->(@tok[@columns]);
				} else {
					$callback->(@tok);
				}
			}
		}
	}
}

1;