#!/usr/bin/perl -w

use strict;

use BP::Loader::Mapper;

package BP::Loader::Mapper::MongoDB;

our $SECTION;

BEGIN {
	$SECTION = 'mongodb';
	$BP::Loader::Mapper::storage_names{$SECTION}=__PACKAGE__;
};

1;
