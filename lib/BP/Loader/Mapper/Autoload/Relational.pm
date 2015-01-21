#!/usr/bin/perl -w

use strict;

use BP::Loader::Mapper;

package BP::Loader::Mapper::Relational;

our $SECTION;
BEGIN {
	$SECTION = 'relational';
	$BP::Loader::Mapper::storage_names{$SECTION}=__PACKAGE__;
};

1;
