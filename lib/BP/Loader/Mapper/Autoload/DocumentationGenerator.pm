#!/usr/bin/perl -w

use strict;

use BP::Loader::Mapper;

package BP::Loader::Mapper::DocumentationGenerator;

our $SECTION;
BEGIN {
	$SECTION = 'gendoc';
	$BP::Loader::Mapper::storage_names{$SECTION}=__PACKAGE__;
};

1;
