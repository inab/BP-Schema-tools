#!/usr/bin/perl -w

use strict;

use BP::Loader::Mapper;

package BP::Loader::Mapper::Elasticsearch;

our $SECTION;

BEGIN {
	$SECTION = 'elasticsearch';
	$BP::Loader::Mapper::storage_names{$SECTION}=__PACKAGE__;
};

1;
