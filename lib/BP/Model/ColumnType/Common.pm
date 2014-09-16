#!/usr/bin/perl -W

use strict;

use Carp;
use File::Basename;
use File::Copy;
use File::Spec;
use IO::File;
use XML::LibXML;
use Encode;
use Digest::SHA1;
use URI;
use Archive::Zip;
use Archive::Zip::MemberRead;
use Scalar::Util;

package BP::Model::ColumnType;

# These two are to prepare the values to be inserted
# Better using something agnostic than JSON::true or JSON::false inside TO_JSON
use boolean 0.32;
use DateTime::Format::ISO8601;

# Static methods to prepare the data once read (data mangling)
sub __integer($) {
	$_[0]+0;
}

sub __decimal($) {
	$_[0]+0.0
}

sub __string($) {
	$_[0]
}

sub __boolean($) {
	defined($_[0])?(($_[0] =~ /^1|[tT](?:rue)?|[yY](?:es)?$/)?boolean::true:boolean::false):boolean::false
}

sub __timestamp($) {
	DateTime::Format::ISO8601->parse_datetime($_[0]);
}

sub __duration($) {
	$_[0]
}

# The pattern matching the contents for this type, and whether it is not a numeric type or yes
use constant {
	TYPEPATTERN	=>	0,
	ISNOTNUMERIC	=>	1,
	DATATYPEMANGLER	=>	2,
	DATATYPECHECKER	=>	3,
};

use constant {
	STRING_TYPE	=> 'string',
	TEXT_TYPE	=> 'text',
	INTEGER_TYPE	=> 'integer',
	DECIMAL_TYPE	=> 'decimal',
	BOOLEAN_TYPE	=> 'boolean',
	TIMESTAMP_TYPE	=> 'timestamp',
	DURATION_TYPE	=> 'duration',
	COMPOUND_TYPE	=> 'compound',
};

use constant ItemTypes => {
	BP::Model::ColumnType::STRING_TYPE	=> [1,1,\&__string,undef],	# With this we avoid costly checks
	BP::Model::ColumnType::TEXT_TYPE	=> [1,1,\&__string,undef],	# With this we avoid costly checks
	BP::Model::ColumnType::INTEGER_TYPE	=> [qr/^0|(?:-?[1-9][0-9]*)$/,undef,\&__integer,undef],
	BP::Model::ColumnType::DECIMAL_TYPE	=> [qr/^(?:0|(?:-?[1-9][0-9]*))(?:\.[0-9]+)?(?:e(?:0|(?:-?[1-9][0-9]*)))?$/,undef,\&__decimal,undef],
	BP::Model::ColumnType::BOOLEAN_TYPE	=> [qr/^[10]|[tT](?:rue)?|[fF](?:alse)?|[yY](?:es)?|[nN]o?$/,1,\&__boolean,undef],
	BP::Model::ColumnType::TIMESTAMP_TYPE	=> [qr/^[1-9][0-9][0-9][0-9](?:(?:1[0-2])|(?:0[1-9]))(?:(?:[0-2][0-9])|(?:3[0-1]))$/,1,\&__timestamp,undef],
	BP::Model::ColumnType::DURATION_TYPE	=> [qr/^P(?:(?:0|[1-9][0-9]*)W|(?:(?:0|[1-9][0-9]*)Y)(?:(?:0|[1-9][0-9]*)M)(?:(?:0|[1-9][0-9]*)D)(?:T(?:(?:0|[1-9][0-9]*)H)(?:(?:0|[1-9][0-9]*)M)(?:(?:0|[1-9][0-9]*)S))?)$/,1,\&__duration,undef],
	BP::Model::ColumnType::COMPOUND_TYPE	=> [undef,1,undef,undef]
};

# Always valid value
sub __true { 1 };

use constant {
	IDREF	=>	0,
	REQUIRED	=>	1,
	DESIRABLE	=>	-1,
	OPTIONAL	=>	-2
};

use constant {
	SCALAR_CONTAINER	=>	0,
	SET_CONTAINER	=>	1,
	ARRAY_CONTAINER	=>	2,
	HASH_CONTAINER	=>	3,
};

1;
