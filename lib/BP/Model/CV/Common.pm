#!/usr/bin/perl -W

use strict;

package BP::Model::CV::Common;

# printOboKeyVal parameters:
#	O: the output file handle
#	key: the key name
#	val: the value
sub printOboKeyVal($$$) {
	$_[0]->print($_[1],': ',$_[2],"\n");
}

1;
