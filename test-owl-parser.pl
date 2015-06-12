#!/usr/bin/perl -w
use strict;
#use open qw (:std :utf8);

use FindBin;
use lib "$FindBin::Bin/lib";
use BP::Model;

# load Experimental Factor Ontology (http://www.ebi.ac.uk/efo/efo.owl)
foreach my $owlfile (@ARGV) {
	my $OWL_CV = BP::Model::CV->new();
	
	if(open(my $OWL,'<',$owlfile)) {
		$OWL_CV->__parseOWL($OWL,undef);
		
		if(open(my $OUT,'>:encoding(UTF-8)',$owlfile.'.obo')) {
			my $comments = <<CEOF;
Generated using $0 from
	$owlfile
CEOF
			$OWL_CV->OBOserialize($OUT,$comments);
			close($OUT);
		}
		close($OWL);
	}
}
