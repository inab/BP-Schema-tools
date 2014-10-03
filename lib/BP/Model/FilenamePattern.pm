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

use BP::Model::Common;
use BP::Model::ColumnType::Common;

package BP::Model::FilenamePattern;

use constant FileTypeSymbolPrefixes => {
	'$' => 'BP::Model::Annotation',
	'@' => 'BP::Model::CV',
	'\\' => 'Regexp',
	'%' => 'BP::Model::SimpleType'
};

# This is the constructor.
# parseFilenameFormat parameters:
#	filenameFormatDecl: a XML::LibXML::Element 'dcc:filename-format' node
#		which has all the information to defined a named filename format
#		(or pattern)
#	model: A BP::Model instance, where this filename-pattern is declared
# returns a BP::Model::FilenamePattern instance, with all the needed information
# to recognise a filename following the defined pattern
sub parseFilenameFormat($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	my $filenameFormatDecl = shift;
	my $model = shift;
	
	# First, let's get the symbolic name
	my $name = $filenameFormatDecl->getAttribute('name');
	
	# This array will hold the different variable parts to validate
	# from the file pattern
	my @parts = ();
	
	# Name, regular expression, parts, registered concept domains
	my @filenamePattern = ($name,undef,\@parts,{});
	
	# And now, the format string
	my $formatString = $filenameFormatDecl->textContent();
	
	# The valid separators
	# To be revised (scaping)
	my $validSeps = join('',map { ($_ eq '\\')?('\\'.$_):$_ } keys(%{(FileTypeSymbolPrefixes)}));
	my $validSepsR = '['.$validSeps.']';
	my $validSepsN = '[^'.$validSeps.']+';
	
	my $pattern = '';
	my $tokenString = $formatString;
	
	# First one, the origin
	if($tokenString =~ /^($validSepsN)/) {
		$pattern = qr/\Q$1\E/;
		$tokenString = substr($tokenString,length($1));
	}
	
	# Now, the different pieces
	my $modelAnnotationsHash = $model->annotations->hash;
	while($tokenString =~ /($validSepsR)(\$?[a-zA-Z][a-zA-Z0-9]*)([^$validSeps]*)/g) {
		# Pattern for the content
		if(FileTypeSymbolPrefixes->{$1} eq 'Regexp') {
			my $pat = $model->getNamedPattern($2);
			if(defined($pat)) {
				# Check against the pattern!
				$pattern = qr/$pattern($pat)/;
				
				# No additional check
				push(@parts,undef);
			} else {
				Carp::croak("Unknown pattern '$2' used in filename-format '$formatString'"."\nOffending XML fragment:\n".$filenameFormatDecl->toString()."\n");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'BP::Model::SimpleType') {
			my $typeObject = $model->getItemType($2);
			if(defined($typeObject)) {
				my $type = $typeObject->[BP::Model::ColumnType::TYPEPATTERN];
				if(defined($type)) {
					my $spat = ($type->isa('Regexp'))?$type:'.+';
					$pattern = qr/$pattern($spat)/;
					
					# No additional check
					push(@parts,undef);
				} else {
					Carp::croak("Type '$2' used in filename-format '$formatString' was not simple"."\nOffending XML fragment:\n".$filenameFormatDecl->toString()."\n");
				}
			} else {
				Carp::croak("Unknown type '$2' used in filename-format '$formatString'"."\nOffending XML fragment:\n".$filenameFormatDecl->toString()."\n");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'BP::Model::CV') {
			my $CV = $model->getNamedCV($2);
			if(defined($CV)) {
				$pattern = qr/$pattern(.+)/;
				
				# Check the value against the CV
				push(@parts,$CV);
			} else {
				Carp::croak("Unknown controlled vocabulary '$2' used in filename-format '$formatString'"."\nOffending XML fragment:\n".$filenameFormatDecl->toString()."\n");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'BP::Model::Annotation') {
			my $annot = $2;
			
			# Is it a context-constant?
			if(substr($annot,0,1) eq '$') {
				$pattern = qr/$pattern(.+)/;
				
				# Store the value in this context variable
				push(@parts,$annot);
			} else {
				
				if(exists($modelAnnotationsHash->{$annot})) {
					# As annotations are at this point known constants, then check the exact value
					my $exact = $modelAnnotationsHash->{$annot};
					$pattern = qr/$pattern\Q$exact\E/;
					
					# No additional check
					push(@parts,undef);
				} else {
					Carp::croak("Unknown model annotation '$2' used in filename-format '$formatString'"."\nOffending XML fragment:\n".$filenameFormatDecl->toString()."\n");
				}
			}
		} else {
			# For unimplemented checks (shouldn't happen)
			$pattern = qr/$pattern(.+)/;
			
			# No checks, because we don't know what to check
			push(@parts,undef);
		}
		
		# The uninteresting value
		$pattern = qr/$pattern\Q$3\E/  if(defined($3) && length($3)>0);
	}
	
	# Now, the Regexp object!
	# Finishing the pattern building
	$filenamePattern[1] = qr/^$pattern$/;
	
	return bless(\@filenamePattern,$class);
}

# Name, regular expression, parts

# The symbolic name of this filename pattern
sub name {
	return $_[0]->[0];
}

# A Regexp object, representing the filename format
sub pattern {
	return $_[0]->[1];
}

# An array of post matching validations to be applied
# (mainly BP::Model::CV validation and context constant catching)
sub postValidationParts {
	return $_[0]->[2];
}

# It returns a hash of BP::Model::ConceptDomain instances
sub registeredConceptDomains {
	return $_[0]->[3];
}

# This method tries to match an input string against the filename-format pattern
#	filename: a string with a relative filename to match
# If it works, it returns a reference to a hash with the matched values correlated to
# the context constants
sub match($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $filename = shift;
	
	# Let's match the values!
	my @values = $filename =~ $self->pattern;
	
	my $p_parts = $self->postValidationParts;
	
	if(scalar(@values) ne scalar(@{$p_parts})) {
		#Carp::croak("Matching filename $filename against pattern ".$self->pattern." did not work!");
		return wantarray ? (undef,undef) : undef;
	}
	
	my %rethash = ();
	my $ipart = -1;
	foreach my $part (@{$p_parts}) {
		$ipart++;
		next  unless(defined($part));
		
		# Is it a CV?
		if(Scalar::Util::blessed($part) && $part->isa('BP::Model::CV')) {
			Carp::croak('Validation against CV did not match')  unless($part->isValid($values[$ipart]));
			
			# Let's save the matched CV value
			$rethash{$part->name} = $values[$ipart]  if(defined($part->name));
		# Context constant
		} elsif(ref($part) eq '') {
			$rethash{$part} = $values[$ipart];
		}
	}
	
	return wantarray ? (\%rethash,\@values) : \%rethash;
}

# This method matches the concept related to this
sub matchConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $filename = shift;
	
	my $retConcept = undef;
	
	my($mappedValues,$extractedValues) = $self->match($filename);
	
	if(defined($mappedValues) && exists($mappedValues->{'$domain'}) && exists($mappedValues->{'$concept'})) {
		my $domainName = $mappedValues->{'$domain'};
		my $conceptName = $mappedValues->{'$concept'};
		if(exists($self->registeredConceptDomains->{$domainName})) {
			my $conceptDomain = $self->registeredConceptDomains->{$domainName};
			if(exists($conceptDomain->conceptHash->{$conceptName})) {
				$retConcept = $conceptDomain->conceptHash->{$conceptName};
			}
		}
	}
	
	return wantarray ? ($retConcept,$mappedValues,$extractedValues) : $retConcept;
}

# This method is called when new concept domains are being read,
# and they use this filename-format, so they can be found later
#	conceptDomain:	a BP::Model::ConceptDomain instance, which uses this filename-pattern
# The method returns nothing
sub registerConceptDomain($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $conceptDomain = shift;
	
	# The concept domain is registered, then
	$self->registeredConceptDomains->{$conceptDomain->name} = $conceptDomain;
}

1;
