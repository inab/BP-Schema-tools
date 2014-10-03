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
use BP::Model::CV::Common;

use BP::Model::AnnotationSet;
use BP::Model::CV::Abstract;
use BP::Model::CV::External;
use BP::Model::CV::Term;
use BP::Model::DescriptionSet;

package BP::Model::CV;

use base qw(BP::Model::CV::Abstract);

use boolean 0.32;

use constant {
	INLINE	=>	'inline',
	NULLVALUES	=>	'null-values',
	CVLOCAL	=>	'cvlocal',
	URIFETCHED	=>	'uris',
};

use constant {
	CVFORMAT_CVFORMAT	=>	0,
	CVFORMAT_OBO	=>	1,
};

# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs, the CV (hash and keys), and the aliases (hash and keys)
use constant {
	CVNAME	=>	0,		# The CV symbolic name
	CVKIND	=>	1,		# the CV type
	CVURI	=>	2,		# the array of CV uri
	CVLOCALPATH	=>	3,	# the CV local filename
	CVLOCALFORMAT	=>	4,	# the CV local format
	CVANNOT	=>	5,		# the annotations
	CVDESC	=>	6,		# the documentation paragraphs
	CVHASH	=>	7,		# The CV hash
	CVKEYS	=>	8,		# The ordered CV term keys (for documentation purposes)
	CVALKEYS	=>	9,	# The ordered CV alias keys (for documentation purposes)
	CVXMLEL		=>	10,	# XML element of cv-file element
	CVID		=>	11,	# The CV id (used by SQL and MongoDB uniqueness purposes)
	CVLAX		=>	12,	# Disable checks on this CV
};

# This is the empty constructor
sub new() {
	my($self)=shift;
	my($class)=ref($self) || $self;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	$self = $class->SUPER::new()  unless(ref($self));
	
	# The CV symbolic name, the CV type, the array of CV uri, the CV local filename, the CV local format, the annotations, the documentation paragraphs, the CV (hash and array), aliases (array), XML element of cv-file element
	my $cvAnnot = BP::Model::AnnotationSet->new();
	my $cvDesc = BP::Model::DescriptionSet->new();
	@{$self}=(undef,undef,undef,undef,undef,$cvAnnot,$cvDesc,undef,undef,[],undef,undef,undef);
	
	$self->[BP::Model::CV::CVKEYS] = [];
	# Hash shared by terms and term-aliases
	$self->[BP::Model::CV::CVHASH] = {};
	
	return $self;
}

# This is the constructor and/or the parser
# parseCV parameters:
#	cv: a XML::LibXML::Element 'dcc:cv' node
#	model: a BP::Model instance
# returns a BP::Model::CV array reference, with all the controlled vocabulary
# stored inside.
# If the CV is in an external file, this method reads it, and calls the model to
# sanitize the paths and to digest the read lines.
# If the CV is in an external URI, this method only checks whether it is available (TBD)
sub parseCV($$) {
	my $self = shift;
	
	# Dual instance/class method
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new();
	}
	
	my $cv = shift;
	my $model = shift;
	
	$self->annotations->parseAnnotations($cv);
	$self->description->parseDescriptions($cv);
	
	$self->[BP::Model::CV::CVNAME] = $cv->getAttribute('name')  if($cv->hasAttribute('name'));
	
	$self->[BP::Model::CV::CVLAX] = boolean($cv->hasAttribute('lax') && $cv->getAttribute('lax') eq 'true');
	
	foreach my $el ($cv->childNodes()) {
		next  unless($el->nodeType == XML::LibXML::XML_ELEMENT_NODE);
		
		if($el->localname eq 'e') {
			unless(defined($self->[BP::Model::CV::CVKIND])) {
				$self->[BP::Model::CV::CVKIND] = ($cv->localname eq BP::Model::CV::NULLVALUES) ? BP::Model::CV::NULLVALUES : BP::Model::CV::INLINE;
			}
			$self->addTerm(BP::Model::CV::Term->new($el->getAttribute('v'),$el->textContent()));
		} elsif($el->localname eq 'cv-uri') {
			unless(defined($self->[BP::Model::CV::CVKIND])) {
				$self->[BP::Model::CV::CVKIND] = BP::Model::CV::URIFETCHED;
				$self->[BP::Model::CV::CVURI] = [];
			}
			
			# As we are not fetching the content, we are not initializing neither cvHash nor cvKeys references
			push(@{$self->[BP::Model::CV::CVURI]},BP::Model::CV::External->parseCVExternal($el,$self));
			
		} elsif($el->localname eq 'cv-file') {
			my $cvPath = $el->textContent();
			$cvPath = $model->sanitizeCVpath($cvPath);
			
			my $cvFormat = ($el->hasAttribute('format') && $el->getAttribute('format') eq 'obo')?BP::Model::CV::CVFORMAT_OBO : BP::Model::CV::CVFORMAT_CVFORMAT;
			
			# Local fetch
			$self->[BP::Model::CV::CVKIND] = BP::Model::CV::CVLOCAL  unless(defined($self->[BP::Model::CV::CVKIND]));
			$self->[BP::Model::CV::CVLOCALPATH] = $cvPath;
			$self->[BP::Model::CV::CVLOCALFORMAT] = $cvFormat;
			# Saving it for a possible storage in a bpmodel
			$self->[BP::Model::CV::CVXMLEL] = $el;
			
			my $CVH = $model->openCVpath($cvPath);
			if($cvFormat == BP::Model::CV::CVFORMAT_CVFORMAT) {
				$self->__parseCVFORMAT($CVH,$model);
			} elsif($cvFormat == BP::Model::CV::CVFORMAT_OBO) {
				$self->__parseOBO($CVH,$model);
			}
			
			$CVH->close();
			# We register the local CVs, even the local dumps of the remote CVs
			$model->registerCV($self);
		} elsif($el->localname eq 'term-alias') {
			my $alias = BP::Model::CV::Term->parseAlias($el);
			$self->addTerm($alias);
		}
	}
	
	# As we should have the full ontology (if it has been materialized), let's get the lineage of each term
	$self->validateAndEnactAncestors();
	
	return $self;
}

# As we should have the full ontology (if it has been materialized), let's get the lineage of each term
sub validateAndEnactAncestors(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $doRecover = shift;
	
	if(scalar(@{$self->order}) > 0) {
		my $p_CV = $self->CV;
		my @terms = (@{$self->order},@{$self->aliasOrder});
		foreach my $term (@terms) {
			my $term_ancestors = $p_CV->{$term}->calculateAncestors($p_CV,$doRecover);
		}
	}
}

# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs and the CV
# my @structCV=(undef,undef,{},[],{});

# The name of this CV (optional)
sub name {
	return $_[0]->[BP::Model::CV::CVNAME];
}

# The kind of CV
sub kind {
	return $_[0]->[BP::Model::CV::CVKIND];
}

sub isLax {
	return $_[0]->[BP::Model::CV::CVLAX];
}

# Ref to an array of BP::Model::CV::External, holding these values (it could be undef)
sub uri {
	return $_[0]->[BP::Model::CV::CVURI];
}

# Filename holding these values (optional)
sub localFilename {
	return $_[0]->[BP::Model::CV::CVLOCALPATH];
}

# Format of the filename holding these values (optional)
sub localFormat {
	return $_[0]->[BP::Model::CV::CVLOCALFORMAT];
}

# An instance of a BP::Model::AnnotationSet, holding the annotations
# for this CV
sub annotations {
	return $_[0]->[BP::Model::CV::CVANNOT];
}

# An instance of a BP::Model::DescriptionSet, holding the documentation
# for this CV
sub description {
	return $_[0]->[BP::Model::CV::CVDESC];
}

# The hash holding the CV in memory
sub CV {
	return $_[0]->[BP::Model::CV::CVHASH];
}

# The order of the CV values (as in the file)
sub order {
	return $_[0]->[BP::Model::CV::CVKEYS];
}

# The order of the alias values (as in the file)
sub aliasOrder {
	return $_[0]->[BP::Model::CV::CVALKEYS];
}

# The original XML::LibXML::Element instance where
# the path to a CV file was read from
sub xmlElement {
	return $_[0]->[BP::Model::CV::CVXMLEL];
}

# The id
sub id {
	unless(defined($_[0]->[BP::Model::CV::CVID])) {
		if(defined($_[0]->[BP::Model::CV::CVNAME])) {
			$_[0]->[BP::Model::CV::CVID] = $_[0]->[BP::Model::CV::CVNAME];
		} else {
			$_[0]->[BP::Model::CV::CVID] = $_[0]->_anonId;
		}
	}
	
	return $_[0]->[BP::Model::CV::CVID];
}

# With this method we check the locality of the CV
sub isLocal() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $cvkey = shift;
	
	return scalar(@{$self->order})>0;
}

# With this method a term or a term-alias is validated
# TODO: fetch on demand the CV if it is not materialized
sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $cvkey = shift;
	
	return exists($self->CV->{$cvkey});
}

# A instance of BP::Model::CV::Term
sub getTerm($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $cvkey = shift;
	
	return exists($self->CV->{$cvkey})?$self->CV->{$cvkey}:undef;
}

# It returns a self-contained array of instances of descendants of BP::Model::CV
sub getEnclosedCVs() {
	return [ $_[0] ];
}

# addTerm parameters:
#	term: a BP::Model::CV::Term instance, which can be a term or an alias
sub addTerm($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $term = shift;

	# Let's initialize
	foreach my $key (@{$term->isAlias?$term->keys:[$term->key]}) {
		# There are collissions!!!!
		if($self->isValid($key)) {
			# The original term
			my $origTerm = $self->CV->{$key};
			# Is an irresoluble collission?
			if($term->isAlias || $origTerm->isAlias || $origTerm->key eq $term->key || ($term->key ne $key && $origTerm->key ne $key)) {
				Carp::croak('Repeated key '.$key.' on'.((!defined($self->kind) || $self->kind eq INLINE)?' inline':'').' controlled vocabulary'.(defined($self->localFilename)?(' from '.$self->localFilename):'').(defined($self->name)?(' '.$self->name):''));
			}
			
			# As it is a resoluble one, let's fix it
			if($origTerm->key eq $key) {
				# Type 1: a previous term is in the alternate list of the new one
				# In this case, we remove the previous one
				foreach my $oldkey (@{$origTerm->keys}) {
					delete($self->CV->{$oldkey});
				}
				my @del_indexes = grep { $self->order->[$_] eq $key } 0..(scalar(@{$self->order})-1);
				map { splice(@{$self->order}, $_, 1) } @del_indexes;
			} elsif($term->key eq $key) {
				# Type 2: new term is an alternate one!
				# In this case, we consider the new term "old"
				# so we skip it
				return;
			}
		}
		$self->CV->{$key}=$term;
		$term->_setParentCV($self);
	}
	# We save here only the main key, not the alternate ones
	# and not the aliases!!!!!!
	unless($term->isAlias) {
		push(@{$self->order},$term->key);
	} else {
		push(@{$self->aliasOrder},$term->key);
	}
}

# __parseCVFORMAT parameters:
#	CVH: The file handle to read the controlled vocabulary file
#	model: a BP::Model instance, where the digestion of this file is going to be registered.
sub __parseCVFORMAT($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $CVH = shift;
	my $model = shift;
	
	while(my $cvline=$CVH->getline()) {
		chomp($cvline);
		
		$model->digestCVline($cvline);
		chomp($cvline);
		if(substr($cvline,0,1) eq '#') {
			if(substr($cvline,1,1) eq '#') {
				# Registering the additional documentation
				$cvline = substr($cvline,2);
				my($key,$value) = split(/ /,$cvline,2);
				
				# Adding embedded documentation and annotations
				if($key eq '') {
					$self->description->addDescription($value);
				} else {
					$self->annotations->addAnnotation($key,$value);
				}
			} else {
				next;
			}
		} else {
			my($key,$value) = split(/\t/,$cvline,2);
			$self->addTerm(BP::Model::CV::Term->new($key,$value));
		}
	}
}

sub __parseOBO($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $CVH = shift;
	my $model = shift;
	my $namespace = shift;
	
	my $keys = undef;
	my $name = undef;
	my $parents = undef;
	my $union = undef;
	my $terms = undef;
	while(my $cvline=$CVH->getline()) {
		chomp($cvline);
		
		$model->digestCVline($cvline);
		
		# Removing the trailing comments
		if((my $exclpos = index($cvline,'!'))!=-1) {
			$cvline = substr($cvline,0,$exclpos);
		}
		
		# And removing the trailing modifiers, because we don't need that info
		$cvline =~ s/\{.*\}\s*$//  if(index($cvline,'{')!=-1);
		
		$cvline =~ s/\s+$// if(substr($cvline,length($cvline)-1,1) eq ' ');
		
		# Skipping empty lines
		next  if(length($cvline)==0);
		
		# The moment to save a term
		
		if(substr($cvline,0,1) eq '[') {
			$terms = 1;
			if(defined($keys)) {
				$self->addTerm(BP::Model::CV::Term->new($keys,$name,defined($parents)?$parents:$union,(defined($union) && !defined($parents))?1:undef));
				
				# Cleaning!
				$keys = undef;
			}
			
			if($cvline eq '[Term]') {
				$keys = [];
				$name = undef;
				$parents = undef;
				$union = undef;
			}
		} elsif(defined($terms)) {
			if(defined($keys)) {
				my($elem,$val) = split(/:\s+/,$cvline,2);
				if($elem eq 'id') {
					unshift(@{$keys},$val);
				} elsif($elem eq 'alt_id') {
					push(@{$keys},$val);
				} elsif($elem eq 'name') {
					$name = $val;
				} elsif($elem eq 'namespace' && defined($namespace) && $namespace ne $val) {
					# Skipping the term, because it is not from the specific namespace we are interested in
					$keys = undef;
				} elsif($elem eq 'is_obsolete') {
					# Skipping the term, because it is obsolete
					$keys = undef;
				} elsif($elem eq 'is_a') {
					if(defined($parents)) {
						push(@{$parents},$val);
					} else {
						$parents = [$val];
					}
				} elsif($elem eq 'union_of') {
					# These are used to represent the aliases inside this implementation
					if(defined($union)) {
						push(@{$union},$val);
					} else {
						$union = [$val];
					}
				}
			}
		} else {
			# Global features
			my($elem,$val) = split(/:\s+/,$cvline,2);
			
			# Global remarks are treated as descriptions of the controlled vocabulary
			if($elem eq 'remark') {
				$self->description->addDescription(fromOBO($val));
			} elsif($elem eq 'data-version') {
				$self->annotations->addAnnotation($elem,fromOBO($val));
			#} else {
			#	$self->annotations->addAnnotation($elem,$val);
			}
		}
	}
	# Last term in a file
	$self->addTerm(BP::Model::CV::Term->new($keys,$name,defined($parents)?$parents:$union,(defined($union) && !defined($parents))?1:undef))  if(defined($keys));
}


my @OBOTRANS = (
	["\n"	=>	'\n'],
	[' '	=>	'\W'],
	["\t"	=>	'\t'],
	[':'	=>	'\:'],
	[','	=>	'\,'],
	['"'	=>	'\"'],
	['('	=>	'\('],
	[')'	=>	'\)'],
	['['	=>	'\['],
	[']'	=>	'\]'],
	['{'	=>	'\{'],
	['}'	=>	'\}'],
);

sub fromOBO($) {
	my $str = shift;
	
	my @tok = split(/\\\\/,$str);
	foreach my $tok (@tok) {
		foreach my $trans (@OBOTRANS) {
			$tok =~ s/\Q$trans->[1]\E/$trans->[0]/gs;
		}
	}
	
	return join('\\',@tok);
}

sub toOBO($) {
	my $str = shift;
	
	my @tok = split(/\\/,$str);
	foreach my $tok (@tok) {
		foreach my $trans (@OBOTRANS) {
			$tok =~ s/\Q$trans->[0]\E/$trans->[1]/gs;
		}
	}
	
	return join('\\\\',@tok);
}

# This method serializes the BP::Model::CV instance into a OBO structure
# serialize parameters:
#	O: the output file handle
#	comments: the comments to put
sub OBOserialize($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $O = shift;
	my $comments = shift;
	if(defined($comments)) {
		$comments = [$comments]   if(ref($comments) eq '');
		
		foreach my $comment (@{$comments}) {
			foreach my $commentLine (split(/\n/,$comment)) {
				print $O '! ',$commentLine,"\n";
			}
		}
	}
	
	# We need this
	BP::Model::CV::Common::printOboKeyVal($O,'format-version','1.2');
	my @timetoks = localtime();
	BP::Model::CV::Common::printOboKeyVal($O,'date',sprintf('%02d:%02d:%04d %02d:%02d',$timetoks[3],$timetoks[4]+1,$timetoks[5]+1900,$timetoks[2],$timetoks[1]));
	BP::Model::CV::Common::printOboKeyVal($O,'auto-generated-by','BP::Model $Id$');
	
	# Do we have a data version?
	my $dataVersion = undef;
	if(exists($self->annotations->hash->{'data-version'})) {
		$dataVersion = $self->annotations->hash->{'data-version'};
	} else {
		$dataVersion = sprintf('%04d-%02d-%02d',$timetoks[5]+1900,$timetoks[4]+1,$timetoks[3]);
	}
	BP::Model::CV::Common::printOboKeyVal($O,'data-version',toOBO($dataVersion));
	
	# Are there descriptions?
	foreach my $desc (@{$self->description}) {
		BP::Model::CV::Common::printOboKeyVal($O,'remark',toOBO($desc));
	}
	
	# And now, print each one of the terms
	my $CVhash = $self->CV;
	foreach my $term (@{$self->order},@{$self->aliasOrder}) {
		$CVhash->{$term}->OBOserialize($O);
	}
}

sub version() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $dataVersion = undef;
	$dataVersion = $self->annotations->hash->{'data-version'}  if(exists($self->annotations->hash->{'data-version'}));
	
	return $dataVersion;
}

1;

