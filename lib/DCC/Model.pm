#!/usr/bin/perl -W

use strict;

use Carp;
use File::Basename;
use File::Spec;
use XML::LibXML;

package DCC::Model;
#use version 0.77;
#our $VERSION = qv('0.2.0');

# Used "blesses"
# DCC::Model::Collection
# DCC::Model::Index
# DCC::Model::DescriptionSet
# DCC::Model::AnnotationSet
# DCC::Model::CV
# DCC::Model::ConceptType
# DCC::Model::ColumnSet
# DCC::Model::ComplexType
# DCC::Model::ColumnType
# DCC::Model::Column
# DCC::Model::FilenamePattern

use constant DCCSchemaFilename => 'bp-schema.xsd';
use constant dccNamespace => 'http://www.blueprint-epigenome.eu/dcc/schema';
use constant ItemTypes => {
	'string'	=> 1,	# With this we avoid costly checks
	'integer'	=> qr/^0|(?:-?[1-9][0-9]*)$/,
	'decimal'	=> qr/^(?:0|(?:-?[1-9][0-9]*))(?:\.[0-9]+)?$/,
	'boolean'	=> qr/^[10]|[tT](?:rue)?|[fF](?:alse)?|[yY](?:es)?|[nN]o?$/,
	'timestamp'	=> qr/^[1-9][0-9][0-9][0-9](?:(?:1[0-2])|(?:0[1-9]))(?:(?:[0-2][0-9])|(?:3[0-1]))$/,
	'complex'	=> undef
};

use constant FileTypeSymbolPrefixes => {
	'$' => 'DCC::Annotation',
	'@' => 'DCC::Model::CV',
	'\\' => 'Pattern',
	'%' => 'DCC::Type'
};

##############
# Prototypes #
##############

# 'new' is not included in the prototypes
sub digestModel($);
sub parseDescriptions($);
sub parseAnnotations($);
sub parseCVElement($);

sub __parse_pattern($;$);

sub load_patterns($);
sub parseConceptType($;$);
sub parseColumnSet($$);
sub parseColumn($);
sub parseFilenameFormat($);

#################
# Class methods #
#################

# The constructor takes as input the filename
sub new($) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	# my $self  = $class->SUPER::new();
	my $self = {};
	bless($self,$class);
	
	# Let's start processing the input model
	my $modelPath = shift;
	my $modelAbsPath = File::Spec->rel2abs($modelPath);
	my $modelDir = dirname($modelAbsPath);
	
	$self->{_modelDir}=$modelDir;
	
	# DCC model XML file parsing
	my $model = undef;
	
	eval {
		XML::LibXML->load_xml(location=>$modelPath);
	};
	
	# Was there some model parsing error?
	if($@) {
		Carp::croak("Error while parsing model $modelPath: ".$@);
	}
	
	# Schema preparation
	my $schemaDir = dirname(__FILE__);
	$self->{_schemaDir}=$schemaDir;
	
	my $schemaPath = File::Spec->catfile($schemaDir,DCCSchemaFilename);
	my $dccschema = XML::LibXML::Schema->new(location=>$schemaPath);
	
	# Model validated against the XML Schema
	eval {
		$dccschema->validate($model);
	};
	
	# Was there some schema validation error?
	if($@) {
		Carp::croak("Error while validating model $modelPath against $schemaPath: ".$@);
	}
	
	# Setting the internal system item types
	%{$self->{TYPES}} = %{(ItemTypes)};
	
	# No error, so, let's process it!!
	$self->digestModel($model);
	
	return $self;
}

####################
# Instance Methods #
####################

# digestModel parameters:
#	model: XML::LibXML::Document node following DCC schema
# The method parses the input XML::LibXML::Document and fills in the
# internal memory structures used to represent a DCC model
sub digestModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $model = shift;
	my $modelRoot = $model->documentElement();
	
	# First, let's store the key values, project name and schema version
	$self->{project} = $model->getAttribute('project');
	$self->{schemaVer} = $model->getAttribute('schemaVer');
	
	# Now, let's store the annotations
	$self->{ANNOTATIONS} = $self->parseAnnotations($modelRoot);
	
	# Now, the collection domain
	my %collections = ();
	$self->{COLLECTIONS}=\%collections;
	foreach my $colDom ($modelRoot->getChildrenByTagNameNS(dccNamespace,'collection-domain')) {
		foreach my $coll ($colDom->childNodes()) {
			next  unless($coll->nodeType == XML::LibXML::XML_ELEMENT_NODE && $coll->localname eq 'collection');
			
			# Collection name, collection path, index declarations
			my @collection = ($coll->getAttribute('name'),$coll->getAttribute('path'),[]);
			$collections{$collection[0]} = bless(\@collection,'DCC::Model::Collection');
			
			# And the index declarations for this collection
			foreach my $ind ($coll->childNodes()) {
				next  unless($ind->nodeType == XML::LibXML::XML_ELEMENT_NODE && $ind->localname eq 'index');
				
				# Is index unique?, attributes (attribute name, ascending/descending)
				my @index = (($ind->hasAttribute('unique') && $ind->getAttribute('unique') eq 'true')?1:undef,[]);
				push(@{$collection[2]},bless(\@index,'DCC::Model::Index'));
				
				foreach my $attr ($ind->childNodes()) {
					next  unless($attr->nodeType == XML::LibXML::XML_ELEMENT_NODE && $attr->localname eq 'attr');
					
					push(@{$index[1]},[$attr->getAttribute('name'),($attr->hasAttribute('ord') && $attr->getAttribute('ord') eq '-1')?-1:1]);
				}
			}
		}
		last;
	}
	
	# Next stop, controlled vocabulary
	my %cv = ();
	$self->{CV} = \%cv;
	foreach my $cvDecl ($modelRoot->getChildrenByTagNameNS(dccNamespace,'cv-declarations')) {
		# Let's setup the path to disk stored CVs
		my $cvdir = undef;
		$cvdir = $cvDecl->getAttribute('dir')  if($cvDecl->hasAttribute('dir'));
		if(defined($cvdir)) {
			# We need to translate relative paths to absolute ones
			$cvdir = File::Spec->rel2abs($cvdir,$self->{_modelDir})  unless(File::Spec->file_name_is_absolute($cvdir));
		} else {
			$cvdir = $self->{_modelDir};
		}
		
		$self->{_cvDir} = $cvdir;
		
		my $destCVCol = $cvDecl->getAttribute('collection');
		if(exists($collections{$destCVCol})) {
			$self->{_cvColl} = $collections{$destCVCol};
		} else {
			Carp::croak("Destination collection $destCVCol for CV has not been declared");
		}
		foreach my $cv ($cvDecl->childNodes()) {
			next  unless($cv->nodeType == XML::LibXML::XML_ELEMENT_NODE && $cv->localname eq 'cv');
			
			my $p_structCV = $self->parseCVElement($cv);
			
			# Let's store named CVs here, not anonymous ones
			$cv{$p_structCV->name}=$p_structCV  if(defined($p_structCV->name));
		}
		
		last;
	}
	
	# A safeguard for this parameter
	$self->{_cvDir} = $self->{_modelDir}  unless(exists($self->{_cvDir}));
	
	# Now, the pattern declarations
	foreach my $patternDecl ($modelRoot->getChildrenByTagNameNS(dccNamespace,'pattern-declarations')) {
		$self->load_patterns($patternDecl);
		
		last;
	}
	
	# And we start with the concept types
	my %conceptTypes = ();
	$self->{CTYPES} = \%conceptTypes;
	foreach my $conceptTypesDecl ($modelRoot->getChildrenByTagNameNS(dccNamespace,'concept-types')) {
		foreach my $ctype ($conceptTypesDecl->childNodes()) {
			next  unless($ctype->nodeType == XML::LibXML::XML_ELEMENT_NODE && $ctype->localname eq 'concept-type');
			
			my @conceptTypes = $self->parseConceptType($ctype);
			
			# Now, let's store the concrete (non-anonymous, abstract) concept types
			map { $conceptTypes{$_->[0]} = $_  if(defined($_->[0])); } @conceptTypes;
		}
		
		last;
	}
	
	# The different filename formats
	my %filenameFormats = ();
	$self->{FPATTERN} = \%filenameFormats;
	
	foreach my $filenameFormatDecl ($modelRoot->getChildrenByTagNameNS(dccNamespace,'filename-format')) {
		my $filenameFormat = $self->parseFilenameFormat($filenameFormatDecl);
		
		$filenameFormats{$filenameFormat->name} = $filenameFormat;
	}
}

# parseDescriptions parameters:
#	container: a XML::LibXML::Element container of 'dcc:description' elements
# returns a DCC::Model::DescriptionSet array reference, containing the contents of all
# the 'dcc:description' elements found
sub parseDescriptions($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $container = shift;
	
	my @descriptions = ();
	foreach my $description ($container->getChildrenByTagNameNS(dccNamespace,'description')) {
		push(@descriptions,$description->textContent());
	}
	
	return bless(\@descriptions,'DCC::Model::DescriptionSet');
}

# parseAnnotations paremeters:
#	container: a XML::LibXML::Element container of 'dcc:annotation' elements
# returns a DCC::Model::AnnotationSet hash reference, containing the contents of all the
# 'dcc:annotation' elements found
sub parseAnnotations($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $container = shift;
	
	my %annotations = ();
	foreach my $annotation ($container->getChildrenByTagNameNS(dccNamespace,'annotation')) {
		$annotations{$annotation->getAttribute('key')} = $annotation->textContent();
	}
	
	return bless(\%annotations,'DCC::Model::AnnotationSet');
}

# parseCVElement parameters:
#	cv: a XML::LibXML::Element 'dcc:cv' node
# returns a DCC:CV array reference, with all the controlled vocabulary
# stored inside. If the CV is in an external file, this method reads it
sub parseCVElement($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cv = shift;
	
	# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs and the CV
	my %cvAnnot = ();
	my @cvDoc = ();
	my @structCV=(undef,undef,bless(\%cvAnnot,'DCC::Model::AnnotationSet'),bless(\@cvDoc,'DCC::Model::DescriptionSet'),{});
	
	$structCV[0] = $cv->getAttribute('name')  if($cv->hasAttribute('name'));
	
	foreach my $el ($cv->childNodes()) {
		next  unless($el->nodeType == XML::LibXML::XML_ELEMENT_NODE);
		
		if($el->localname eq 'e') {
			$structCV[4]{$el->getAttribute('v')} = $el->textContent();
		} elsif($el->localname eq 'cv-file') {
			my $cvPath = $el->textContent();
			$cvPath  = File::Spec->rel2abs($cvPath,$self->{_cvDir})  unless(File::Spec->file_name_is_absolute($cvPath));
			
			my $CV;
			if(open($CV,'<',$cvPath)) {
				while(my $cvline=<$CV>) {
					if(substr($cvline,0,1) eq '#') {
						if(substr($cvline,1,2) eq '#') {
							# Registring the additional documentation
							chomp($cvline);
							my($key,$value) = split(/ /,$cvline,2);
							
							if($key eq '') {
								push(@cvDoc,$value);
							} else {
								$cvAnnot{$key}=$value;
							}
						} else {
							next;
						}
					}
					
					chomp($cvline);
					my($key,$value) = split(/\t/,$cvline,2);
					$structCV[4]{$key}=$value;
				}
				close($CV);
			} else {
				Carp::croak("Unable to open CV file $cvPath");
			}
		}
	}
	
	return bless(\@structCV,'DCC::Model::CV');
}

# __parse_pattern parameters:
#	pattern: a XML::LibXML::Element 'dcc:pattern' node
#	localname: an optional local name given to this
#		pattern, used when an error is signaled.
# returns a Regexp instance, which has been extracted from the input node
sub __parse_pattern($;$) {
	my($pattern,$localname)=@_;
	
	$localname='(anonymous)'  unless(defined($localname));
	
	my($pat)=undef;

	if($pattern->hasAttribute('match')) {
		$pat=$pattern->getAttribute('match');
		eval {
			$pat = qr/^$pat$/;
		};

		if($@) {
			Carp::croak("ERROR: Invalid pattern '$localname' => $pat. Reason: $@\n");
		}
	}
	
	# Undef patterns is no pattern
	return $pat;
}

# load_patterns parameters:
#	patternDecl: a XML::LibXML::Element 'dcc:pattern-declarations' node
# it returns nothing, but it stores in the instance, under the 'PATTERNS'
# key all the declared patterns in the instance
sub load_patterns($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $patternDecl = shift;
	
	my %PATTERN = ();
	$self->{PATTERNS}=\%PATTERN;
	
	my(@patterns)=$patternDecl->childNodes();
	foreach my $pattern (@patterns) {
		next unless($pattern->nodeType()==XML::LibXML::XML_ELEMENT_NODE);
		
		my($localname)=$pattern->localname();
		if($localname eq 'pattern') {
			my($name)=$pattern->getAttribute('name');
			
			Carp::croak("ERROR: duplicate pattern declaration: $name\n")  if(exists($PATTERN{$name}));
			
			$PATTERN{$name} = __parse_pattern($pattern,$name);
		#} else {
		#	warn "WARNING: definitions file is garbled. Found unexpected element in pattern section: $localname\n";
		}
	}
}

# parseConceptType parameters:
#	ctypeElem: a XML::LibXML::Element 'dcc:concept-type' node
#	ctypeParent: an optional 'DCC::Model::ConceptType' instance, which is the
#		parent concept type
# returns an array of 'DCC::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseConceptType($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $ctypeElem = shift;
	# Optional parameter, the conceptType parent
	my $ctypeParent = undef;
	$ctypeParent = shift  if(scalar(@_) > 0);
	
	# concept-type name (could be anonymous)
	# collection/key based (true,undef)
	# collection/key name (value,undef)
	# parent
	# columnSet
	my @ctype = (undef,undef,undef,$ctypeParent,undef);
	
	# If it has name, then it has to have either a collection name or a key name
	# either inline or inherited from the ancestors
	if($ctypeElem->hasAttribute('name')) {
		$ctype[0] = $ctypeElem->getAttribute('name');
		
		if($ctypeElem->hasAttribute('collection')) {
			if($ctypeElem->hasAttribute('key')) {
				Carp::croak("A concept type cannot have a quantum storage state of physical and virtual collection");
			}
			
			$ctype[1] = 1;
			$ctype[2] = $ctypeElem->getAttribute('collection');
		} elsif($ctypeElem->hasAttribute('key')) {
			$ctype[2] = $ctypeElem->getAttribute('key');
		} elsif(defined($ctypeParent) && defined($ctypeParent->[2])) {
			# Let's fetch the inheritance
			$ctype[1] = $ctypeParent->[1];
			$ctype[2] = $ctypeParent->[2];
		} else {
			Carp::croak("A concept type must have a storage state of physical or virtual collection");
		}
	}
	
	# The returning values array
	my $me = bless(\@ctype,'DCC::Model::ConceptType');
	my @retval = ($me);
	
	# Let's parse the columns
	$ctype[4] = $self->parseColumnSet($ctypeElem,defined($ctypeParent)?$ctypeParent->[4]:undef);
	
	# Now, let's find subtypes
	foreach my $subtypes ($ctypeElem->getChildrenByTagNameNS(dccNamespace,'subtypes')) {
		foreach my $childCTypeElem ($subtypes->childNodes()) {
			next  unless($childCTypeElem->nodeType == XML::LibXML::XML_ELEMENT_NODE && $childCTypeElem->localname() eq 'concept-type');
			
			# Parse subtypes and store them!
			push(@retval,$self->parseConceptType($childCTypeElem,$me));
		}
		last;
	}
	
	return @retval;
}

# parseColumnSet parameters:
#	container: a XML::LibXML::Element node, containing 'dcc:column' elements
#	parentColumnSet: a DCC::Model::ColumnSet instance, which is the parent.
# returns a DCC::Model::ColumnSet instance with all the DCC::Model::Column instances (including
# the inherited ones from the parent).
sub parseColumnSet($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $container = shift;
	my $parentColumnSet = shift;
	
	my @columnNames = ();
	my %columnDecl = ();
	# Inheriting column information from parent columnSet
	if(defined($parentColumnSet)) {
		@columnNames = @{$parentColumnSet->[0]};
		%columnDecl = %{$parentColumnSet->[1]};
	}
	my @columnSet = (\@columnNames,\%columnDecl);
	
	foreach my $colDecl ($container->childNodes()) {
		next  unless($colDecl->nodeType == XML::LibXML::XML_ELEMENT_NODE && $colDecl->localname() eq 'column');
		
		my $column = $self->parseColumn($colDecl);
		
		# We want to keep the original column order as far as possible
		push(@columnNames,$column->[0])  unless(exists($columnDecl{$column->[0]}));
		$columnDecl{$column->[0]}=$column;
	}
	
	return bless(\@columnSet,'DCC::Model::ColumnSet');
}

# parseColumn parameters:
#	colDecl: a XML::LibXML::Element 'dcc:column' node, which defines
#		a column
# returns a DCC::Model::Column instance, with all the information related to
# types, restrictions and enumerated values used by this column.
sub parseColumn($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $colDecl = shift;
	
	# Item type
	# column use (idref, required, optional)
	# content restrictions
	# default value
	# array separators
	my @columnType = ();
	# Column name, description, annotations, column type
	my @column = (
		$colDecl->getAttribute('name'),
		$self->parseDescriptions($colDecl),
		$self->parseAnnotations($colDecl),
		bless(\@columnType,'DCC::Model::ColumnType')
	);
	
	# Let's parse the column type!
	foreach my $colType ($colDecl->getChildrenByTagNameNS(dccNamespace,'column-type')) {
		#First, the item type
		my $itemType = $colType->getAttribute('item-type');
		
		Carp::croak("unknown type '$itemType' for column $column[0]")  unless(exists($self->{TYPES}{$itemType}));
		
		$columnType[0] = $itemType;
		
		# Column use
		my $columnKind = $colType->getAttribute('column-kind');
		# Idref equals 0; required, 1; optional, -1
		$columnType[1] = ($columnKind eq 'idref')?0:(($columnKind eq 'required')?1:-1);
		
		# Content restrictions (children have precedence over attributes)
		# First, is it a complex type?
		unless(defined($self->{TYPES}{$itemType})) {
			if($colType->hasAttribute('complex-template') && $colType->hasAttribute('complex-seps')) {
				# tokens, separators
				my $seps = $colType->getAttribute('complex-seps');
				my @tokenNames = split(/[$seps]/,$colType->getAttribute('complex-template'));
				
				# TODO: refactor complex types
				# complex separators, token names
				my @complexDecl = ($seps,\@tokenNames);
				$columnType[2] = bless(\@complexDecl,'DCC::Model::ComplexType');
			} else {
				Carp::croak("Column $column[0] was declared as complex, but some of the needed attributes (complex-template, complex-seps) is not declared");
			}
		} else {
			my @cvChildren = $colType->getChildrenByTagNameNS(dccNamespace,'cv');
			my @patChildren = $colType->getChildrenByTagNameNS(dccNamespace,'pattern');
			if(scalar(@cvChildren)>0 || (scalar(@patChildren)==0 && $colType->hasAttribute('cv'))) {
				if(scalar(@cvChildren)>0) {
					$columnType[2] = $self->parseCVElement($cvChildren[0]);
				} elsif(exists($self->{CV}{$colType->getAttribute('cv')})) {
					$columnType[2] = $self->{CV}{$colType->getAttribute('cv')};
				} else {
					Carp::croak("Column $column[0] tried to use undeclared CV ".$colType->getAttribute('cv'));
				}
			} elsif(scalar(@patChildren)>0) {
				$columnType[2] = __parse_pattern($patChildren[0]);
			} elsif($colType->hasAttribute('pattern')) {
				if(exists($self->{PATTERNS}{$colType->getAttribute('pattern')})) {
					$columnType[2] = $self->{PATTERNS}{$colType->getAttribute('pattern')};
				} else {
					Carp::croak("Column $column[0] tried to use undeclared pattern ".$colType->getAttribute('pattern'));
				}
			} else {
				$columnType[2] = undef;
			}
		}
		
		# Default value
		$columnType[3] = $colType->hasAttribute('default')?$colType->getAttribute('default'):undef;
		
		# Array separators
		$columnType[4] = ($colType->hasAttribute('array-seps') && length($colType->hasAttribute('array-seps')) > 0)?$colType->getAttribute('array-seps'):undef;
		
		last;
	}
	
	return bless(\@column,'DCC::Model::Column');
}

# parseFilenameFormat parameters:
#	filenameFormatDecl: a XML::LibXML::Element 'dcc:filename-format' node
#		which has all the information to defined a named filename format
#		(or pattern)
# returns a DCC::Model::FilenamePattern instance, with all the needed information
# to recognise a filename following the defined pattern
sub parseFilenameFormat($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $filenameFormatDecl = shift;
	
	# First, let's get the symbolic name
	my $name = $filenameFormatDecl->getAttribute('name');
	
	# This array will hold the different variable parts to validate
	# from the file pattern
	my @parts = ();
	
	# Name, regular expression, parts
	my @filenamePattern = ($name,undef,\@parts);
	
	# And now, the format string
	my $formatString = $filenameFormatDecl->textContent();
	
	# The valid separators
	# To be revised (scaping)
	my $validSeps = join('',keys(%{(FileTypeSymbolPrefixes)}));
	my $validSepsR = '['.$validSeps.']';
	my $validSepsN = '[^'.$validSeps.']+';
	
	my $pattern = '^';
	my $tokenString = $formatString;
	
	# First one, the origin
	if($tokenString =~ /^($validSepsN)/) {
		$pattern .= '\Q'.$1.'\E';
		$tokenString = substr($tokenString,length($1));
	}
	
	# Now, the different pieces
	while($tokenString =~ /([$validSepsR])(\$?[a-zA-Z][a-zA-Z0-9]*)([^$validSeps]*)/g) {
		# Pattern for the content
		if(FileTypeSymbolPrefixes->{$1} eq 'Pattern') {
			if(exists($self->{PATTERNS}{$2})) {
				# Check against the pattern!
				$pattern .= '('.$self->{PATTERNS}{$2}.')';
				
				# No additional check
				push(@parts,undef);
			} else {
				Carp::croak("Unknown pattern '$2' used in filename-format '$formatString'");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'DCC::Type') {
			if(exists($self->{TYPES}{$2})) {
				my $type = $self->{TYPES}{$2};
				if(defined($type)) {
					$pattern .= '('.((ref($type) eq 'Pattern')?$type:'.+').')';
					
					# No additional check
					push(@parts,undef);
				} else {
					Carp::croak("Type '$2' used in filename-format '$formatString' was not simple");
				}
			} else {
				Carp::croak("Unknown type '$2' used in filename-format '$formatString'");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'DCC::Model::CV') {
			if(exists($self->{CV}{$2})) {
				$pattern .= '(.+)';
				
				# Check the value against the CV
				push(@parts,$self->{CV}{$2});
			} else {
				Carp::croak("Unknown controlled vocabulary '$2' used in filename-format '$formatString'");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'DCC::Annotation') {
			my $annot = $2;
			
			# Is it a context-constant?
			if(substr($annot,0,1) eq '$') {
				$pattern .= '(.+)';
				
				# Store the value in this context variable
				push(@parts,$annot);
			} elsif(exists($self->{ANNOTATIONS}{$annot})) {
				# As annotations are at this point known constants, then check the exact value
				$pattern .= '(\Q'.$self->{ANNOTATIONS}{$annot}.'\E)';
				
				# No additional check
				push(@parts,undef);
			} else {
				Carp::croak("Unknown annotation '$2' used in filename-format '$formatString'");
			}
		} else {
			# For unimplemented checks (shouldn't happen)
			$pattern .= '(.+)';
			
			# No checks, because we don't know what to check
			push(@parts,undef);
		}
		
		# The uninteresting value
		$pattern .= '\Q'.$3.'\E'  if(defined($3) && length($3)>0);
	}
	
	# Finishing the pattern building
	$pattern .= '$';
	
	# Now, the Pattern object!
	$filenamePattern[1] = qr/$pattern/;
	
	return bless(\@filenamePattern,'DCC::Model::FilenamePattern');
}

# And now, the helpers for the different pseudo-packages

package DCC::Model::Collection;


package DCC::Model::Index;


package DCC::Model::DescriptionSet;


package DCC::Model::AnnotationSet;


package DCC::Model::CV;
# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs and the CV
my @structCV=(undef,undef,{},[],{});

# The name of this CV (optional)
sub name {
	return $_[0]->[0];
}

# Filename holding this values (optional)
sub filename {
	return $_[0]->[1];
}

# An instance of a DCC::Model::AnnotationSet, holding the annotations
# for this CV
sub annotations {
	return $_[0]->[2];
}

# An instance of a DCC::Model::DescriptionSet, holding the documentation
# for this CV
sub documentation {
	return $_[0]->[3];
}

# The hash holding the CV in memory
sub CV {
	return $_[0]->[4];
}

# With this method a key is validated
sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvkey = shift;
	
	return exists($self->CV->{$cvkey});
}


package DCC::Model::ConceptType;


package DCC::Model::ColumnSet;


package DCC::Model::ComplexType;
# TODO: Complex type refactor in the near future, so work for filename patterns
# can be reused


package DCC::Model::ColumnType;

# Item type
sub type {
	return $_[0]->[0];
}

# column use (idref, required, optional)
# Idref equals 0; required, 1; optional, -1
sub use {
	return $_[0]->[1];
}

# content restrictions
sub restriction {
	return $_[0]->[2];
}

# default value
sub default {
	return $_[0]->[3];
}

# array separators
sub arraySeps {
	return $_[0]->[4];
}


package DCC::Model::Column;

# The column name
sub name {
	return $_[0]->[0];
}

# The description, a DCC::Model::DescriptionSet instance
sub description {
	return $_[0]->[1];
}

# Annotations, a DCC::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[2];
}

# It returns a DCC::Model::ColumnType instance
sub columnType {
	return $_[0]->[3];
}


package DCC::Model::FilenamePattern;

# Name, regular expression, parts

sub name {
	return $_[0]->[0];
}

sub pattern {
	return $_[0]->[1];
}

sub postValidationParts {
	return $_[0]->[2];
}

# This method tries to match an input string against the filename-format pattern
# If it works, it returns a reference to a hash with the matched values correlated to
# the context constants
sub match($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $filename = shift;
	
	# Let's match the values!
	my @values = $filename =~ $self->pattern;
	
	my $p_parts = $self->postValidationParts;
	
	if(scalar(@values) ne scalar(@{$p_parts})) {
		Carp::croak("Matching filename $filename against pattern ".$self->pattern." did not work!");
	}
	
	my %rethash = ();
	my $ipart = -1;
	foreach my $part (@{$p_parts}) {
		$ipart++;
		next  unless(defined($part));
		
		# Is it a CV?
		if(ref($part) eq 'DCC::Model::CV') {
			Carp::croak('Validation against CV did not match')  unless($part->isValid($values[$ipart]));
		# Context constant
		} elsif(ref($part) eq '') {
			$rethash{$part} = $values[$ipart];
		}
	}
	
	return \%rethash;
}

1;