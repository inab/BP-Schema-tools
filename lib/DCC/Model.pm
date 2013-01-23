#!/usr/bin/perl -W

use strict;

use Carp;
use File::Basename;
use File::Spec;
use XML::LibXML;

# Early subpackage constant declarations
package DCC::Model::ColumnType;

use constant {
	IDREF	=>	0,
	REQUIRED	=>	1,
	OPTIONAL	=>	-1,
};


# Main package
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
# DCC::Model::ConceptDomain
# DCC::Model::Concept

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
	my $modelDir = File::Basename::dirname($modelAbsPath);
	
	$self->{_modelDir}=$modelDir;
	
	# DCC model XML file parsing
	my $model = undef;
	
	eval {
		$model = XML::LibXML->load_xml(location=>$modelPath);
	};
	
	# Was there some model parsing error?
	if($@) {
		Carp::croak("Error while parsing model $modelPath: ".$@);
	}
	
	# Schema preparation
	my $schemaDir = File::Basename::dirname(__FILE__);
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
	
	my $modelDoc = shift;
	my $modelRoot = $modelDoc->documentElement();
	
	# First, let's store the key values, project name and schema version
	$self->{project} = $modelRoot->getAttribute('project');
	$self->{schemaVer} = $modelRoot->getAttribute('schemaVer');
	
	# The documentation directory, which complements this model
	my $docsDir = $modelRoot->getAttribute('docsDir');
	# We need to translate relative paths to absolute ones
	$docsDir = File::Spec->rel2abs($docsDir,$self->{_modelDir})  unless(File::Spec->file_name_is_absolute($docsDir));
	$self->{_docsDir} = $docsDir;
	
	# Now, let's store the annotations
	$self->{ANNOTATIONS} = $self->parseAnnotations($modelRoot);
	
	# Now, the collection domain
	my %collections = ();
	$self->{COLLECTIONS}=\%collections;
	foreach my $colDom ($modelRoot->getChildrenByTagNameNS(dccNamespace,'collection-domain')) {
		foreach my $coll ($colDom->childNodes()) {
			next  unless($coll->nodeType == XML::LibXML::XML_ELEMENT_NODE && $coll->localname eq 'collection');
			
			# Collection name, collection path, index declarations
			my @collection = ($coll->getAttribute('name'),$coll->getAttribute('path'),undef);
			$collections{$collection[0]} = bless(\@collection,'DCC::Model::Collection');
			
			# And the index declarations for this collection
			$collection[2] = $self->parseIndexes($coll);
		}
		last;
	}
	
	# Next stop, controlled vocabulary
	my %cv = ();
	my @cvArray = ();
	$self->{CV} = \%cv;
	$self->{CVARRAY} = \@cvArray;
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
			if(defined($p_structCV->name)) {
				$cv{$p_structCV->name}=$p_structCV;
				push(@cvArray,$p_structCV);
			}
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
			map { $conceptTypes{$_->name} = $_  if(defined($_->name)); } @conceptTypes;
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
	
	# Oh, no! The concept domains!
	my @conceptDomains = ();
	my %conceptDomainHash = ();
	$self->{CDOMAINS} = \@conceptDomains;
	$self->{CDOMAINHASH} = \%conceptDomainHash;
	foreach my $conceptDomainDecl ($modelRoot->getChildrenByTagNameNS(dccNamespace,'concept-domain')) {
		my $conceptDomain = $self->parseConceptDomain($conceptDomainDecl);
		
		push(@conceptDomains,$conceptDomain);
		$conceptDomainHash{$conceptDomain->name} = $conceptDomain;
	}
	
	# But the work it is not finished, because
	# we have to propagate the foreign keys
	foreach my $conceptDomain (@conceptDomains) {
		foreach my $concept (@{$conceptDomain->concepts}) {
			foreach my $relatedConceptNames (@{$concept->relatedConceptNames}) {
				my($domainName, $conceptName, $prefix) = @{$relatedConceptNames};
				
				my $relatedDomain = $conceptDomain;
				if(defined($domainName)) {
					unless(exists($conceptDomainHash{$domainName})) {
						Carp::croak("Concept domain $domainName referred from concept ".$conceptDomain->name.'.'.$concept->name." does not exist");
					}
					
					$relatedDomain = $conceptDomainHash{$domainName};
				}
				
				my $relatedConcept = undef;
				if(exists($relatedDomain->conceptHash->{$conceptName})) {
					$relatedConcept = $relatedDomain->conceptHash->{$conceptName};
				} else {
					Carp::croak("Concept $domainName.$conceptName referred from concept ".$conceptDomain->name.'.'.$concept->name." does not exist");
				}
				
				# And now, let's propagate!
				$concept->columnSet->addColumns($relatedConcept->refColumns($prefix));
			}
		}
	}
	
	# That's all folks, friends!
}

# parseIndexes parameters:
#	container: a XML::LibXML::Element container of 'dcc:index' elements
# returns an array reference, containing DCC::Model::Index instances
sub parseIndexes($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $container = shift;
	
	# And the index declarations for this collection
	my @indexes = ();
	foreach my $ind ($container->getChildrenByTagNameNS(dccNamespace,'index')) {
		# Is index unique?, attributes (attribute name, ascending/descending)
		my @index = (($ind->hasAttribute('unique') && $ind->getAttribute('unique') eq 'true')?1:undef,[]);
		push(@indexes,bless(\@index,'DCC::Model::Index'));
		
		foreach my $attr ($ind->childNodes()) {
			next  unless($attr->nodeType == XML::LibXML::XML_ELEMENT_NODE && $attr->localname eq 'attr');
			
			push(@{$index[1]},[$attr->getAttribute('name'),($attr->hasAttribute('ord') && $attr->getAttribute('ord') eq '-1')?-1:1]);
		}
	}
	
	return \@indexes;
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
	
	my %annotationHash = ();
	my @annotationOrder = ();
	my @annotations = (\%annotationHash,\@annotationOrder);
	foreach my $annotation ($container->getChildrenByTagNameNS(dccNamespace,'annotation')) {
		unless(exists($annotationHash{$annotation->getAttribute('key')})) {
			push(@annotationOrder,$annotation->getAttribute('key'));
		}
		$annotationHash{$annotation->getAttribute('key')} = $annotation->textContent();
	}
	
	return bless(\@annotations,'DCC::Model::AnnotationSet');
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
	my @cvAnnotKeys = ();
	my %cvAnnotHash = ();
	my @cvAnnot = (\%cvAnnotHash,\@cvAnnotKeys);
	my @cvDoc = ();
	my %cvHash = ();
	my @cvKeys = ();
	my @structCV=(undef,undef,bless(\@cvAnnot,'DCC::Model::AnnotationSet'),bless(\@cvDoc,'DCC::Model::DescriptionSet'),\%cvHash,\@cvKeys);
	
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
								$cvAnnotHash{$key}=$value;
								push(@cvAnnotKeys,$key);
							}
						} else {
							next;
						}
					}
					
					chomp($cvline);
					my($key,$value) = split(/\t/,$cvline,2);
					$cvHash{$key}=$value;
					push(@cvKeys,$key);
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
			Carp::croak("ERROR: Invalid pattern '$localname' => $pat. Reason: $@");
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
			
			Carp::croak("ERROR: duplicate pattern declaration: $name")  if(exists($PATTERN{$name}));
			
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
	# indexes
	my @ctype = (undef,undef,undef,$ctypeParent,undef,undef);
	
	# If it has name, then it has to have either a collection name or a key name
	# either inline or inherited from the ancestors
	if($ctypeElem->hasAttribute('name')) {
		$ctype[0] = $ctypeElem->getAttribute('name');
		
		if($ctypeElem->hasAttribute('collection')) {
			if($ctypeElem->hasAttribute('key')) {
				Carp::croak("A concept type cannot have a quantum storage state of physical and virtual collection");
			}
			
			$ctype[1] = 1;
			my $collName = $ctypeElem->getAttribute('collection');
			# Let's store the DCC::Model::Collection
			if(exists($self->{COLLECTIONS}{$collName})) {
				$ctype[2] = $self->{COLLECTIONS}{$collName};
			} else {
				Carp::croak("Collection '$collName', used by concept type '$ctype[0]', does not exist");
			}
		} elsif($ctypeElem->hasAttribute('key')) {
			$ctype[2] = $ctypeElem->getAttribute('key');
		} elsif(defined($ctypeParent) && defined($ctypeParent->path)) {
			# Let's fetch the inheritance
			$ctype[1] = $ctypeParent->isCollection;
			$ctype[2] = $ctypeParent->path;
		} else {
			Carp::croak("A concept type must have a storage state of physical or virtual collection");
		}
	}
	
	# The returning values array
	my $me = bless(\@ctype,'DCC::Model::ConceptType');
	my @retval = ($me);
	
	# Let's parse the columns
	$ctype[4] = $self->parseColumnSet($ctypeElem,defined($ctypeParent)?$ctypeParent->columnSet:undef);
	
	# And the index declarations
	$ctype[5] = [@{$self->parseIndexes($ctypeElem)}];
	# inheriting the ones from the parent concept types
	push(@{$ctype[5]},@{$ctypeParent->indexes})  if(defined($ctypeParent));
	
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
	my @idColumnNames = ();
	my %columnDecl = ();
	# Inheriting column information from parent columnSet
	if(defined($parentColumnSet)) {
		@idColumnNames = @{$parentColumnSet->idColumnNames};
		@columnNames = @{$parentColumnSet->columnNames};
		%columnDecl = %{$parentColumnSet->columns};
	}
	# Array with the idref column names
	# Array with column names (all)
	# Hash of DCC::Model::Column instances
	my @columnSet = (\@idColumnNames,\@columnNames,\%columnDecl);
	
	foreach my $colDecl ($container->childNodes()) {
		next  unless($colDecl->nodeType == XML::LibXML::XML_ELEMENT_NODE && $colDecl->localname() eq 'column');
		
		my $column = $self->parseColumn($colDecl);
		
		# We want to keep the original column order as far as possible
		if(exists($columnDecl{$column->name})) {
			if($columnDecl{$column->name}->columnType->use eq DCC::Model::ColumnType::IDREF) {
				Carp::croak('It is not allowed to redefine column '.$column->name.'. It is an idref one!');
			}
		} else {
			push(@columnNames,$column->name);
			# Is it a id column?
			push(@idColumnNames,$column->name)  if($column->columnType->use eq DCC::Model::ColumnType::IDREF);
		}
		
		$columnDecl{$column->name}=$column;
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
	my @columnType = (undef,undef,undef,undef,undef);
	# Column name, description, annotations, column type, is masked, related concept, related column from the concept
	my @column = (
		$colDecl->getAttribute('name'),
		$self->parseDescriptions($colDecl),
		$self->parseAnnotations($colDecl),
		bless(\@columnType,'DCC::Model::ColumnType'),
		undef,
		undef,
		undef
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
		$columnType[1] = ($columnKind eq 'idref')?DCC::Model::ColumnType::IDREF :(($columnKind eq 'required')?DCC::Model::ColumnType::REQUIRED : DCC::Model::ColumnType::OPTIONAL);
		
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
	
	# Name, regular expression, parts, registered concept domains
	my @filenamePattern = ($name,undef,\@parts,{});
	
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
				$pattern .= '(\Q'.$self->{ANNOTATIONS}->hash->{$annot}.'\E)';
				
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

# parseConceptDomain parameters:
#	conceptDomainDecl: a XML::LibXML::Element 'dcc:concept-domain' element
# it returns a DCC::Model::ConceptDomain instance, with all the concept domain
# structures and data
sub parseConceptDomain($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptDomainDecl = shift;
	
	# concept domain name
	# full name of the concept domain
	# Filename Pattern for the filenames
	# An array with the concepts under this concept domain umbrella
	my @concepts = ();
	my %conceptHash = ();
	my @conceptDomain = (
		$conceptDomainDecl->getAttribute('domain'),
		$conceptDomainDecl->getAttribute('fullname'),
		undef,
		\@concepts,
		\%conceptHash
	);
	
	# Does the filename-pattern exist?
	my $filenameFormatName = $conceptDomainDecl->getAttribute('filename-format');
	unless(exists($self->{FPATTERN}{$filenameFormatName})) {
		Carp::croak("Concept domain $conceptDomain[0] uses the unknown filename format $filenameFormatName");
	}
	
	$conceptDomain[2] = $self->{FPATTERN}{$filenameFormatName};
	
	# Last, chicken and egg problem, part 1
	my $retConceptDomain = bless(\@conceptDomain,'DCC::Model::ConceptDomain');

	# And now, next method handles parsing of embedded concepts
	push(@concepts,$self->parseConceptContainer($conceptDomainDecl,$retConceptDomain));
	# The concept hash will help on concept identification
	map { $conceptHash{$_->name} = $_; } @concepts;
	
	# Last, chicken and egg problem, part 2
	$retConceptDomain->filenamePattern->registerConceptDomain($retConceptDomain);
	
	return $retConceptDomain;
}

# parseConceptContainer paramereters:
#	conceptContainerDecl: A XML::LibXML::Element 'dcc:concept-domain'
#		or 'dcc:subconcepts' instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, where this concept
#		has been defined.
#	parentConcept: An optional, parent DCC::Model::Concept instance of
#		all the concepts to be parsed from the container
# it returns an array of DCC::Model::Concept instances, which are all the
# concepts and subconcepts inside the input concept container
sub parseConceptContainer($$;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptContainerDecl = shift;
	my $conceptDomain = shift;
	my $parentConcept = shift;	# This is optional (remember!)
	
	my @concepts = ();
	foreach my $conceptDecl ($conceptContainerDecl->getChildrenByTagNameNS(dccNamespace,'concept')) {
		push(@concepts,$self->parseConcept($conceptDecl,$conceptDomain,$parentConcept));
	}
	
	return @concepts;
}

# parseConcept paramereters:
#	conceptDecl: A XML::LibXML::Element 'dcc:concept' instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, where this concept
#		has been defined.
#	parentConcept: An optional, parent DCC::Model::Concept instance of
#		the concept to be parsed from conceptDecl
# it returns an array of DCC::Model::Concept instances, the first one
# corresponds to this concept, and the other ones are the subconcepts
sub parseConcept($$;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptDecl = shift;
	my $conceptDomain = shift;
	my $parentConcept = shift;	# This is optional (remember!)

	my $conceptName = $conceptDecl->getAttribute('name');
	my $conceptFullname = $conceptDecl->getAttribute('fullname');
	my $basetypeName = $conceptDecl->getAttribute('basetype');
	Carp::croak("Concept $conceptFullname ($conceptName) is based on undefined base type $basetypeName")  unless(exists($self->{CTYPES}{$basetypeName}));
	my $basetype = $self->{CTYPES}{$basetypeName};
	
	# This array will contain the names of the related concepts
	my @related = ();
	
	# We don't have to inherit the related concepts, because subconcepts
	# are not subclasses!!!!!
	#########push(@related,@{$parentConcept->relatedConceptNames})  if(defined($parentConcept));
	
	# Preparing the columns
	my $columnSet = $self->parseColumnSet($conceptDecl,$basetype->columnSet);
	# and adding the ones from the parent concept
	# (and later from the related stuff)
	
	$columnSet->addColumns($parentConcept->columnSet->idColumns(! $parentConcept->baseConceptType->isCollection),1)  if(defined($parentConcept));
	
	# name
	# fullname
	# basetype
	# concept domain
	# Description Set
	# Annotation Set
	# ColumnSet
	# related conceptNames
	my @thisConcept = (
		$conceptName,
		$conceptFullname,
		$basetype,
		$conceptDomain,
		$self->parseDescriptions($conceptDecl),
		$self->parseAnnotations($conceptDecl),
		$columnSet,
		\@related,
	);
	
	# Saving the related concepts (the ones explicitly declared within this concept)
	foreach my $relatedDecl ($conceptDecl->getChildrenByTagNameNS(dccNamespace,'related-to')) {
		push(@related,[
			($relatedDecl->hasAttribute('domain'))?$relatedDecl->getAttribute('domain'):undef ,
			$relatedDecl->getAttribute('concept') ,
			($relatedDecl->hasAttribute('prefix'))?$relatedDecl->getAttribute('prefix'):undef ,
		]);
	}
	
	# And last, the subconcepts
	my $concept =  bless(\@thisConcept,'DCC::Model::Concept');
	
	my @concepts = ($concept);
	
	foreach my $conceptContainerDecl ($conceptDecl->getChildrenByTagNameNS(dccNamespace,'subconcepts')) {
		push(@concepts,$self->parseConceptContainer($conceptContainerDecl,$conceptDomain,$concept));
	}
	
	return @concepts;
}

# It returns an array with DCC::Model::ConceptDomain instances (all the concept domains)
sub conceptDomains() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{CDOMAINS};
}

# getConceptDomain parameters:
#	conceptDomainName: The name of the concept domain to look for
# returns a DCC::Model::ConceptDomain object or undef (if it does not exist)
sub getConceptDomain($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptDomainName = shift;
	
	return exists($self->{CDOMAINHASH}{$conceptDomainName})?$self->{CDOMAINHASH}{$conceptDomainName}:undef;
}

# It returns a DCC::Model::AnnotationSet instance
sub annotations() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{ANNOTATIONS};
}

# It returns the project name
sub projectName() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{project};
}

# It returns a schema version
sub schemaVer() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{schemaVer};
}

# The base documentation directory
sub documentationDir() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{_docsDir};
}

# A reference to an array of DCC::Model::CV instances
sub namedCVs() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{CVARRAY};
}

1;

# And now, the helpers for the different pseudo-packages

package DCC::Model::Collection;

# Collection name
sub name {
	return $_[0]->[0];
}

# collection path
sub path {
	return $_[0]->[1];
}

# index declarations
sub indexes {
	return $_[0]->[2];
}

#my @collection = ($coll->getAttribute('name'),$coll->getAttribute('path'),[]);

1;


package DCC::Model::Index;

# Is index unique?
sub isUnique {
	return $_[0]->[0];
}

# attributes (attribute name, ascending/descending)
sub indexAttributes {
	return $_[0]->[1];
}

1;


package DCC::Model::DescriptionSet;

# No method yet (it is a pure array)

1;


package DCC::Model::AnnotationSet;

sub hash {
	return $_[0]->[0];
}

# The order of the keys (when they are given in a description)
sub order {
	return $_[0]->[1];
}

1;


package DCC::Model::CV;

# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs and the CV
# my @structCV=(undef,undef,{},[],{});

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
sub description {
	return $_[0]->[3];
}

# The hash holding the CV in memory
sub CV {
	return $_[0]->[4];
}

# The order of the CV values (as in the file)
sub CVorder {
	return $_[0]->[5];
}

# With this method a key is validated
sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvkey = shift;
	
	return exists($self->CV->{$cvkey});
}

1;


package DCC::Model::ConceptType;

# concept-type name (could be anonymous), so it would return undef
sub name {
	return $_[0]->[0];
}

# collection/key based (true,undef)
sub isCollection {
	return $_[0]->[1];
}

# It can be either a DCC::Model::Collection instance
# or a string
sub path {
	return $_[0]->[2];
}

# collection, a DCC::Model::Collection instance
# An abstract concept type will return undef here
sub collection {
	return $_[0]->[2];
}

# The key name when this concept is stored as a value of an array inside a bigger concept
# An abstract concept type will return undef here
sub key {
	return $_[0]->[2];
}

# parent
# It returns either undef (when it has no parent),
# or a DCC::Model::ConceptType instance
sub parent {
	return $_[0]->[3];
}

# columnSet
# It returns a DCC::Model::ColumnSet instance, with all the column declarations
sub columnSet {
	return $_[0]->[4];
}

# It returns a reference to an array full of DCC::Model::Index instances
sub indexes {
	return $_[0]->[5];
}

1;


package DCC::Model::ColumnSet;

# Reference to an array with the idref column names
sub idColumnNames {
	return $_[0]->[0];
}

# Array with column names (all)
sub columnNames {
	return $_[0]->[1];
}

# Hash of DCC::Model::Column instances
sub columns {
	return $_[0]->[2];
}

# idColumns parameters:
#	doMask: Are the columns masked for storage?
# It returns a DCC::Model::ColumnSet instance, with the column declarations
# corresponding to columns with idref restriction
sub idColumns(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $doMask = shift;
	
	my @columnNames = @{$self->idColumnNames};
	my $p_columns = $self->columns;
	my %columns = map { $_ => $p_columns->{$_}->clone($doMask) } @columnNames;
	
	my @columnSet = (
		\@columnNames,
		\@columnNames,
		\%columns
	);
	
	return bless(\@columnSet,ref($self));
}

# refColumns parameters:
#	relatedConcept: A DCC::Model::Concept instance, which this columnSet belongs
#		The kind of relation could be inheritance, or 1:N
#	prefix: The optional prefix to be set to the name when the columns are cloned
# It returns a DCC::Model::ColumnSet instance, with the column declarations
# corresponding to columns with idref restriction
sub refColumns(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $relatedConcept = shift;
	my $prefix = shift;
	
	my @columnNames = @{$self->idColumnNames};
	my $p_columns = $self->columns;
	my %columns = map { $_ => $p_columns->{$_}->cloneRelated($relatedConcept,$prefix) } @columnNames;
	
	my @columnSet = (
		\@columnNames,
		\@columnNames,
		\%columns
	);
	
	return bless(\@columnSet,ref($self));
}

# addColumns parameters:
#	inputColumnSet: A DCC::Model::ColumnSet instance which contains
#		the columns to be added to this column set. Those
#		columns with the same name are overwritten.
#	isPKFriendly: if true, when the columns are idref, their role are
#		kept if there are already idref columns
# the method stores the columns in the input columnSet in the current
# one. New columns can override old ones, unless some of the old ones
# is typed as idref. In that case, an exception is fired.
sub addColumns($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $inputColumnSet = shift;
	my $isPKFriendly = shift;
	
	my $inputColumnsHash = $inputColumnSet->columns;
	
	# First, let's see whether there is some of our idkeys in the
	# input, so we can stop early
	foreach my $columnName (@{$self->idColumnNames}) {
		if(exists($inputColumnsHash->{$columnName})) {
			Carp::croak("Trying to add already declared idcolumn $columnName");
		}
	}
	my $doAddIDREF = scalar(@{$self->idColumnNames}) > 0 && $isPKFriendly;
	
	# And now, let's add them!
	my $p_columnsHash = $self->columns;
	my $p_columnNames = $self->columnNames;
	my $p_idColumnNames = $self->idColumnNames;
	
	# We want to keep the original column order as far as possible
	foreach my $inputColumnName (@{$inputColumnSet->columnNames}) {
		# Registering the column names (if there is no column with that name!)
		my $inputColumn = $inputColumnsHash->{$inputColumnName};
		unless(exists($p_columnsHash->{$inputColumnName})) {
			push(@{$p_columnNames},$inputColumnName);
			# Is it a id column which should be added?
			if($doAddIDREF && $inputColumn->columnType->use eq DCC::Model::ColumnType::IDREF) {
				push(@{$p_idColumnNames},$inputColumnName);
			}
		}
		
		$p_columnsHash->{$inputColumnName} = $inputColumn;
	}
}

1;


package DCC::Model::ComplexType;
# TODO: Complex type refactor in the near future, so work for filename patterns
# can be reused

1;


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

1;


package DCC::Model::Column;

use constant {
	NAME => 0,
	DESCRIPTION => 1,
	ANNOTATIONS => 2,
	COLUMNTYPE => 3,
	ISMASKED => 4,
	RELCONCEPT => 5,
	RELCOLUMN => 6
};

# The column name
sub name {
	return $_[0]->[NAME];
}

# The description, a DCC::Model::DescriptionSet instance
sub description {
	return $_[0]->[DESCRIPTION];
}

# Annotations, a DCC::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[ANNOTATIONS];
}

# It returns a DCC::Model::ColumnType instance
sub columnType {
	return $_[0]->[COLUMNTYPE];
}

# If this column is masked (because it is a inherited idref on a concept hosted in a hash)
# it will return true, otherwise undef
sub isMasked {
	return $_[0]->[ISMASKED];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a DCC::Model::Concept instance
# Otherwise, it will return undef
sub relatedConcept {
	return $_[0]->[RELCONCEPT];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a DCC::Model::Column instance
# which correlates to
# Otherwise, it will return undef
sub relatedColumn {
	return $_[0]->[RELCOLUMN];
}

# clone parameters:
#	doMask: optional, it signals whether to mark cloned column
#		as masked, so it should not be considered for value storage in the database.
# it returns a DCC::Model::Column instance
sub clone(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $doMask = shift;
	
	# Cloning this object
	my $retval = bless(\@{$self},ref($self));
	
	$retval->[ISMASKED] = ($doMask)?1:undef;
	
	return $retval;
}

# cloneRelated parameters:
#	relatedConcept: A DCC::Model::Concept instance, which this column is related to.
#		The kind of relation could be inheritance, or 1:N
#	prefix: The optional prefix to be set to the name when the column is cloned
# it returns a DCC::Model::Column instance
sub cloneRelated($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $relatedConcept = shift;
	my $prefix = shift;
	
	# Cloning this object
	my $retval = $self->clone();
	
	# Adding the prefix
	$retval->[NAME] = $prefix.$retval->[NAME]  if(defined($prefix) && length($prefix)>0);
	
	# And adding the relation info
	# to this column
	$retval->[RELCONCEPT] = $relatedConcept;
	$retval->[RELCOLUMN] = $self;
	
	return $retval;
}

1;


package DCC::Model::FilenamePattern;

# Name, regular expression, parts

# The symbolic name of this filename pattern
sub name {
	return $_[0]->[0];
}

# A Pattern object, representing the filename format
sub pattern {
	return $_[0]->[1];
}

# An array of post matching validations to be applied
# (mainly DCC::Model::CV validation and context constant catching)
sub postValidationParts {
	return $_[0]->[2];
}

# It returns a hash of DCC::Model::ConceptDomain instances
sub registeredConceptDomains {
	return $_[0]->[3];
}

# This method tries to match an input string against the filename-format pattern
#	filename: a string with a relative filename to match
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
		#Carp::croak("Matching filename $filename against pattern ".$self->pattern." did not work!");
		return undef;
	}
	
	my %rethash = ();
	my $ipart = -1;
	foreach my $part (@{$p_parts}) {
		$ipart++;
		next  unless(defined($part));
		
		# Is it a CV?
		if(ref($part) eq 'DCC::Model::CV') {
			Carp::croak('Validation against CV did not match')  unless($part->isValid($values[$ipart]));
			
			# Let's save the matched CV value
			$rethash{$part->name} = $values[$ipart]  if(defined($part->name));
		# Context constant
		} elsif(ref($part) eq '') {
			$rethash{$part} = $values[$ipart];
		}
	}
	
	return \%rethash;
}

# This method matches the concept related to this
sub matchConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $filename = shift;
	
	my $retConcept = undef;
	
	my $extractedValues = $self->match($filename);
	
	if(defined($extractedValues) && exists($extractedValues->{'$domain'}) && exists($extractedValues->{'$concept'})) {
		my $domainName = $extractedValues->{'$domain'};
		my $conceptName = $extractedValues->{'$concept'};
		if(exists($self->registeredConceptDomains->{$domainName})) {
			my $conceptDomain = $self->registeredConceptDomains->{$domainName};
			if(exists($conceptDomain->conceptHash->{$conceptName})) {
				$retConcept = $conceptDomain->conceptHash->{$conceptName};
			}
		}
	}
	
	return $retConcept;
}

# This method is called when new concept domains are being read,
# and they use this filename-format, so they can be found later
#	conceptDomain:	a DCC::Model::ConceptDomain instance, which uses this filename-pattern
# The method returns nothing
sub registerConceptDomain($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptDomain = shift;
	
	# The concept domain is registered, then
	$self->registeredConceptDomains->{$conceptDomain->name} = $conceptDomain;
}

1;


package DCC::Model::ConceptDomain;
# concept domain name
sub name {
	return $_[0]->[0];
}

# full name of the concept domain
sub fullname {
	return $_[0]->[1];
}

# Filename Pattern for the filenames
# A DCC::Model::FilenamePattern instance
sub filenamePattern {
	return $_[0]->[2];
}

# A hash with the concepts under this concept domain umbrella
# A hash of DCC::Model::Concept instances
sub concepts {
	return $_[0]->[3];
}

# A hash with the concepts under this concept domain umbrella
# A hash of DCC::Model::Concept instances
sub conceptHash {
	return $_[0]->[4];
}

1;


package DCC::Model::Concept;

# name
sub name {
	return $_[0]->[0];
}

# fullname
sub fullname {
	return $_[0]->[1];
}

# The DCC::Model::ConceptType instance basetype
sub baseConceptType {
	return $_[0]->[2];
}

# The DCC::Model::ConceptDomain instance where this concept is defined
sub conceptDomain {
	return $_[0]->[3];
}

# A DCC::Model::DescriptionSet instance, with all the descriptions
sub description {
	return $_[0]->[4];
}

# A DCC::Model::AnnotationSet instance, with all the annotations
sub annotations {
	return $_[0]->[5];
}

# A DCC::Model::ColumnSet instance with all the columns (including the inherited ones) of this concept
sub columnSet {
	return $_[0]->[6];
}

# related conceptNames, an array of trios concept domain name, concept name, prefix
sub relatedConceptNames {
	return $_[0]->[7];
}

# refColumns parameters:
#	prefix: The optional prefix to put on cloned idref columns
# It returns a DCC::Model::ColumnSet instance with clones of all the idref columns
# referring to this object and with a possible prefix.
sub refColumns(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $prefix = shift;
	
	return $self->columnSet->refColumns($self,$prefix);
}

1;