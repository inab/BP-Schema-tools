#!/usr/bin/perl -W

use strict;
use Carp;
use File::Basename;
use File::Spec;
use XML::LibXML;

package DCC::Model;

use constant DCCSchemaFilename => 'bp-schema.xsd';
use constant dccNamespace => 'http://www.blueprint-epigenome.eu/dcc/schema';
use constant ItemTypes => qw(
	string integer decimal boolean timestamp complex
);


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
	map { $self->{TYPES}{$_} = ($_ eq 'complex') ? 1 : undef; } ItemTypes;
	
	# No error, so, let's process it!!
	$self->digestModel($model);
	
	return $self;
}

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
			$collections{$collection[0]} = bless(\@collection,'DCC::Collection');
			
			# And the index declarations for this collection
			foreach my $ind ($coll->childNodes()) {
				next  unless($ind->nodeType == XML::LibXML::XML_ELEMENT_NODE && $ind->localname eq 'index');
				
				# Is index unique?, attributes (attribute name, ascending/descending)
				my @index = (($ind->hasAttribute('unique') && $ind->getAttribute('unique') eq 'true')?1:undef,[]);
				push(@{$collection[2]},bless(\@index,'DCC::Index'));
				
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
			$cv{$p_structCV->[0]}=$p_structCV  if(defined($p_structCV->[0]));
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
			next  unless($ctype->nodeType == XML::LibXML::XML_ELEMENT_NODE && $ctype->localname eq 'cv');
			
			my @conceptTypes = $self->parseConceptType($ctype);
			
			# Now, let's store the concrete (non-anonymous, abstract) concept types
			map { $conceptTypes{$_->[0]} = $_  if(defined($_->[0])); } @conceptTypes;
		}
		
		last;
	}
}

sub parseDescriptions($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $container = shift;
	
	my @descriptions = ();
	foreach my $description ($container->getChildrenByTagNameNS(dccNamespace,'description')) {
		push(@descriptions,$description->textContent());
	}
	
	return bless(\@descriptions,'DCC::DescriptionSet');
}

sub parseAnnotations($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $container = shift;
	
	my %annotations = ();
	foreach my $annotation ($container->getChildrenByTagNameNS(dccNamespace,'annotation')) {
		$annotations{$annotation->getAttribute('key')} = $annotation->textContent();
	}
	
	return bless(\%annotations,'DCC::AnnotationSet');
}

sub parseCVElement($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cv = shift;
	
	# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs and the CV
	my @structCV=(undef,undef,{},[],{});
	
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
								push(@{$structCV[3]},$value);
							} else {
								$structCV[2]{$key}=$value;
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
	
	return bless(\@structCV,'DCC::CV');
}

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

sub parseConceptType($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $ctypeElem = shift;
	# Optional parameter, the parent
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
	my $me = bless(\@ctype,'DCC::ConceptType');
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
	
	return bless(\@columnSet,'DCC::ColumnSet');
}

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
		\@columnType
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
		if(defined($self->{TYPES}{$itemType})) {
			if($colType->hasAttribute('complex-template') && $colType->hasAttribute('complex-seps')) {
				# tokens, separators
				my $seps = $colType->getAttribute('complex-seps');
				my @tokenNames = split(/[$seps]/,$colType->getAttribute('complex-template'));
				my @complexDecl = ($seps,\@tokenNames);
				$columnType[2] = bless(\@complexDecl,'DCC::ComplexType');
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
		$columnType[4] = $colType->hasAttribute('array-seps')?$colType->getAttribute('array-seps'):undef;
		
		last;
	}
	
	return bless(\@column,'DCC::Column');
}

1;