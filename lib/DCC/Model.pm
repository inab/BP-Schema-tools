#!/usr/bin/perl -W

use strict;

use Carp;
use File::Basename;
use File::Copy;
use File::Spec;
use IO::File;
use XML::LibXML;
use Digest::SHA1;
use URI;
use Archive::Zip;
use Archive::Zip::MemberRead;

# Early subpackage constant declarations
package DCC::Model::Column;

use constant {
	NAME => 0,
	DESCRIPTION => 1,
	ANNOTATIONS => 2,
	COLUMNTYPE => 3,
	ISMASKED => 4,
	REFCONCEPT => 5,
	REFCOLUMN => 6,
	RELATED_CONCEPT => 7
};


package DCC::Model::ColumnType;

use constant {
	TYPE	=>	0,
	USE	=>	1,
	RESTRICTION	=>	2,
	DEFAULT	=>	3,
	ARRAYSEPS	=>	4,
	ALLOWEDNULLS	=>	5
};

use constant {
	IDREF	=>	0,
	REQUIRED	=>	1,
	DESIRABLE	=>	-1,
	OPTIONAL	=>	-2
};

use constant STR2TYPE => {
	'idref' => IDREF,
	'required' => REQUIRED,
	'desirable' => DESIRABLE,
	'optional' => OPTIONAL
};

package DCC::Model::CV;

# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs, the CV (hash and keys), and the aliases (hash and keys)
use constant {
	CVNAME	=>	0,
	CVKIND	=>	1,
	CVURI	=>	2,
	CVANNOT	=>	3,
	CVDESC	=>	4,
	CVHASH	=>	5,
	CVKEYS	=>	6,
	CVALHASH	=>	7,
	CVALKEYS	=>	8,
	CVXMLEL		=>	9
};

use constant {
	INLINE	=>	'inline',
	NULLVALUES	=>	'null-values',
	CVFORMAT	=>	'cvformat',
	URIFETCHED	=>	'uris',
};

package DCC::Model::ConceptType;

# Prototypes
sub parseConceptTypeLineage($$;$);


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
# DCC::Model::CompoundType
# DCC::Model::ColumnType
# DCC::Model::Column
# DCC::Model::FilenamePattern
# DCC::Model::ConceptDomain
# DCC::Model::Concept

use constant DCCSchemaFilename => 'bp-schema.xsd';
use constant dccNamespace => 'http://www.blueprint-epigenome.eu/dcc/schema';
# The pattern matching the contents for this type, and whether it is not a numeric type or yes
use constant {
	TYPEPATTERN	=>	0,
	ISNOTNUMERIC	=>	1
};

use constant ItemTypes => {
	'string'	=> [1,1],	# With this we avoid costly checks
	'integer'	=> [qr/^0|(?:-?[1-9][0-9]*)$/,undef],
	'decimal'	=> [qr/^(?:0|(?:-?[1-9][0-9]*))(?:\.[0-9]+)?$/,undef],
	'boolean'	=> [qr/^[10]|[tT](?:rue)?|[fF](?:alse)?|[yY](?:es)?|[nN]o?$/,1],
	'timestamp'	=> [qr/^[1-9][0-9][0-9][0-9](?:(?:1[0-2])|(?:0[1-9]))(?:(?:[0-2][0-9])|(?:3[0-1]))$/,1],
	'duration'	=> [qr/^$/,1],
	'compound'	=> [undef,1]
};

use constant {
	BPMODEL_MODEL	=> 'bp-model.xml',
	BPMODEL_SCHEMA	=> 'bp-schema.xsd',
	BPMODEL_CV	=> 'cv',
	BPMODEL_SIG	=> 'signatures.txt',
	BPMODEL_SIG_KEYVAL_SEP	=> ': '
};

use constant Signatures => ['schemaSHA1','modelSHA1','cvSHA1'];

##############
# Prototypes #
##############

# 'new' is not included in the prototypes
sub digestModel($);

sub __parse_pattern($;$);

sub parsePatterns($);

#################
# Class methods #
#################

# The constructor takes as input the filename
sub new($;$) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	# my $self  = $class->SUPER::new();
	my $self = {};
	bless($self,$class);
	
	# Let's start processing the input model
	my $modelPath = shift;
	my $forceBPModel = shift;
	my $modelAbsPath = File::Spec->rel2abs($modelPath);
	my $modelDir = File::Basename::dirname($modelAbsPath);
	
	$self->{_modelAbsPath} = $modelAbsPath;
	$self->{_modelDir} = $modelDir;
	
	# First, let's try opening it as a bpmodel
	my $zipErrMsg = undef;
	Archive::Zip::setErrorHandler(sub { $zipErrMsg = $_[0]; });
	my $bpzip = Archive::Zip->new($modelAbsPath);
	# Was a bpmodel required?
	Archive::Zip::setErrorHandler(undef);
	if(defined($forceBPModel)  && !defined($bpzip)) {
		Carp::croak("Passed model is not in BPModel format: ".$zipErrMsg);
	}
	
	# Now, let's save what it is needed
	my $expectedModelSHA1 = undef;
	my $expectedSchemaSHA1 = undef;
	my $expectedCvSHA1 = undef;
	if(defined($bpzip)) {
		$self->{_BPZIP} = $bpzip;
		($expectedSchemaSHA1,$expectedModelSHA1,$expectedCvSHA1) = $self->readSignatures(@{(Signatures)});
	}
	
	# DCC model XML file parsing
	my $model = undef;
	my $modelSHA1 = undef;
	my $SHA = Digest::SHA1->new;
	my $CVSHA = Digest::SHA1->new;
	
	$self->{_SHA}=$SHA;
	$self->{_CVSHA}=$CVSHA;
	
	eval {
		my $X;
		# First, let's compute SHA1
		$X = $self->openModel();
		if(defined($X)) {
			$SHA->addfile($X);
			$modelSHA1 = $SHA->clone->hexdigest;
			
			Carp::croak("$modelPath is corrupted (wrong model SHA1) $modelSHA1 => $expectedModelSHA1")  if(defined($expectedModelSHA1) && $expectedModelSHA1 ne $modelSHA1);
		} else {
			Carp::croak("Unable to open model to compute its SHA1");
		}
		seek($X,0,0);
		$model = XML::LibXML->load_xml(IO => $X);
		# Temp filehandles are closed by File::Temp
		close($X)  unless($X->isa('File::Temp'));
	};
	
	# Was there some model parsing error?
	if($@) {
		Carp::croak("Error while parsing model $modelPath: ".$@);
	}
	
	$self->{_modelSHA1} = $modelSHA1;
	
	# Schema SHA1
	my $SCHpath = $self->librarySchemaPath();
	my $SCH = undef;
	if(open($SCH,'<:utf8',$SCHpath)) {
		my $SCSHA = Digest::SHA1->new;
		
		$SCSHA->addfile($SCH);
		close($SCH);
		
		my $schemaSHA1 = $SCSHA->hexdigest;
		Carp::croak("$modelPath is corrupted (wrong schema SHA1) $schemaSHA1 => $expectedSchemaSHA1")  if(defined($expectedSchemaSHA1) && $expectedSchemaSHA1 ne $schemaSHA1);
		$self->{_schemaSHA1} = $schemaSHA1;
	} else {
		Carp::croak("Unable to calculate bpmodel schema SHA1");
	}
	
	# Schema preparation
	my $dccschema = $self->schemaModel();
	
	# Model validated against the XML Schema
	eval {
		$dccschema->validate($model);
	};
	
	# Was there some schema validation error?
	if($@) {
		Carp::croak("Error while validating model $modelPath against the schema: ".$@);
	}
	
	# Setting the internal system item types
	%{$self->{TYPES}} = %{(ItemTypes)};
	
	# No error, so, let's process it!!
	$self->digestModel($model);
	
	# Now, we should have SHA1 of full model (model+CVs) and CVs only
	my $cvSHA1 = $self->{_CVSHA}->hexdigest;
	Carp::croak("$modelPath is corrupted (wrong CV SHA1) $cvSHA1 => $expectedCvSHA1")  if(defined($expectedCvSHA1) && $expectedCvSHA1 ne $cvSHA1);
		
	$self->{_cvSHA1} = $cvSHA1;
	$self->{_fullmodelSHA1} = $self->{_SHA}->hexdigest;
	delete($self->{_SHA});
	delete($self->{_CVSHA});
	
	return $self;
}

sub modelPath() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{_modelAbsPath};
}

# openModel parameters:
#	(none)
# It returns an open filehandle (either a File::Temp instance or a Perl GLOB (file handle))
sub openModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# Is it inside a Zip?
	if(exists($self->{_BPZIP})) {
		# Extracting to a temp file
		my $temp = File::Temp->new();
		my $member = $self->{_BPZIP}->memberNamed(BPMODEL_MODEL);
		
		Carp::croak("Model not found inside bpmodel ".$self->{_modelAbsPath})  unless(defined($member));
		
		Carp::croak("Error extracting model from bpmodel ".$self->{_modelAbsPath}) if($member->extractToFileHandle($temp)!=Archive::Zip::AZ_OK);
		
		# Assuring the file pointer is in the right place
		$temp->seek(0,0);
		
		return $temp;
	} else {
		my $X;
		
		open($X,'<:utf8',$self->{_modelAbsPath});
		
		return $X;
	}
}

sub librarySchemaPath() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	unless(exists($self->{_schemaPath})) {
		if(exists($self->{_BPZIP})) {
			my $memberSchema = $self->{_BPZIP}->memberNamed(BPMODEL_SCHEMA);
			Carp::croak("Unable to find embedded bpmodel schema in bpmodel")  unless(defined($memberSchema));
			my $tempSchema = File::Temp->new();
			Carp::croak("Unable to save embedded bpmodel schema")  if($memberSchema->extractToFileNamed($tempSchema->filename())!=Archive::Zip::AZ_OK);
			
			$self->{_schemaPath} = $tempSchema;
		} else {
			my $schemaDir = File::Basename::dirname(__FILE__);
			
			$self->{_schemaPath} = File::Spec->catfile($schemaDir,DCCSchemaFilename);
		}
	}
	
	return $self->{_schemaPath};
}

# schemaModel parameters:
#	(none)
# returns a XML::LibXML::Schema instance
sub schemaModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# Schema preparation
	my $dccschema = XML::LibXML::Schema->new(location => $self->librarySchemaPath());
	
	return $dccschema;
}

# reformatModel parameters:
#	(none)
# The method applies the needed changes on DOM representation of the model
# in order to be valid once it is stored in bpmodel format
sub reformatModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# Used later
	my $modelDoc = $self->{modelDoc};

	# change CV root
	$self->{_cvDecl}->setAttribute('dir',BPMODEL_CV);
	
	# This hash is used to avoid collisions
	my %newPaths = ();
	# Change CV paths to relative ones
	foreach my $CVfile (@{$self->{CVfiles}}) {
		my $cv = $CVfile->xmlElement();
		
		my $origPath = $CVfile->filename();
		
		my(undef,undef,$newPath) = File::Spec->splitpath($origPath);
		
		# Fixing collisions
		if(exists($newPaths{$newPath})) {
			my $newNewPath = $newPaths{$newPath}[1].'-'.$newPaths{$newPath}[0].$newPaths{$newPath}[2];
			$newPaths{$newPath}[0]++;
			$newPath = $newNewPath;
		} else {
			my $extsep = rindex($newPath,'.');
			my $newPathName = ($extsep!=-1)?substr($newPath,0,$extsep):$newPath;
			my $newPathExt = ($extsep!=-1)?substr($newPath,$extsep):'';
			$newPaths{$newPath} = [1,$newPathName,$newPathExt];
		}
		
		# As the path is a text node
		# first we remove all the text nodes
		# and we append a new one with the new path
		$cv->removeChildNodes();
		$cv->appendChild($modelDoc->createTextNode($newPath));
	}
	
	# Create a temp file and save the model to it
	my $tempModelFile = File::Temp->new();
	$modelDoc->toFile($tempModelFile->filename(),2);
	
	# Calculate new SHA1, and store it
	my $SHA = Digest::SHA1->new();
	$SHA->addfile($tempModelFile);
	$self->{_modelSHA1} = $SHA->hexdigest();
	
	# Return Temp::File instance
	seek($tempModelFile,0,0);
	return $tempModelFile;
}

sub readSignatures(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	Carp::croak("Signatures can only be read from a bpmodel")  unless(exists($self->{_BPZIP}));
	
	my(@keys) = @_;

	my($signatures,$status) = $self->{_BPZIP}->contents(BPMODEL_SIG);
	
	Carp::croak("Signatures not found/not read in bpmodel")  if($status!=Archive::Zip::AZ_OK);
	my %signatures = ();
	foreach my $sigline (split("\n",$signatures)) {
		my($key,$val) = split(BPMODEL_SIG_KEYVAL_SEP,$sigline,2);
		$signatures{$key} = $val;
	}
	
	return @signatures{@keys};
}

sub __keys2string(@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my(@keys) = @_;
	
	return join("\n",map { $_.BPMODEL_SIG_KEYVAL_SEP.$self->{'_'.$_} } @keys);
}

# saveBPModel parameters:
#	filename: The file where the model (in bpmodel format) is going to be saved
# TODO/TOFINISH
sub saveBPModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $filename = shift;
	
	my $retval = undef;
	if(exists($self->{_BPZIP})) {
		# A simple copy does the work
		$retval = ::copy($self->modelPath(),$filename);
	} else {
		# Let's reformat the model in order to be stored as a bpmodel format
		my $tempModelFile = $self->reformatModel();
		
		# Let's create the output
		my $bpModel = Archive::Zip->new();
		$bpModel->zipfileComment("Created by ".ref($self).' $Rev$');
		
		# First, add the file which contains the SHA1 sums about all the saved contents
		$bpModel->addString(join("\n",$self->__keys2string(@{(Signatures)})),BPMODEL_SIG);

		# Let's store the model (which could have changed since it was loaded)
		my $modelMember = $bpModel->addFile($tempModelFile->filename(),BPMODEL_MODEL,Archive::Zip::COMPRESSION_LEVEL_BEST_COMPRESSION);
		# Setting up the right timestamp
		my @modelstats = stat($self->modelPath());
		$modelMember->setLastModFileDateTimeFromUnix($modelstats[9]);
		
		# Next, the used schema
		$bpModel->addFile($self->librarySchemaPath(),BPMODEL_SCHEMA,Archive::Zip::COMPRESSION_LEVEL_BEST_COMPRESSION);
		
		# Add the controlled vocabulary
		my $cvprefix = BPMODEL_CV.'/';
		my $cvprefixLength = length($cvprefix);
		
		# First, the CV directory declaration
		my $cvmember = $bpModel->addDirectory($cvprefix);
		
		# And now, the CV members
		foreach my $CVfile (@{$self->{CVfiles}}) {
			$bpModel->addFile($CVfile->filename(),$cvprefix.$CVfile->xmlElement()->textContent(),Archive::Zip::COMPRESSION_LEVEL_BEST_COMPRESSION);
		}
		
		# Let's save it
		$retval = $bpModel->overwriteAs($filename) == Archive::Zip::AZ_OK;
	}
	
	return $retval;
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
	
	my $model = $self;
	
	$self->{modelDoc} = $modelDoc;
	
	# First, let's store the key values, project name and schema version
	$self->{project} = $modelRoot->getAttribute('project');
	$self->{schemaVer} = $modelRoot->getAttribute('schemaVer');
	
	# The documentation directory, which complements this model
	my $docsDir = $modelRoot->getAttribute('docsDir');
	# We need to translate relative paths to absolute ones
	$docsDir = File::Spec->rel2abs($docsDir,$self->{_modelDir})  unless(File::Spec->file_name_is_absolute($docsDir));
	$self->{_docsDir} = $docsDir;
	
	# Now, let's store the annotations
	$self->{ANNOTATIONS} = DCC::Model::AnnotationSet->parseAnnotations($modelRoot);
	
	# Now, the collection domain
	my $p_collections = undef;
	foreach my $colDom ($modelRoot->getChildrenByTagNameNS(DCC::Model::dccNamespace,'collection-domain')) {
		$p_collections = $self->{COLLECTIONS} = DCC::Model::Collection::parseCollections($colDom);
		last;
	}
	
	# Next stop, controlled vocabulary
	my %cv = ();
	my @cvArray = ();
	$self->{CV} = \%cv;
	$self->{CVARRAY} = \@cvArray;
	foreach my $cvDecl ($modelRoot->getChildrenByTagNameNS(DCC::Model::dccNamespace,'cv-declarations')) {
		$self->{_cvDecl} = $cvDecl;
		# Let's setup the path to disk stored CVs
		my $cvdir = undef;
		$cvdir = $cvDecl->getAttribute('dir')  if($cvDecl->hasAttribute('dir'));
		if(defined($cvdir)) {
			# We need to translate relative paths to absolute ones
			# but only when the model is not yet a bpmodel
			$cvdir = File::Spec->rel2abs($cvdir,$self->{_modelDir})  unless(exists($self->{_BPZIP}) || File::Spec->file_name_is_absolute($cvdir));
		} else {
			$cvdir = $self->{_modelDir};
		}
		
		$self->{_cvDir} = $cvdir;
		
		my $destCVCol = $cvDecl->getAttribute('collection');
		if(exists($p_collections->{$destCVCol})) {
			$self->{_cvColl} = $p_collections->{$destCVCol};
		} else {
			Carp::croak("Destination collection $destCVCol for CV has not been declared");
		}
		foreach my $cv ($cvDecl->childNodes()) {
			next  unless($cv->nodeType == XML::LibXML::XML_ELEMENT_NODE && $cv->localname eq 'cv');
			
			my $p_structCV = DCC::Model::CV->parseCV($cv,$model);
			
			# Let's store named CVs here, not anonymous ones
			if(defined($p_structCV->name)) {
				$cv{$p_structCV->name}=$p_structCV;
				push(@cvArray,$p_structCV);
			}
		}
		
		last;
	}
	
	foreach my $nullDecl ($modelRoot->getChildrenByTagNameNS(DCC::Model::dccNamespace,'null-values')) {
		my $p_nullCV = DCC::Model::CV->parseCV($nullDecl,$model);
		
		# Let's store the controlled vocabulary for nulls
		# as a DCC::Model::CV
		$self->{NULLCV} = $p_nullCV;
	}
	
	# A safeguard for this parameter
	$self->{_cvDir} = $self->{_modelDir}  unless(exists($self->{_cvDir}));
	
	# Now, the pattern declarations
	foreach my $patternDecl ($modelRoot->getChildrenByTagNameNS(DCC::Model::dccNamespace,'pattern-declarations')) {
		$self->{PATTERNS} = DCC::Model::parsePatterns($patternDecl);		
		last;
	}
	
	# And we start with the concept types
	my %conceptTypes = ();
	$self->{CTYPES} = \%conceptTypes;
	foreach my $conceptTypesDecl ($modelRoot->getChildrenByTagNameNS(DCC::Model::dccNamespace,'concept-types')) {
		foreach my $ctype ($conceptTypesDecl->childNodes()) {
			next  unless($ctype->nodeType == XML::LibXML::XML_ELEMENT_NODE && $ctype->localname eq 'concept-type');
			
			my @conceptTypeLineage = DCC::Model::ConceptType::parseConceptTypeLineage($ctype,$model);
			
			# Now, let's store the concrete (non-anonymous, abstract) concept types
			map { $conceptTypes{$_->name} = $_  if(defined($_->name)); } @conceptTypeLineage;
		}
		
		last;
	}
	
	# The different filename formats
	my %filenameFormats = ();
	$self->{FPATTERN} = \%filenameFormats;
	
	foreach my $filenameFormatDecl ($modelRoot->getChildrenByTagNameNS(DCC::Model::dccNamespace,'filename-format')) {
		my $filenameFormat = DCC::Model::FilenamePattern->parseFilenameFormat($filenameFormatDecl,$model);
		
		$filenameFormats{$filenameFormat->name} = $filenameFormat;
	}
	
	# Oh, no! The concept domains!
	my @conceptDomains = ();
	my %conceptDomainHash = ();
	$self->{CDOMAINS} = \@conceptDomains;
	$self->{CDOMAINHASH} = \%conceptDomainHash;
	foreach my $conceptDomainDecl ($modelRoot->getChildrenByTagNameNS(DCC::Model::dccNamespace,'concept-domain')) {
		my $conceptDomain = $self->parseConceptDomain($conceptDomainDecl);
		
		push(@conceptDomains,$conceptDomain);
		$conceptDomainHash{$conceptDomain->name} = $conceptDomain;
	}
	
	# But the work it is not finished, because
	# we have to propagate the foreign keys
	foreach my $conceptDomain (@conceptDomains) {
		foreach my $concept (@{$conceptDomain->concepts}) {
			foreach my $relatedConcept (@{$concept->relatedConcepts}) {
				my $domainName = $relatedConcept->conceptDomainName;
				my $conceptName = $relatedConcept->conceptName;
				my $prefix = $relatedConcept->keyPrefix;
				
				my $refDomain = $conceptDomain;
				if(defined($domainName)) {
					unless(exists($conceptDomainHash{$domainName})) {
						Carp::croak("Concept domain $domainName referred from concept ".$conceptDomain->name.'.'.$concept->name." does not exist");
					}
					
					$refDomain = $conceptDomainHash{$domainName};
				}
				
				if(exists($refDomain->conceptHash->{$conceptName})) {
					# And now, let's propagate!
					my $refConcept = $refDomain->conceptHash->{$conceptName};
					my $refColumnSet = $refConcept->refColumns($relatedConcept);
					$concept->columnSet->addColumns($refColumnSet);
					$relatedConcept->setRelatedConcept($refConcept,$refColumnSet);
				} else {
					Carp::croak("Concept $domainName.$conceptName referred from concept ".$conceptDomain->name.'.'.$concept->name." does not exist");
				}
				
			}
		}
	}
	
	# That's all folks, friends!
}

# sanitizeCVpath parameters:
#	cvPath: a CV path from the model to be sanizited
# returns a sanizited path, according to the model
sub sanitizeCVpath($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvPath = shift;
	
	if(exists($self->{_BPZIP})) {
		# It must be self contained
		$cvPath = $self->{_cvDir}.'/'.$cvPath;
	} else {
		$cvPath  = File::Spec->rel2abs($cvPath,$self->{_cvDir})  unless(File::Spec->file_name_is_absolute($cvPath));
	}
	
	return $cvPath;
}

# openCVpath parameters:
#	cvPath: a sanizited CV path from the model
# returns a IO::Handle like instance, corresponding to the CV
sub openCVpath($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvPath = shift;
	
	my $CV;
	if(exists($self->{_BPZIP})) {
		my $cvMember = $self->{_BPZIP}->memberNamed($cvPath);
		if(defined($cvMember)) {
			$CV = $cvMember->readFileHandle();
		} else {
			Carp::croak("Unable to open CV member $cvPath");
		}
	} elsif(!open($CV,'<:utf8',$cvPath)) {
		Carp::croak("Unable to open CV file $cvPath");
	}
	
	return $CV;
}

# digestCVline parameters:
#	cvline: a line read from a controlled vocabulary
# The SHA1 digests are updated
sub digestCVline($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvline = shift;
	$self->{_SHA}->add($cvline);
	$self->{_CVSHA}->add($cvline);
}

# registerCVfile parameters:
#	cv: a XML::LibXML::Element 'dcc:cv' node
# returns a DCC::Model::CV array reference, with all the controlled vocabulary
# stored inside.
# If the CV is in an external file, this method reads it, and registers it to save it later.
# If the CV is in an external URI, this method only checks whether it is available (TBD)
sub registerCV($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $CV = shift;
	
	# This key is internally used to register all the file CVs
	$self->{CVfiles} = []  unless(exists($self->{CVfiles}));
	
	push(@{$self->{CVfiles}},$CV)  if($CV->kind eq DCC::Model::CV::CVFORMAT);
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

# parsePatterns parameters:
#	patternDecl: a XML::LibXML::Element 'dcc:pattern-declarations' node
# it returns a reference to a hash containing all the parsed patterns
sub parsePatterns($) {
	my $patternDecl = shift;
	
	my %PATTERN = ();
	
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
	
	return \%PATTERN;
}

sub getCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $collName = shift;
	
	return exists($self->{COLLECTIONS}{$collName})?$self->{COLLECTIONS}{$collName}:undef;
}

sub getItemType($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $itemType = shift;
	
	return exists($self->{TYPES}{$itemType})?$self->{TYPES}{$itemType}:undef;
}

sub isValidNull($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $val = shift;
	
	return $self->{NULLCV}->isValid($val);
}

sub getNamedCV($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $name = shift;
	
	return exists($self->{CV}{$name})?$self->{CV}{$name}:undef;
}

sub getNamedPattern($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $name = shift;
	
	return exists($self->{PATTERNS}{$name})?$self->{PATTERNS}{$name}:undef;
}

sub getConceptType($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $name = shift;
	
	return exists($self->{CTYPES}{$name})?$self->{CTYPES}{$name}:undef;
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
#		or 'dcc:weak-concepts' instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, where this concept
#		has been defined.
#	idConcept: An optional, identifying DCC::Model::Concept instance of
#		all the (weak) concepts to be parsed from the container
# it returns an array of DCC::Model::Concept instances, which are all the
# concepts and weak concepts inside the input concept container
sub parseConceptContainer($$;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptContainerDecl = shift;
	my $conceptDomain = shift;
	my $idConcept = shift;	# This is optional (remember!)
	
	my @concepts = ();
	foreach my $conceptDecl ($conceptContainerDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'concept')) {
		push(@concepts,$self->parseConcept($conceptDecl,$conceptDomain,$idConcept));
	}
	
	return @concepts;
}

# parseConcept paramereters:
#	conceptDecl: A XML::LibXML::Element 'dcc:concept' instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, where this concept
#		has been defined.
#	idConcept: An optional, identifying DCC::Model::Concept instance of
#		the concept to be parsed from conceptDecl
# it returns an array of DCC::Model::Concept instances, the first one
# corresponds to this concept, and the other ones are the weak-concepts
sub parseConcept($$;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptDecl = shift;
	my $conceptDomain = shift;
	my $idConcept = shift;	# This is optional (remember!)
	my $model = $self;

	my $conceptName = $conceptDecl->getAttribute('name');
	my $conceptFullname = $conceptDecl->getAttribute('fullname');
	
	my @baseConceptTypes = $conceptDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'base-concept-type');
	
	Carp::croak("Concept $conceptFullname ($conceptName) has no base type (no dcc:base-concept-type)!")  if(scalar(@baseConceptTypes)==0);
	my $basetypeName = undef;
	my $basetype = undef;
	foreach my $baseConceptType (@baseConceptTypes) {
		$basetypeName = $baseConceptType->getAttribute('name');
		$basetype = $model->getConceptType($basetypeName);
		Carp::croak("Concept $conceptFullname ($conceptName) is based on undefined base type $basetypeName")  unless(defined($basetype));
		last;
	}
	
	
	# This array will contain the names of the related concepts
	my @related = ();
	
	# We don't have to inherit the related concepts, because weak concepts
	# are not subclasses!!!!!
	#########push(@related,@{$idConcept->relatedConcepts})  if(defined($idConcept));
	
	# Preparing the columns
	my $columnSet = DCC::Model::ColumnSet->parseColumnSet($conceptDecl,$basetype->columnSet,$model);
	# and adding the ones from the identifying concept
	# (and later from the related stuff)
	
	$columnSet->addColumns($idConcept->idColumns(! $idConcept->baseConceptType->isCollection),1)  if(defined($idConcept));
	
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
		DCC::Model::DescriptionSet->parseDescriptions($conceptDecl),
		DCC::Model::AnnotationSet->parseAnnotations($conceptDecl),
		$columnSet,
		$idConcept,
		\@related,
	);
	
	# Saving the related concepts (the ones explicitly declared within this concept)
	foreach my $relatedDecl ($conceptDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'related-to')) {
		push(@related,bless([
				($relatedDecl->hasAttribute('domain'))?$relatedDecl->getAttribute('domain'):undef ,
				$relatedDecl->getAttribute('concept') ,
				($relatedDecl->hasAttribute('prefix'))?$relatedDecl->getAttribute('prefix'):undef ,
				undef,
				undef,
				($relatedDecl->hasAttribute('arity') && $relatedDecl->getAttribute('arity') eq 'M')?'M':1,
				($relatedDecl->hasAttribute('m-ary-sep'))?$relatedDecl->getAttribute('m-ary-sep'):',',
				($relatedDecl->hasAttribute('partial-participation') && $relatedDecl->hasAttribute('partial-participation') eq 1)?1:undef
			],'DCC::Model::RelatedConcept')
		);
	}
	
	# And last, the weak concepts
	my $concept =  bless(\@thisConcept,'DCC::Model::Concept');
	
	my @concepts = ($concept);
	
	foreach my $conceptContainerDecl ($conceptDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'weak-concepts')) {
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

# It returns a model version
sub schemaVer() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{schemaVer};
}

# It returns the SHA1 digest of the model
sub modelSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{_modelSHA1};
}

sub fullmodelSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{_fullmodelSHA1};
}

sub CVSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{_cvSHA1};
}

sub schemaSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{_schemaSHA1};
}

# It returns schemaVer
sub versionString() {
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

# A reference to a DCC::Model::CV instance, for the valid null values for this model
sub nullCV() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{NULLCV};
}

sub types() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{TYPES};
}

1;

# And now, the helpers for the different pseudo-packages

package DCC::Model::Collection;

sub parseCollections($) {
	my $colDom = shift;
	
	my %collections = ();
	foreach my $coll ($colDom->childNodes()) {
		next  unless($coll->nodeType == XML::LibXML::XML_ELEMENT_NODE && $coll->localname eq 'collection');
		
		my $collection = DCC::Model::Collection->parseCollection($coll);
		$collections{$collection->name} = $collection;
	}
	
	return \%collections;
}

# This is the constructor
sub parseCollection($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $coll = shift;
	
	# Collection name, collection path, index declarations
	my @collection = ($coll->getAttribute('name'),$coll->getAttribute('path'),DCC::Model::Index::parseIndexes($coll));
	return bless(\@collection,$class);
}

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

# This is an static method.
# parseIndexes parameters:
#	container: a XML::LibXML::Element container of 'dcc:index' elements
# returns an array reference, containing DCC::Model::Index instances
sub parseIndexes($) {
	my $container = shift;
	
	# And the index declarations for this collection
	my @indexes = ();
	foreach my $ind ($container->getChildrenByTagNameNS(DCC::Model::dccNamespace,'index')) {
		push(@indexes,DCC::Model::Index->parseIndex($ind));
	}
	
	return \@indexes;
}

# This is the constructor.
# parseIndex parameters:
#	ind: a XML::LibXML::Element which is a 'dcc:index'
# returns a DCC::Model::Index instance
sub parseIndex($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $ind = shift;
	
	# Is index unique?, attributes (attribute name, ascending/descending)
	my @index = (($ind->hasAttribute('unique') && $ind->getAttribute('unique') eq 1)?1:undef,[]);

	foreach my $attr ($ind->childNodes()) {
		next  unless($attr->nodeType == XML::LibXML::XML_ELEMENT_NODE && $attr->localname eq 'attr');
		
		push(@{$index[1]},[$attr->getAttribute('name'),($attr->hasAttribute('ord') && $attr->getAttribute('ord') eq '-1')?-1:1]);
	}

	return bless(\@index,$class);
}

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

# This is the constructor.
# parseDescriptions parameters:
#	container: a XML::LibXML::Element container of 'dcc:description' elements
# returns a DCC::Model::DescriptionSet array reference, containing the contents of all
# the 'dcc:description' XML elements found
sub parseDescriptions($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $container = shift;
	
	my @descriptions = ();
	foreach my $description ($container->getChildrenByTagNameNS(DCC::Model::dccNamespace,'description')) {
		my @dChildren = $description->nonBlankChildNodes();
		
		my $value = undef;
		# We only save the nodeset when 
		foreach my $dChild (@dChildren) {
			next  unless($dChild->nodeType == XML::LibXML::XML_ELEMENT_NODE);
			
			$value = \@dChildren;
			last;
		}
		
		push(@descriptions,defined($value)?$value:$description->textContent());
	}
	
	return bless(\@descriptions,$class);
}

# This method adds a new annotation to the annotation set
sub addDescription($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	my $desc = shift;

	push(@{$self},$desc);
}

1;


package DCC::Model::AnnotationSet;

# This is the constructor.
# parseAnnotations paremeters:
#	container: a XML::LibXML::Element container of 'dcc:annotation' elements
# It returns a DCC::Model::AnnotationSet hash reference, containing the contents of all the
# 'dcc:annotation' XML elements found
sub parseAnnotations($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $container = shift;
	
	my %annotationHash = ();
	my @annotationOrder = ();
	my @annotations = (\%annotationHash,\@annotationOrder);
	foreach my $annotation ($container->getChildrenByTagNameNS(DCC::Model::dccNamespace,'annotation')) {
		unless(exists($annotationHash{$annotation->getAttribute('key')})) {
			push(@annotationOrder,$annotation->getAttribute('key'));
		}
		my @aChildren = $annotation->nonBlankChildNodes();
		
		my $value = undef;
		foreach my $aChild (@aChildren) {
			next  unless($aChild->nodeType == XML::LibXML::XML_ELEMENT_NODE);
			if($aChild->namespaceURI() eq DCC::Model::dccNamespace) {
				$value = $aChild;
			} else {
				$value = \@aChildren;
			}
			last;
		}
		$annotationHash{$annotation->getAttribute('key')} = defined($value)?$value:$annotation->textContent();
	}
	
	return bless(\@annotations,$class);
}

sub hash {
	return $_[0]->[0];
}

# The order of the keys (when they are given in a description)
sub order {
	return $_[0]->[1];
}

# This method adds a new annotation to the annotation set
sub addAnnotation($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	my $key = shift;
	my $value = shift;
	
	my $hash = $self->hash;
	push(@{$self->order},$key)  unless(exists($hash->{$key}));
	$hash->{$key} = $value;
}

1;


package DCC::Model::CV::External;

# It returns a URI object, pointing to the fetchable controlled vocabulary
sub uri {
	return $_[0]->[0];
}

# It describes the format, so validators know how to handle it
sub format {
	return $_[0]->[1];
}

# It returns a URI object, pointing to the documentation about the CV
sub docURI {
	return $_[0]->[2];
}

1;


package DCC::Model::CV;

# This is the constructor
# parseCV parameters:
#	cv: a XML::LibXML::Element 'dcc:cv' node
#	model: a DCC::Model instance
# returns a DCC::Model::CV array reference, with all the controlled vocabulary
# stored inside.
# If the CV is in an external file, this method reads it, and calls the model to
# sanitize the paths and to digest the read lines.
# If the CV is in an external URI, this method only checks whether it is available (TBD)
sub parseCV($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $cv = shift;
	my $model = shift;
	
	# The CV symbolic name, the CV type, the CV filename, the annotations, the documentation paragraphs, the CV (hash and array), aliases (hash and array), XML element of cv-file element
	my $cvAnnot = DCC::Model::AnnotationSet->parseAnnotations($cv);
	my $cvDesc = DCC::Model::DescriptionSet->parseDescriptions($cv);
	my @structCV=(undef,undef,undef,$cvAnnot,$cvDesc,undef,undef,{},[],undef);
	my $createdCV = bless(\@structCV,$class);
	
	$structCV[DCC::Model::CV::CVNAME] = $cv->getAttribute('name')  if($cv->hasAttribute('name'));
	
	my %cvHash = ();
	my @cvKeys = ();
	foreach my $el ($cv->childNodes()) {
		next  unless($el->nodeType == XML::LibXML::XML_ELEMENT_NODE);
		
		if($el->localname eq 'e') {
			unless(defined($structCV[DCC::Model::CV::CVHASH])) {
				$structCV[DCC::Model::CV::CVHASH] = \%cvHash;
				$structCV[DCC::Model::CV::CVKEYS] = \@cvKeys;
				$structCV[DCC::Model::CV::CVKIND] = ($cv->localname eq DCC::Model::CV::NULLVALUES) ? DCC::Model::CV::NULLVALUES : DCC::Model::CV::INLINE;
			}
			my $key = $el->getAttribute('v');
			if(exists($cvHash{$key})) {
				Carp::croak('Repeated key '.$key.' on inline controlled vocabulary'.(defined($structCV[DCC::Model::CV::CVNAME])?(' '.$structCV[DCC::Model::CV::CVNAME]):''));
			}
			$cvHash{$key} = $el->textContent();
			push(@cvKeys,$key);
		} elsif($el->localname eq 'cv-uri') {
			unless(defined($structCV[DCC::Model::CV::CVKIND])) {
				$structCV[DCC::Model::CV::CVKIND] = DCC::Model::CV::URIFETCHED;
				$structCV[DCC::Model::CV::CVURI] = [];
			}
			
			# Although it is not going to be materialized here (at least, not yet)
			# let's check whether it is a valid cv-uri
			my $cvURI = $el->textContent();
			
			# TODO: validate URI
			my @externalCV = (URI->new($cvURI),$el->getAttribute('format'),$el->hasAttribute('doc')?URI->new($el->getAttribute('doc')):undef);
			push(@{$structCV[DCC::Model::CV::CVURI]},bless(\@externalCV,'DCC::Model::CV::External'));
			
			# As we are not fetching the content, we are not initializing neither cvHash nor cvKeys references
		} elsif($el->localname eq 'cv-file') {
			my $cvPath = $el->textContent();
			$cvPath = $model->sanitizeCVpath($cvPath);
			
			$structCV[DCC::Model::CV::CVKIND] = DCC::Model::CV::CVFORMAT;
			$structCV[DCC::Model::CV::CVURI] = $cvPath;
			$structCV[DCC::Model::CV::CVHASH] = \%cvHash;
			$structCV[DCC::Model::CV::CVKEYS] = \@cvKeys;
			# Saving it for a possible storage in a bpmodel
			$structCV[DCC::Model::CV::CVXMLEL] = $el;
			
			my $CV = $model->openCVpath($cvPath);
			while(my $cvline=$CV->getline()) {
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
							$cvDesc->addDescription($value);
						} else {
							$cvAnnot->addAnnotation($key,$value);
						}
					} else {
						next;
					}
				} else {
					my($key,$value) = split(/\t/,$cvline,2);
					if(exists($cvHash{$key})) {
						Carp::croak('Repeated key '.$key.' on controlled vocabulary from '.$cvPath.(defined($structCV[DCC::Model::CV::CVNAME])?(' '.$structCV[DCC::Model::CV::CVNAME]):''));
					}
					$cvHash{$key}=$value;
					push(@cvKeys,$key);
				}
			}
			
			$CV->close();
			$model->registerCV($createdCV);
		} elsif($el->localname eq 'term-alias') {
			my $alias = $class->parseCV($el,$model);
			my $key = $alias->name;
			if(exists($structCV[DCC::Model::CV::CVALHASH]->{$key})) {
				Carp::croak('Repeated term alias '.$key.' on controlled vocabulary'.(defined($structCV[DCC::Model::CV::CVNAME])?(' '.$structCV[DCC::Model::CV::CVNAME]):''));
			}
			
			$structCV[DCC::Model::CV::CVALHASH]->{$key} = $alias;
			push(@{$structCV[DCC::Model::CV::CVALKEYS]},$key);
		}
	}
	
	return $createdCV;
}

# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs and the CV
# my @structCV=(undef,undef,{},[],{});

# The name of this CV (optional)
sub name {
	return $_[0]->[DCC::Model::CV::CVNAME];
}

# The kind of CV
sub kind {
	return $_[0]->[DCC::Model::CV::CVKIND];
}

# Filename or URI holding these values (optional)
sub filename {
	return $_[0]->[DCC::Model::CV::CVURI];
}

# An instance of a DCC::Model::AnnotationSet, holding the annotations
# for this CV
sub annotations {
	return $_[0]->[DCC::Model::CV::CVANNOT];
}

# An instance of a DCC::Model::DescriptionSet, holding the documentation
# for this CV
sub description {
	return $_[0]->[DCC::Model::CV::CVDESC];
}

# The hash holding the CV in memory
sub CV {
	return $_[0]->[DCC::Model::CV::CVHASH];
}

# The order of the CV values (as in the file)
sub order {
	return $_[0]->[DCC::Model::CV::CVKEYS];
}

# The hash holding the aliases (DCC::Model::CV instances) in memory
sub alias {
	return $_[0]->[DCC::Model::CV::CVALHASH];
}

# The order of the alias values (as in the file)
sub aliasOrder {
	return $_[0]->[DCC::Model::CV::CVALKEYS];
}

# The original XML::LibXML::Element instance where
# the path to a CV file was read from
sub xmlElement {
	return $_[0]->[DCC::Model::CV::CVXMLEL];
}

# With this method we check the locality of the CV
sub isLocal() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvkey = shift;
	
	return defined($self->CV);
}

# With this method a key is validated
# TODO: fetch on demand the CV is it is not materialized
sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvkey = shift;
	
	return exists($self->CV->{$cvkey});
}

1;


package DCC::Model::ConceptType;

# This is an static method.
# parseConceptTypeLineage parameters:
#	ctypeElem: a XML::LibXML::Element 'dcc:concept-type' node
#	model: a DCC::Model instance where the concept type was defined
#	ctypeParent: an optional 'DCC::Model::ConceptType' instance, which is the
#		parent concept type
# returns an array of 'DCC::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseConceptTypeLineage($$;$) {
	my $ctypeElem = shift;
	my $model = shift;
	# Optional parameter, the conceptType parent
	my $ctypeParent = undef;
	$ctypeParent = shift  if(scalar(@_) > 0);
	
	# The returning values array
	my $me = DCC::Model::ConceptType->parseConceptType($ctypeElem,$model,$ctypeParent);
	my @retval = ($me);
	
	# Now, let's find subtypes
	foreach my $subtypes ($ctypeElem->getChildrenByTagNameNS(DCC::Model::dccNamespace,'subtypes')) {
		foreach my $childCTypeElem ($subtypes->childNodes()) {
			next  unless($childCTypeElem->nodeType == XML::LibXML::XML_ELEMENT_NODE && $childCTypeElem->localname() eq 'concept-type');
			
			# Parse subtypes and store them!
			push(@retval,DCC::Model::ConceptType::parseConceptTypeLineage($childCTypeElem,$model,$me));
		}
		last;
	}
	
	return @retval;
}

# This is the constructor.
# parseConceptType parameters:
#	ctypeElem: a XML::LibXML::Element 'dcc:concept-type' node
#	model: a DCC::Model instance where the concept type was defined
#	ctypeParent: an optional 'DCC::Model::ConceptType' instance, which is the
#		parent concept type
# returns an array of 'DCC::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseConceptType($$;$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $ctypeElem = shift;
	my $model = shift;
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
			# Let's link the DCC::Model::Collection
			my $collection = $model->getCollection($collName);
			if(defined($collection)) {
				$ctype[2] = $collection;
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
	
	# Let's parse the columns
	$ctype[4] = DCC::Model::ColumnSet->parseColumnSet($ctypeElem,defined($ctypeParent)?$ctypeParent->columnSet:undef,$model);
	
	# And the index declarations
	$ctype[5] = DCC::Model::Index::parseIndexes($ctypeElem);
	# inheriting the ones from the parent concept types
	push(@{$ctype[5]},@{$ctypeParent->indexes})  if(defined($ctypeParent));
	
	# The returning values array
	return bless(\@ctype,$class);
}

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

# This is the constructor.
# parseColumnSet parameters:
#	container: a XML::LibXML::Element node, containing 'dcc:column' elements
#	parentColumnSet: a DCC::Model::ColumnSet instance, which is the parent.
#	model: a DCC::Model instance, used to validate the columns.
# returns a DCC::Model::ColumnSet instance with all the DCC::Model::Column instances (including
# the inherited ones from the parent).
sub parseColumnSet($$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $container = shift;
	my $parentColumnSet = shift;
	my $model = shift;
	
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
	
	my @checkDefault = ();
	foreach my $colDecl ($container->childNodes()) {
		next  unless($colDecl->nodeType == XML::LibXML::XML_ELEMENT_NODE && $colDecl->localname() eq 'column');
		
		my $column = DCC::Model::Column->parseColumn($colDecl,$model);
		
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
		push(@checkDefault,$column)  if(defined($column->columnType->default) && ref($column->columnType->default));
	}
	
	# And now, second pass, where we check the consistency of default values
	foreach my $column (@checkDefault) {
		if(exists($columnDecl{${$column->columnType->default}})) {
			my $defColumn = $columnDecl{${$column->columnType->default}};
			
			if($defColumn->columnType->use >= DCC::Model::ColumnType::IDREF) {
				$column->columnType->setDefault($defColumn);
			} else {
				Carp::croak('Column '.$column->name.' pretends to use as default value generator column '.${$column->columnType->default}.' which is not neither idref nor required!');
			}
		} else {
			Carp::croak('Column '.$column->name.' pretends to use as default value generator unknown column '.${$column->columnType->default});
		}
	}
	
	return bless(\@columnSet,$class);
}

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
#	idConcept: The concept owning the id columns
#	doMask: Are the columns masked for storage?
# It returns a DCC::Model::ColumnSet instance, with the column declarations
# corresponding to columns with idref restriction
sub idColumns($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $idConcept = shift;
	my $doMask = shift;
	
	my @columnNames = @{$self->idColumnNames};
	my $p_columns = $self->columns;
	my %columns = map { $_ => $p_columns->{$_}->cloneRelated($idConcept,undef,$doMask) } @columnNames;
	
	my @columnSet = (
		\@columnNames,
		\@columnNames,
		\%columns
	);
	
	return bless(\@columnSet,ref($self));
}

# relatedColumns parameters:
#	myConcept: A DCC::Model::Concept instance, which this columnSet belongs
#		The kind of relation could be inheritance, or 1:N
#	relatedConcept: A DCC::Model::RelatedConcept instance
#		(which contains the optional prefix to be set to the name when the columns are cloned)
# It returns a DCC::Model::ColumnSet instance, with the column declarations
# corresponding to columns with idref restriction
sub relatedColumns(;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $myConcept = shift;
	
	my $relatedConcept = shift;
	
	my @columnNames = @{$self->idColumnNames};
	my $p_columns = $self->columns;
	my @refColumnNames = ();
	my %columns = map {
		my $refColumn = $p_columns->{$_}->cloneRelated($myConcept,$relatedConcept);
		my $refColumnName = $refColumn->name;
		push(@refColumnNames,$refColumnName);
		$refColumnName => $refColumn
	} @columnNames;
	
	my @columnSet = (
		\@refColumnNames,
		\@refColumnNames,
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


package DCC::Model::CompoundType;
# TODO: Compound type refactor in the near future, so work for filename patterns
# can be reused

sub template {
	return $_[0]->[0];
}

sub seps {
	return $_[0]->[1];
}

sub tokens {
	return $_[0]->[2];
}

1;


package DCC::Model::ColumnType;

# This is the constructor.
# parseColumnType parameters:
#	containerDecl: a XML::LibXML::Element containing 'dcc:column-type' nodes, which
#		defines a column type. Only the first one is parsed.
#	model: a DCC::Model instance, used to validate.
#	columnName: The column name, used for error messages
# returns a DCC::Model::ColumnType instance, with all the information related to
# types, restrictions and enumerated values of this ColumnType.
sub parseColumnType($$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $containerDecl = shift;
	my $model = shift;
	my $columnName = shift;
	
	# Item type
	# column use (idref, required, optional)
	# content restrictions
	# default value
	# array separators
	# null values
	my @nullValues = ();
	my @columnType = (undef,undef,undef,undef,undef,\@nullValues);
	
	# Let's parse the column type!
	foreach my $colType ($containerDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'column-type')) {
		#First, the item type
		my $itemType = $colType->getAttribute('item-type');
		
		my $refItemType = $model->getItemType($itemType);
		Carp::croak("unknown type '$itemType' for column $columnName")  unless(defined($refItemType));
		
		$columnType[DCC::Model::ColumnType::TYPE] = $itemType;
		
		# Column use
		my $columnKind = $colType->getAttribute('column-kind');
		# Idref equals 0; required, 1; desirable, -1; optional, -2
		if(exists((DCC::Model::ColumnType::STR2TYPE)->{$columnKind})) {
			$columnType[DCC::Model::ColumnType::USE] = (DCC::Model::ColumnType::STR2TYPE)->{$columnKind};
		} else {
			Carp::croak("Column $columnName has a unknown kind: $columnKind");
		}
		
		# Content restrictions (children have precedence over attributes)
		# First, is it a compound type?
		unless(defined($refItemType->[DCC::Model::TYPEPATTERN])) {
			if($colType->hasAttribute('compound-template') && $colType->hasAttribute('compound-seps')) {
				# tokens, separators
				my $template = $colType->getAttribute('compound-template');
				my $seps = $colType->getAttribute('compound-seps');
				
				my %sepVal = ();
				foreach my $sep (split(//,$seps)) {
					if(exists($sepVal{$sep})) {
						Carp::croak("Column $columnName has repeated the compound separator $sep!")
					}
					
					$sepVal{$sep}=undef;
				}
				
				my @tokenNames = split(/[$seps]/,$template);
				
				# TODO: refactor compound types
				# compound separators, token names
				my @compoundDecl = ($template,$seps,\@tokenNames);
				$columnType[DCC::Model::ColumnType::RESTRICTION] = bless(\@compoundDecl,'DCC::Model::CompoundType');
			} else {
				Carp::croak("Column $columnName was declared as compound type, but some of the needed attributes (compound-template, compound-seps) is not declared");
			}
		} else {
			# Let's save allowed null values
			foreach my $null ($colType->getChildrenByTagNameNS(DCC::Model::dccNamespace,'null')) {
				my $val = $null->textContent();
				
				if($model->isValidNull($val)) {
					# Let's save the default value
					push(@nullValues,$val);
				} else {
					Carp::croak("Column $columnName uses an unknown default value: $val");
				}
			}
			
			my @cvChildren = $colType->getChildrenByTagNameNS(DCC::Model::dccNamespace,'cv');
			my @patChildren = $colType->getChildrenByTagNameNS(DCC::Model::dccNamespace,'pattern');
			if(scalar(@cvChildren)>0 || (scalar(@patChildren)==0 && $colType->hasAttribute('cv'))) {
				if(scalar(@cvChildren)>0) {
					$columnType[DCC::Model::ColumnType::RESTRICTION] = DCC::Model::CV->parseCV($cvChildren[0],$model);
				} else {
					my $namedCV = $model->getNamedCV($colType->getAttribute('cv'));
					if(defined($namedCV)) {
						$columnType[DCC::Model::ColumnType::RESTRICTION] = $namedCV;
					} else {
						Carp::croak("Column $columnName tried to use undeclared CV ".$colType->getAttribute('cv'));
					}
				}
			} elsif(scalar(@patChildren)>0) {
				$columnType[DCC::Model::ColumnType::RESTRICTION] = DCC::Model::__parse_pattern($patChildren[0]);
			} elsif($colType->hasAttribute('pattern')) {
				my $PAT = $model->getNamedPattern($colType->getAttribute('pattern'));
				if(defined($PAT)) {
					$columnType[DCC::Model::ColumnType::RESTRICTION] = $PAT;
				} else {
					Carp::croak("Column $columnName tried to use undeclared pattern ".$colType->getAttribute('pattern'));
				}
			} else {
				$columnType[DCC::Model::ColumnType::RESTRICTION] = undef;
			}
		}
		
		# Default value
		my $defval = $colType->hasAttribute('default')?$colType->getAttribute('default'):undef;
		# Default values must be rechecked once all the columns are available
		$columnType[DCC::Model::ColumnType::DEFAULT] = (defined($defval) && substr($defval,0,2) eq '$$') ? \substr($defval,2): $defval;
		
		# Array separators
		$columnType[DCC::Model::ColumnType::ARRAYSEPS] = undef;
		if($colType->hasAttribute('array-seps')) {
			my $arraySeps = $colType->getAttribute('array-seps');
			if(length($arraySeps) > 0) {
				my %sepVal = ();
				foreach my $sep (split(//,$arraySeps)) {
					if(exists($sepVal{$sep})) {
						Carp::croak("Column $columnName has repeated the array separator $sep!")
					}
					
					$sepVal{$sep}=undef;
				}
				$columnType[DCC::Model::ColumnType::ARRAYSEPS] = $arraySeps;
			}
		}
		
		last;
	}
	
	return bless(\@columnType,$class);
}

# Item type
sub type {
	return $_[0]->[DCC::Model::ColumnType::TYPE];
}

# column use (idref, required, optional)
# Idref equals 0; required, 1; optional, -1
sub use {
	return $_[0]->[DCC::Model::ColumnType::USE];
}

# content restrictions
sub restriction {
	return $_[0]->[DCC::Model::ColumnType::RESTRICTION];
}

# default value
sub default {
	return $_[0]->[DCC::Model::ColumnType::DEFAULT];
}

# array separators
sub arraySeps {
	return $_[0]->[DCC::Model::ColumnType::ARRAYSEPS];
}

# An array of allowed null values
sub allowedNulls {
	return $_[0]->[DCC::Model::ColumnType::ALLOWEDNULLS];
}

sub setDefault($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $val = shift;
	
	$self->[DCC::Model::ColumnType::DEFAULT] = $val;
}

# clone parameters:
#	relatedConcept: optional, it signals whether to change cloned columnType
#		according to relatedConcept hints
# it returns a DCC::Model::ColumnType instance
sub clone(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $relatedConcept = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(defined($relatedConcept) && (ref($relatedConcept) eq '' || !$relatedConcept->isa('DCC::Model::RelatedConcept')));
	
	# Cloning this object
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	if(defined($relatedConcept)) {
		if($relatedConcept->isPartial) {
			$retval->[DCC::Model::ColumnType::USE] = DCC::Model::ColumnType::DESIRABLE;
		}
		
		if($relatedConcept->arity eq 'M') {
			my $sep = $relatedConcept->mArySeparator;
			if(defined($retval->[DCC::Model::ColumnType::ARRAYSEPS]) && index($retval->[DCC::Model::ColumnType::ARRAYSEPS],$sep)!=-1) {
				Carp::croak("Cloned column has repeated the array separator $sep!");
			}
			
			if(defined($retval->[DCC::Model::ColumnType::ARRAYSEPS])) {
				$retval->[DCC::Model::ColumnType::ARRAYSEPS] = $sep . $retval->[DCC::Model::ColumnType::ARRAYSEPS];
			} else {
				$retval->[DCC::Model::ColumnType::ARRAYSEPS] = $sep;
			}
		}
	}
	
	return $retval;
}

1;


package DCC::Model::Column;

# This is the constructor.
# parseColumn parameters:
#	colDecl: a XML::LibXML::Element 'dcc:column' node, which defines
#		a column
#	model: a DCC::Model instance, used to validate
# returns a DCC::Model::Column instance, with all the information related to
# types, restrictions and enumerated values used by this column.
sub parseColumn($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $colDecl = shift;
	my $model = shift;
	
	# Column name, description, annotations, column type, is masked, related concept, related column from the concept
	my @column = (
		$colDecl->getAttribute('name'),
		DCC::Model::DescriptionSet->parseDescriptions($colDecl),
		DCC::Model::AnnotationSet->parseAnnotations($colDecl),
		DCC::Model::ColumnType->parseColumnType($colDecl,$model,$colDecl->getAttribute('name')),
		undef,
		undef,
		undef,
		undef
	);
	
	return bless(\@column,$class);
}

# The column name
sub name {
	return $_[0]->[DCC::Model::Column::NAME];
}

# The description, a DCC::Model::DescriptionSet instance
sub description {
	return $_[0]->[DCC::Model::Column::DESCRIPTION];
}

# Annotations, a DCC::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[DCC::Model::Column::ANNOTATIONS];
}

# It returns a DCC::Model::ColumnType instance
sub columnType {
	return $_[0]->[DCC::Model::Column::COLUMNTYPE];
}

# If this column is masked (because it is a inherited idref on a concept hosted in a hash)
# it will return true, otherwise undef
sub isMasked {
	return $_[0]->[DCC::Model::Column::ISMASKED];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a DCC::Model::Concept instance
# Otherwise, it will return undef
sub refConcept {
	return $_[0]->[DCC::Model::Column::REFCONCEPT];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a DCC::Model::Column instance
# which correlates to
# Otherwise, it will return undef
sub refColumn {
	return $_[0]->[DCC::Model::Column::REFCOLUMN];
}

# If this column is part of a foreign key pointing
# to a concept using related-to, this method will return a DCC::Model::RelatedConcept
# instance which correlates to
# Otherwise, it will return undef
sub relatedConcept {
	return $_[0]->[DCC::Model::Column::RELATED_CONCEPT];
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
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	$retval->[DCC::Model::Column::ISMASKED] = ($doMask)?1:undef;
	
	return $retval;
}

# cloneRelated parameters:
#	refConcept: A DCC::Model::Concept instance, which this column is related to.
#		The kind of relation could be inheritance, or 1:N
#	relatedConcept: optional, DCC::Model::RelatedConcept, which contains the prefix to be set to the name when the column is cloned
#	doMask: optional, it signals whether to mark cloned column
#		as masked, so it should not be considered for value storage in the database.
# it returns a DCC::Model::Column instance
sub cloneRelated($;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $refConcept = shift;
	my $relatedConcept = shift;
	my $prefix = defined($relatedConcept)?$relatedConcept->keyPrefix:undef;
	my $doMask = shift;
	
	# Cloning this object
	my $retval = $self->clone($doMask);
	
	# Adding the prefix
	$retval->[DCC::Model::Column::NAME] = $prefix.$retval->[DCC::Model::Column::NAME]  if(defined($prefix) && length($prefix)>0);
	
	# And adding the relation info
	# to this column
	$retval->[DCC::Model::Column::REFCONCEPT] = $refConcept;
	$retval->[DCC::Model::Column::REFCOLUMN] = $self;
	$retval->[DCC::Model::Column::RELATED_CONCEPT] = $relatedConcept;
	
	# Does this column become optional due the participation?
	# Does this column become an array due the arity?
	if(defined($relatedConcept) && ($relatedConcept->isPartial || $relatedConcept->arity eq 'M')) {
		# First, let's clone the concept type, to avoid side effects
		$retval->[DCC::Model::Column::COLUMNTYPE] = $self->columnType->clone($relatedConcept);
	}
	
	return $retval;
}

1;


package DCC::Model::FilenamePattern;

use constant FileTypeSymbolPrefixes => {
	'$' => 'DCC::Model::Annotation',
	'@' => 'DCC::Model::CV',
	'\\' => 'Regexp',
	'%' => 'DCC::Model::SimpleType'
};

# This is the constructor.
# parseFilenameFormat parameters:
#	filenameFormatDecl: a XML::LibXML::Element 'dcc:filename-format' node
#		which has all the information to defined a named filename format
#		(or pattern)
#	model: A DCC::Model instance, where this filename-pattern is declared
# returns a DCC::Model::FilenamePattern instance, with all the needed information
# to recognise a filename following the defined pattern
sub parseFilenameFormat($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
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
	my $modelAnnotationsHash = $model->annotations->hash;
	while($tokenString =~ /([$validSepsR])(\$?[a-zA-Z][a-zA-Z0-9]*)([^$validSeps]*)/g) {
		# Pattern for the content
		if(FileTypeSymbolPrefixes->{$1} eq 'Regexp') {
			my $pat = $model->getNamedPatter($2);
			if(defined($pat)) {
				# Check against the pattern!
				$pattern .= '('.$pat.')';
				
				# No additional check
				push(@parts,undef);
			} else {
				Carp::croak("Unknown pattern '$2' used in filename-format '$formatString'");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'DCC::Model::SimpleType') {
			my $typeObject = $model->getItemType($2);
			if(defined($typeObject)) {
				my $type = $typeObject->[DCC::Model::TYPEPATTERN];
				if(defined($type)) {
					$pattern .= '('.(($type->isa('Regexp'))?$type:'.+').')';
					
					# No additional check
					push(@parts,undef);
				} else {
					Carp::croak("Type '$2' used in filename-format '$formatString' was not simple");
				}
			} else {
				Carp::croak("Unknown type '$2' used in filename-format '$formatString'");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'DCC::Model::CV') {
			my $CV = $model->getNamedCV($2);
			if(defined($CV)) {
				$pattern .= '(.+)';
				
				# Check the value against the CV
				push(@parts,$CV);
			} else {
				Carp::croak("Unknown controlled vocabulary '$2' used in filename-format '$formatString'");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'DCC::Model::Annotation') {
			my $annot = $2;
			
			# Is it a context-constant?
			if(substr($annot,0,1) eq '$') {
				$pattern .= '(.+)';
				
				# Store the value in this context variable
				push(@parts,$annot);
			} else {
				
				if(exists($modelAnnotationsHash->{$annot})) {
					# As annotations are at this point known constants, then check the exact value
					$pattern .= '(\Q'.$modelAnnotationsHash->{$annot}.'\E)';
					
					# No additional check
					push(@parts,undef);
				} else {
					Carp::croak("Unknown model annotation '$2' used in filename-format '$formatString'");
				}
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
	
	# Now, the Regexp object!
	$filenamePattern[1] = qr/$pattern/;
	
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
		if($part->isa('DCC::Model::CV')) {
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

# A DCC::Model::Concept instance, which represents the identifying concept of this one
sub idConcept {
	return $_[0]->[7];
}

# related conceptNames, an array of DCC::Model::RelatedConcept (trios concept domain name, concept name, prefix)
sub relatedConcepts {
	return $_[0]->[8];
}

# refColumns parameters:
#	relatedConcept: The DCC::Model::RelatedConcept instance which rules this (with the optional prefix to put on cloned idref columns)
# It returns a DCC::Model::ColumnSet instance with clones of all the idref columns
# referring to this object and with a possible prefix.
sub refColumns($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $relatedConcept = shift;
	
	return $self->columnSet->relatedColumns($self,$relatedConcept);
}

# idColumns parameters:
#	doMask: Are the columns masked for storage?
sub idColumns(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $doMask = shift;
	
	return $self->columnSet->idColumns($self,$doMask);
}

1;


package DCC::Model::RelatedConcept;

sub conceptDomainName {
	return $_[0]->[0];
}

sub conceptName {
	return $_[0]->[1];
}

sub keyPrefix {
	return $_[0]->[2];
}

sub concept {
	return $_[0]->[3];
}

# It returns a column set with the remote columns used for this relation
sub columnSet {
	return $_[0]->[4];
}

# It returns 1 or M
sub arity {
	return $_[0]->[5];
}

# It returns the separator
sub mArySeparator {
	return $_[0]->[6]
}

# It returns 1 or undef
sub isPartial {
	return $_[0]->[7];
}

sub setRelatedConcept($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $concept = shift;
	my $columnSet = shift;
	
	Carp::croak('Parameter must be either a DCC::Model::Concept or undef')  unless(!defined($concept) || (ref($concept) && $concept->isa('DCC::Model::Concept')));
	Carp::croak('Parameter must be either a DCC::Model::ColumnSet or undef')  unless(!defined($columnSet) || (ref($columnSet) && $columnSet->isa('DCC::Model::ColumnSet')));
	
	$self->[3] = $concept;
	$self->[4] = $columnSet;
}

1;