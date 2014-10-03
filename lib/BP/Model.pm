#!/usr/bin/perl -W

use v5.12;
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

use BP::Model::AnnotationSet;
use BP::Model::Collection;
use BP::Model::CompoundType;
use BP::Model::ConceptDomain;
use BP::Model::ConceptType;
use BP::Model::CV;
use BP::Model::CV::Meta;
use BP::Model::FilenamePattern;

# Main package
package BP::Model;
#use version 0.77;
#our $VERSION = qv('0.2.0');

use constant BPSchemaFilename => 'bp-schema.xsd';

# This is to keep backward compatibility
use constant dccNamespace => BP::Model::Common::dccNamespace;

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
	
	# BP model XML file parsing
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
	if(open(my $SCH,'<:utf8',$SCHpath)) {
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
	%{$self->{TYPES}} = %{(BP::Model::ColumnType::ItemTypes)};
	
	# Setting the checks column
	foreach my $p_itemType (values(%{$self->{TYPES}})) {
		my $pattern = $p_itemType->[BP::Model::ColumnType::TYPEPATTERN];
		if(defined($pattern)) {
			$p_itemType->[BP::Model::ColumnType::DATATYPECHECKER] =  (ref($pattern) eq 'Regexp')?sub { $_[0] =~ $pattern }:\&BP::Model::ColumnType::__true;
		}
	}
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{_modelAbsPath};
}

# openModel parameters:
#	(none)
# It returns an open filehandle (either a File::Temp instance or a Perl GLOB (file handle))
sub openModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
		# This file must be open in binary, as XML::LibXML and Digest::SHA1 expect it so
		open(my $X,'<:bytes',$self->{_modelAbsPath});
		
		return $X;
	}
}

sub librarySchemaPath() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	unless(exists($self->{_schemaPath})) {
		if(exists($self->{_BPZIP})) {
			my $memberSchema = $self->{_BPZIP}->memberNamed(BPMODEL_SCHEMA);
			Carp::croak("Unable to find embedded bpmodel schema in bpmodel")  unless(defined($memberSchema));
			my $tempSchema = File::Temp->new();
			Carp::croak("Unable to save embedded bpmodel schema")  if($memberSchema->extractToFileNamed($tempSchema->filename())!=Archive::Zip::AZ_OK);
			
			$self->{_schemaPath} = $tempSchema;
		} else {
			my $schemaDir = File::Basename::dirname(__FILE__);
			
			$self->{_schemaPath} = File::Spec->catfile($schemaDir,BPSchemaFilename);
		}
	}
	
	return $self->{_schemaPath};
}

# schemaModel parameters:
#	(none)
# returns a XML::LibXML::Schema instance
sub schemaModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	# Used later
	my $modelDoc = $self->{modelDoc};

	# change CV root
	$self->{_cvDecl}->setAttribute('dir',BPMODEL_CV);
	
	# This hash is used to avoid collisions
	my %newPaths = ();
	# Change CV paths to relative ones
	foreach my $CVfile (@{$self->{CVfiles}}) {
		my $cv = $CVfile->xmlElement();
		
		my $origPath = $CVfile->localFilename();
		
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my(@keys) = @_;
	
	return join("\n",map { $_.BPMODEL_SIG_KEYVAL_SEP.$self->{'_'.$_} } @keys);
}

# saveBPModel parameters:
#	filename: The file where the model (in bpmodel format) is going to be saved
# TODO/TOFINISH
sub saveBPModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
			$bpModel->addFile($CVfile->localFilename(),$cvprefix.$CVfile->xmlElement()->textContent(),Archive::Zip::COMPRESSION_LEVEL_BEST_COMPRESSION);
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
#	model: XML::LibXML::Document node following BP schema
# The method parses the input XML::LibXML::Document and fills in the
# internal memory structures used to represent a BP model
sub digestModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $modelDoc = shift;
	my $modelRoot = $modelDoc->documentElement();
	
	my $model = $self;
	
	$self->{modelDoc} = $modelDoc;
	
	# First, let's store the key values, project name and schema version
	$self->{project} = $modelRoot->getAttribute('project');
	$self->{schemaVer} = $modelRoot->getAttribute('schemaVer');
	
	# Let's register the metadata
	my $destMetaCol = undef;
	foreach my $metaDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'metadata')) {
		# The documentation directory, which complements this model
		my $docsDir = $metaDecl->getAttribute('documentation-dir');
		# We need to translate relative paths to absolute ones
		$docsDir = File::Spec->rel2abs($docsDir,$self->{_modelDir})  unless(File::Spec->file_name_is_absolute($docsDir));
		$self->{_docsDir} = $docsDir;
		
		# The optional collection where mapped metadata is going to be stored
		$destMetaCol = $metaDecl->getAttribute('collection')  if($metaDecl->hasAttribute('collection'));
		
		# Now, let's store the annotations
		$self->{ANNOTATIONS} = BP::Model::AnnotationSet->parseAnnotations($metaDecl);

		last;
	}
	
	
	# Now, the collection domain
	my $p_collections = undef;
	foreach my $colDom ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'collection-domain')) {
		$p_collections = $self->{COLLECTIONS} = BP::Model::Collection::parseCollections($colDom);
		last;
	}
	
	if(defined($destMetaCol)) {
		if(exists($p_collections->{$destMetaCol})) {
			$self->{_metaColl} = $p_collections->{$destMetaCol};
		} else {
			Carp::croak("Destination collection $destMetaCol for metadata has not been declared");
		}
	} else {
		$self->{_metaColl} = undef;
	}

	# Next stop, controlled vocabulary
	my %cv = ();
	my @cvArray = ();
	$self->{CV} = \%cv;
	$self->{CVARRAY} = \@cvArray;
	foreach my $cvDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'cv-declarations')) {
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
		
		foreach my $cv ($cvDecl->childNodes()) {
			next  unless($cv->nodeType == XML::LibXML::XML_ELEMENT_NODE && ($cv->localname eq 'cv' || $cv->localname eq 'meta-cv'));
			
			my $p_structCV = undef;
			if($cv->localname eq 'cv') {
				$p_structCV = BP::Model::CV->parseCV($cv,$model);
			} else {
				$p_structCV = BP::Model::CV::Meta->parseMetaCV($cv,$model);
			}
			
			# Let's store named CVs here, not anonymous ones
			if(Scalar::Util::blessed($p_structCV) && defined($p_structCV->name)) {
				$cv{$p_structCV->name}=$p_structCV;
				push(@cvArray,$p_structCV);
			}
		}
		
		last;
	}
	
	foreach my $nullDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'null-values')) {
		my $p_nullCV = BP::Model::CV->parseCV($nullDecl,$model);
		
		# Let's store the controlled vocabulary for nulls
		# as a BP::Model::CV
		$self->{NULLCV} = $p_nullCV;
	}
	
	# A safeguard for this parameter
	$self->{_cvDir} = $self->{_modelDir}  unless(exists($self->{_cvDir}));
	
	# Now, the pattern declarations
	foreach my $patternDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'pattern-declarations')) {
		$self->{PATTERNS} = BP::Model::Common::parsePatterns($patternDecl);		
		last;
	}
	
	# And we start with the concept types
	my %compoundTypes = ();
	$self->{COMPOUNDTYPES} = \%compoundTypes;
	foreach my $compoundTypesDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'compound-types')) {
		foreach my $compoundTypeDecl ($compoundTypesDecl->childNodes()) {
			next  unless($compoundTypeDecl->nodeType == XML::LibXML::XML_ELEMENT_NODE && $compoundTypeDecl->localname eq 'compound-type');
			
			# We need to give the model because a compound type could use in one of its facets a previously declared compound type
			my $compoundType = BP::Model::CompoundType->parseCompoundType($compoundTypeDecl,$model);
			$compoundTypes{$compoundType->name} = $compoundType;
		}
		
		last;
	}

	# And we start with the concept types
	my %conceptTypes = ();
	$self->{CTYPES} = \%conceptTypes;
	foreach my $conceptTypesDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'concept-types')) {
		foreach my $ctype ($conceptTypesDecl->childNodes()) {
			next  unless($ctype->nodeType == XML::LibXML::XML_ELEMENT_NODE && $ctype->localname eq 'concept-type');
			
			my @conceptTypeLineage = BP::Model::ConceptType::parseConceptTypeLineage($ctype,$model);
			
			# Now, let's store the concrete (non-anonymous, abstract) concept types
			map { $conceptTypes{$_->name} = $_  if(defined($_->name)); } @conceptTypeLineage;
		}
		
		last;
	}
	
	# The different filename formats
	my %filenameFormats = ();
	$self->{FPATTERN} = \%filenameFormats;
	
	foreach my $filenameFormatDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'filename-format')) {
		my $filenameFormat = BP::Model::FilenamePattern->parseFilenameFormat($filenameFormatDecl,$model);
		
		$filenameFormats{$filenameFormat->name} = $filenameFormat;
	}
	
	# Oh, no! The concept domains!
	my @conceptDomains = ();
	my %conceptDomainHash = ();
	$self->{CDOMAINS} = \@conceptDomains;
	$self->{CDOMAINHASH} = \%conceptDomainHash;
	foreach my $conceptDomainDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'concept-domain')) {
		my $conceptDomain = BP::Model::ConceptDomain->parseConceptDomain($conceptDomainDecl,$model);
		
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $cvline = shift;
	$cvline = Encode::encode_utf8($cvline);
	$self->{_SHA}->add($cvline);
	$self->{_CVSHA}->add($cvline);
}

# registerCVfile parameters:
#	cv: a XML::LibXML::Element 'dcc:cv' node
# returns a BP::Model::CV array reference, with all the controlled vocabulary
# stored inside.
# If the CV is in an external file, this method reads it, and registers it to save it later.
# If the CV is in an external URI, this method only checks whether it is available (TBD)
sub registerCV($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $CV = shift;
	
	# This key is internally used to register all the file CVs
	$self->{CVfiles} = []  unless(exists($self->{CVfiles}));
	
	push(@{$self->{CVfiles}},$CV)  if(defined($CV->localFilename));
}

sub collections() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{COLLECTIONS};
}

sub getCollection($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $collName = shift;
	
	return exists($self->{COLLECTIONS}{$collName})?$self->{COLLECTIONS}{$collName}:undef;
}

sub getItemType($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $itemType = shift;
	
	return exists($self->{TYPES}{$itemType})?$self->{TYPES}{$itemType}:undef;
}

sub isValidNull($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $val = shift;
	
	return $self->{NULLCV}->isValid($val);
}

sub getNamedCV($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $name = shift;
	
	return exists($self->{CV}{$name})?$self->{CV}{$name}:undef;
}

sub getNamedPattern($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $name = shift;
	
	return exists($self->{PATTERNS}{$name})?$self->{PATTERNS}{$name}:undef;
}

sub getCompoundType($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $name = shift;
	
	return exists($self->{COMPOUNDTYPES}{$name})?$self->{COMPOUNDTYPES}{$name}:undef;
}

sub getConceptType($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $name = shift;
	
	return exists($self->{CTYPES}{$name})?$self->{CTYPES}{$name}:undef;
}

sub getFilenamePattern($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $name = shift;
	
	return exists($self->{FPATTERN}{$name})?$self->{FPATTERN}{$name}:undef;
}

# It returns an array with BP::Model::ConceptDomain instances (all the concept domains)
sub conceptDomains() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{CDOMAINS};
}

# It returns a hash whose values are BP::Model::ConceptDomain instances (all the concept domains)
sub conceptDomainsHash() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{CDOMAINHASH};
}

# getConceptDomain parameters:
#	conceptDomainName: The name of the concept domain to look for
# returns a BP::Model::ConceptDomain object or undef (if it does not exist)
sub getConceptDomain($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $conceptDomainName = shift;
	
	return exists($self->{CDOMAINHASH}{$conceptDomainName})?$self->{CDOMAINHASH}{$conceptDomainName}:undef;
}

# matchConceptsFromFilename parameters:
#	filename: A filename which we want to know about its corresponding concept
sub matchConceptsFromFilename($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $filename = shift;
	
	# Let's be sure we have a relative path
	$filename = File::Basename::basename($filename);
	
	my @matched = ();
	foreach my $fpattern (values(%{$self->{FPATTERN}})) {
		my($concept,$mappedValues,$extractedValues) = $fpattern->matchConcept($filename);
		
		# Did we match a concept?
		if(defined($concept)) {
			push(@matched,[$concept,$mappedValues,$extractedValues]);
		}
	}
	
	return (scalar(@matched)>0) ? \@matched : undef;
}

# It returns a BP::Model::AnnotationSet instance
sub annotations() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{ANNOTATIONS};
}

# It returns the project name
sub projectName() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{project};
}

# It returns a model version
sub schemaVer() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{schemaVer};
}

# It returns the SHA1 digest of the model
sub modelSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{_modelSHA1};
}

sub fullmodelSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{_fullmodelSHA1};
}

sub CVSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{_cvSHA1};
}

sub schemaSHA1() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{_schemaSHA1};
}

# It returns schemaVer
sub versionString() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{schemaVer};
}

# The base documentation directory
sub documentationDir() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{_docsDir};
}

# A reference to an array of BP::Model::CV instances
sub namedCVs() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{CVARRAY};
}

# A reference to a BP::Model::CV instance, for the valid null values for this model
sub nullCV() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{NULLCV};
}

sub types() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{TYPES};
}

# It returns a BP::Model::Collection instance
sub metadataCollection() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	return $self->{_metaColl};
}

1;
