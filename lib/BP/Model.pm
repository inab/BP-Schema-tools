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

# Needed for JSON::true and JSON::false declarations
use JSON;

# Early subpackage constant declarations
package BP::Model::CV;

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

package BP::Model::ColumnType;

# These two are to prepare the values to be inserted
use boolean;
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
	($_[0] =~ /^1|[tT](?:rue)?|[yY](?:es)?$/)?boolean::true:boolean::false
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
	BP::Model::ColumnType::DECIMAL_TYPE	=> [qr/^(?:0|(?:-?[1-9][0-9]*))(?:\.[0-9]+)?$/,undef,\&__decimal,undef],
	BP::Model::ColumnType::BOOLEAN_TYPE	=> [qr/^[10]|[tT](?:rue)?|[fF](?:alse)?|[yY](?:es)?|[nN]o?$/,1,\&__boolean,undef],
	BP::Model::ColumnType::TIMESTAMP_TYPE	=> [qr/^[1-9][0-9][0-9][0-9](?:(?:1[0-2])|(?:0[1-9]))(?:(?:[0-2][0-9])|(?:3[0-1]))$/,1,\&__timestamp,undef],
	BP::Model::ColumnType::DURATION_TYPE	=> [qr/^$/,1,\&__duration,undef],
	BP::Model::ColumnType::COMPOUND_TYPE	=> [undef,1,undef,undef]
};

# Always valid value
sub __true { 1 };

# Main package
package BP::Model;
#use version 0.77;
#our $VERSION = qv('0.2.0');

use constant BPSchemaFilename => 'bp-schema.xsd';
use constant dccNamespace => 'http://www.blueprint-epigenome.eu/dcc/schema';

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
		open(my $X,'<:utf8',$self->{_modelAbsPath});
		
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
	$self->{ANNOTATIONS} = BP::Model::AnnotationSet->parseAnnotations($modelRoot);
	
	# Now, the collection domain
	my $p_collections = undef;
	foreach my $colDom ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'collection-domain')) {
		$p_collections = $self->{COLLECTIONS} = BP::Model::Collection::parseCollections($colDom);
		last;
	}
	
	# Next stop, controlled vocabulary
	my %cv = ();
	my @cvArray = ();
	$self->{CV} = \%cv;
	$self->{CVARRAY} = \@cvArray;
	foreach my $cvDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'cv-declarations')) {
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
			
			my $p_structCV = BP::Model::CV->parseCV($cv,$model);
			
			# Let's store named CVs here, not anonymous ones
			if(defined($p_structCV->name)) {
				$cv{$p_structCV->name}=$p_structCV;
				push(@cvArray,$p_structCV);
			}
		}
		
		last;
	}
	
	foreach my $nullDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'null-values')) {
		my $p_nullCV = BP::Model::CV->parseCV($nullDecl,$model);
		
		# Let's store the controlled vocabulary for nulls
		# as a BP::Model::CV
		$self->{NULLCV} = $p_nullCV;
	}
	
	# A safeguard for this parameter
	$self->{_cvDir} = $self->{_modelDir}  unless(exists($self->{_cvDir}));
	
	# Now, the pattern declarations
	foreach my $patternDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'pattern-declarations')) {
		$self->{PATTERNS} = BP::Model::parsePatterns($patternDecl);		
		last;
	}
	
	# And we start with the concept types
	my %compoundTypes = ();
	$self->{COMPOUNDTYPES} = \%compoundTypes;
	foreach my $compoundTypesDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'compound-types')) {
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
	foreach my $conceptTypesDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'concept-types')) {
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
	
	foreach my $filenameFormatDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'filename-format')) {
		my $filenameFormat = BP::Model::FilenamePattern->parseFilenameFormat($filenameFormatDecl,$model);
		
		$filenameFormats{$filenameFormat->name} = $filenameFormat;
	}
	
	# Oh, no! The concept domains!
	my @conceptDomains = ();
	my %conceptDomainHash = ();
	$self->{CDOMAINS} = \@conceptDomains;
	$self->{CDOMAINHASH} = \%conceptDomainHash;
	foreach my $conceptDomainDecl ($modelRoot->getChildrenByTagNameNS(BP::Model::dccNamespace,'concept-domain')) {
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $CV = shift;
	
	# This key is internally used to register all the file CVs
	$self->{CVfiles} = []  unless(exists($self->{CVfiles}));
	
	push(@{$self->{CVfiles}},$CV)  if(defined($CV->localFilename));
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

sub getCompoundType($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $name = shift;
	
	return exists($self->{COMPOUNDTYPES}{$name})?$self->{COMPOUNDTYPES}{$name}:undef;
}

sub getConceptType($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $name = shift;
	
	return exists($self->{CTYPES}{$name})?$self->{CTYPES}{$name}:undef;
}

sub getFilenamePattern($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $name = shift;
	
	return exists($self->{FPATTERN}{$name})?$self->{FPATTERN}{$name}:undef;
}

# It returns an array with BP::Model::ConceptDomain instances (all the concept domains)
sub conceptDomains() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{CDOMAINS};
}

# getConceptDomain parameters:
#	conceptDomainName: The name of the concept domain to look for
# returns a BP::Model::ConceptDomain object or undef (if it does not exist)
sub getConceptDomain($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptDomainName = shift;
	
	return exists($self->{CDOMAINHASH}{$conceptDomainName})?$self->{CDOMAINHASH}{$conceptDomainName}:undef;
}

# matchConceptsFromFilename parameters:
#	filename: A filename which we want to know about its corresponding concept
sub matchConceptsFromFilename($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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

# A reference to an array of BP::Model::CV instances
sub namedCVs() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return $self->{CVARRAY};
}

# A reference to a BP::Model::CV instance, for the valid null values for this model
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

# This method is called by JSON library, which gives the structure to
# be translated into JSON
sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# We need collections by path, not by id
	my %jsonColls = map { $_->path => $_ } values(%{$self->{COLLECTIONS}});
	
	# The main features
	my %jsonModel=(
		'project'	=> $self->{project},
		'schemaVer'	=> $self->{schemaVer},
		'annotations'	=> $self->{ANNOTATIONS},
		'collections'	=> \%jsonColls,
		'domains'	=> $self->{CDOMAINHASH},
	);
	
	return \%jsonModel;
}

1;

# And now, the helpers for the different pseudo-packages

package BP::Model::Collection;

sub parseCollections($) {
	my $colDom = shift;
	
	my %collections = ();
	foreach my $coll ($colDom->childNodes()) {
		next  unless($coll->nodeType == XML::LibXML::XML_ELEMENT_NODE && $coll->localname eq 'collection');
		
		my $collection = BP::Model::Collection->parseCollection($coll);
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
	my @collection = ($coll->getAttribute('name'),$coll->getAttribute('path'),BP::Model::Index::parseIndexes($coll));
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

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonCollection = (
		'name'	=> $self->name,
		'path'	=> $self->path,
		'indexes'	=> $self->indexes
	);
	
	return \%jsonCollection;
}

1;


package BP::Model::Index;

# This is an static method.
# parseIndexes parameters:
#	container: a XML::LibXML::Element container of 'dcc:index' elements
# returns an array reference, containing BP::Model::Index instances
sub parseIndexes($) {
	my $container = shift;
	
	# And the index declarations for this collection
	my @indexes = ();
	foreach my $ind ($container->getChildrenByTagNameNS(BP::Model::dccNamespace,'index')) {
		push(@indexes,BP::Model::Index->parseIndex($ind));
	}
	
	return \@indexes;
}

# This is the constructor.
# parseIndex parameters:
#	ind: a XML::LibXML::Element which is a 'dcc:index'
# returns a BP::Model::Index instance
sub parseIndex($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $ind = shift;
	
	# Is index unique?, attributes (attribute name, ascending/descending)
	my @index = (($ind->hasAttribute('unique') && $ind->getAttribute('unique') eq 'true')?1:undef,[]);

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

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return {
		'unique'	=> $self->isUnique ? JSON::true : JSON::false,
		'attrs'	=> [map { { 'name' => $_->[0], 'ord' => $_->[1] } } @{$self->indexAttributes}],
	};
}

1;


package BP::Model::DescriptionSet;

# This is the empty constructor.
sub new() {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	return bless([],$class);
}

# This is the constructor.
# parseDescriptions parameters:
#	container: a XML::LibXML::Element container of 'dcc:description' elements
# returns a BP::Model::DescriptionSet array reference, containing the contents of all
# the 'dcc:description' XML elements found
sub parseDescriptions($) {
	my $self = shift;
	
	my $container = shift;
	
	# Dual instance/class method behavior
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new();
	}
	
	foreach my $description ($container->getChildrenByTagNameNS(BP::Model::dccNamespace,'description')) {
		my @dChildren = $description->nonBlankChildNodes();
		
		my $value = undef;
		# We only save the nodeset when 
		foreach my $dChild (@dChildren) {
			#next  unless($dChild->nodeType == XML::LibXML::XML_ELEMENT_NODE);
			
			$value = \@dChildren;
			last;
		}
		
		push(@{$self},defined($value)?$value:$description->textContent());
	}
	
	return $self;
}

# This method adds a new annotation to the annotation set
sub addDescription($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	my $desc = shift;

	push(@{$self},$desc);
}

# The clone method
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	return $retval;
}

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(scalar(@{$self})>0) {
		my @arrayRef = @{$self};
		
		foreach my $val (@arrayRef) {
			if(ref($val) eq 'ARRAY') {
				$val = join('',map { (ref($_) && $_->can('toString'))?$_->toString():$_ } @{$val});
			} elsif(ref($val) && $val->can('toString')) {
				$val = $val->toString();
			}
		}
		
		return \@arrayRef;
	} else {
		return undef;
	}
}

1;


package BP::Model::AnnotationSet;

# This is the empty constructor.
#	seedAnnotationSet: an optional BP::Model::AnnotationSet used as seed
sub new(;$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $seedAnnotationSet = shift;
	
	my %annotationHash = ();
	my @annotationOrder = ();
	if(defined($seedAnnotationSet)) {
		%annotationHash = %{$seedAnnotationSet->hash};
		@annotationOrder = @{$seedAnnotationSet->order};
	}
	
	my @annotations = (\%annotationHash,\@annotationOrder);
	
	return bless(\@annotations,$class);
}

# This is the constructor.
# parseAnnotations paremeters:
#	container: a XML::LibXML::Element container of 'dcc:annotation' elements
#	seedAnnotationSet: an optional BP::Model::AnnotationSet used as seed
# It returns a BP::Model::AnnotationSet hash reference, containing the contents of all the
# 'dcc:annotation' XML elements found
sub parseAnnotations($;$) {
	my $self = shift;
	
	my $container = shift;
	my $seedAnnotationSet = shift;
	
	# Dual instance/class method behavior
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new($seedAnnotationSet);
	}
	
	my $p_hash = $self->hash;
	my $p_order = $self->order;
	
	foreach my $annotation ($container->getChildrenByTagNameNS(BP::Model::dccNamespace,'annotation')) {
		unless(exists($p_hash->{$annotation->getAttribute('key')})) {
			push(@{$p_order},$annotation->getAttribute('key'));
		}
		my @aChildren = $annotation->nonBlankChildNodes();
		
		my $value = undef;
		foreach my $aChild (@aChildren) {
			next  unless($aChild->nodeType == XML::LibXML::XML_ELEMENT_NODE);
			if($aChild->namespaceURI() eq BP::Model::dccNamespace) {
				$value = $aChild;
			} else {
				$value = \@aChildren;
			}
			last;
		}
		$p_hash->{$annotation->getAttribute('key')} = defined($value)?$value:$annotation->textContent();
	}
	
	return $self;
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

# This method adds the annotations from an existing BP::Model::AnnotationSet instance
sub addAnnotations($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	my $annotations = shift;
	
	my $hash = $self->hash;
	my $annotationsHash = $annotations->hash;
	foreach my $key (@{$annotations->order}) {
		push(@{$self->order},$key)  unless(exists($hash->{$key}));
		$hash->{$key} = $annotationsHash->{$key};
	}
}

# The clone method
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %hash = %{$self->hash};
	my @order = @{$self->order};
	my $retval = bless([\%hash,\@order],ref($self));
	
	return $retval;
}

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(scalar(keys(%{$self->hash}))>0) {
		my %hashRes = %{$self->hash};
		
		foreach my $val (values(%hashRes)) {
			if(ref($val) eq 'ARRAY') {
				$val = join('',map { (ref($_) && $_->can('toString'))?$_->toString():$_ } @{$val});
			} elsif(ref($val) && $val->can('toString')) {
				$val = $val->toString();
			}
		}
		
		return \%hashRes;
	} else {
		return undef;
	}
}

1;

package BP::Model::CV::Term;

use constant {
	KEY	=>	0,
	KEYS	=>	1,
	NAME	=>	2,
	PARENTS	=>	3,
	ANCESTORS	=>	4,
	ISALIAS	=>	5,
};

# Constructor
# new parameters:
#	key: a string, or an array of strings
#	name: a string
#	parents: undef or an array of strings
#	isAlias: undef or true
sub new($$;$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $key = shift;
	my $keys = undef;
	my $name = shift;
	# Optional parameters
	my $parents = shift;
	my $isAlias = shift;
	
	if(ref($key) eq 'ARRAY') {
		$keys = $key;
		$key = $keys->[0];
	} else {
		$keys = [$key];
	}
	
	# Ancestors will be resolved later
	my @term=($key,$keys,$name,$parents,undef,$isAlias);
	
	return bless(\@term,$class);
}

# Alternate constructor
# parseAlias parameters:
#	termAlias: a XML::LibXML::Element node, 'dcc:term-alias'
sub parseAlias($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $termAlias = shift;
	my $key = $termAlias->getAttribute('name');
	my $name = '';
	my @parents = ();
	foreach my $el ($termAlias->childNodes()) {
		next  unless($el->nodeType == XML::LibXML::XML_ELEMENT_NODE);
		
		if($el->localname eq 'e') {
			# Saving the "parents" of the alias
			push(@parents,$el->getAttribute('v'));
		} elsif($el->localname eq 'description') {
			$name = $el->textContent();
		}
	}
	
	return $class->new($key,$name,\@parents,1);
}

# The main key of the term
sub key {
	return $_[0]->[KEY];
}

# All the keys of the term: the main and the alternate ones
sub keys {
	return $_[0]->[KEYS];
}

# The name of the term
sub name {
	return $_[0]->[NAME];
}

# The parent(s) of the term, (through "is a" relationships), as their keys
# undef, when they have no parent
sub parents {
	return $_[0]->[PARENTS];
}

# All the ancestors of the term, through transitive "is a",
# or term aliasing. No order is guaranteed, but there will
# be no duplicates.
sub ancestors {
	return $_[0]->[ANCESTORS];
}

# If it returns true, it is an alias (the union of several terms,
# which have been given the role of the parents)
sub isAlias {
	return $_[0]->[ISALIAS];
}

# If it returns true, a call to ancestors method returns the
# calculated lineage
sub gotLineage {
	return defined($_[0]->[ANCESTORS]);
}

# calculateAncestors parameters:
#	p_CV: a reference to a hash, which is the pool of
#		BP::Model::CV::Term instances where this instance can
#		find its parents.
#	doRecover: If true, it tries to recover from unknown parents,
#		removing them
#	p_visited: a reference to an array, which is the pool of
#		BP::Model::CV::Term keys which are still being
#		visited.
sub calculateAncestors($;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# The pool where I should find my parents
	my $p_CV = shift;
	my $doRecover = shift;
	my $p_visited = shift;
	$p_visited = []  unless(defined($p_visited));
	
	# The output
	my $p_ancestors = undef;
	
	unless($self->gotLineage()) {
		my @ancestors= ();
		my %ancHash = ();
		my @curatedParents = ();
		
		my $saveCurated = undef;
		if(defined($self->parents)) {
			# Let's gather the lineages
			my @visited = (@{$p_visited},$self->key);
			my %visHash = map { $_ => undef } @visited;
			foreach my $parentKey (@{$self->parents}) {
				unless(exists($p_CV->{$parentKey})) {
					Carp::croak("Parent $parentKey does not exist on the CV for term ".$self->key)  unless($doRecover);
					$saveCurated = 1;
					next;
				}
				
				# Sanitizing alternate keys
				my $parent = $p_CV->{$parentKey};
				$parentKey = $parent->key;
				
				Carp::croak("Detected a lineage term loop: @visited $parentKey\n")  if(exists($visHash{$parentKey}));
				
				# Getting the ancestors
				my $parent_ancestors = $parent->calculateAncestors($p_CV,$doRecover,\@visited);
				
				# Saving only the new ones
				foreach my $parent_ancestor (@{$parent_ancestors},$parentKey) {
					unless(exists($ancHash{$parent_ancestor})) {
						push(@ancestors,$parent_ancestor);
						$ancHash{$parent_ancestor} = undef;
					}
				}
				# And the curated parent
				push(@curatedParents,$parentKey);
			}
		}
		
		# Last, setting up the information
		$p_ancestors = \@ancestors;
		$self->[ANCESTORS] = $p_ancestors;
		$self->[PARENTS] = \@curatedParents  if(defined($saveCurated));
	
	} else {
		$p_ancestors = $self->[ANCESTORS];
	}
	
	return $p_ancestors;
}

# This method serializes the BP::Model::CV instance into a OBO structure
# serialize parameters:
#	O: the output file handle
sub OBOserialize($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $O = shift;
	
	# We need this
	print $O "[Term]\n";
	BP::Model::CV::printOboKeyVal($O,'id',$self->key);
	BP::Model::CV::printOboKeyVal($O,'name',$self->name);
	
	# The alterative ids
	my $first = 1;
	foreach my $alt_id (@{$self->keys}) {
		# Skipping the first one, which is the main id
		if(defined($first)) {
			$first=undef;
			next;
		}
		BP::Model::CV::printOboKeyVal($O,'alt_id',$alt_id);
	}
	if(defined($self->parents)) {
		my $propLabel = ($self->isAlias)?'union_of':'is_a';
		foreach my $parKey (@{$self->parents}) {
			BP::Model::CV::printOboKeyVal($O,$propLabel,$parKey);
		}
	}
	print $O "\n";
}


package BP::Model::CV::External;

# This is the constructor
# parseCVExternal parameters:
#	el: a XML::LibXML::Element 'dcc:cv-uri' node
#	model: a BP::Model instance
sub parseCVExternal($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $el = shift;
	
	# Although it is not going to be materialized here (at least, not yet)
	# let's check whether it is a valid cv-uri
	my $cvURI = $el->textContent();
	
	# TODO: validate URI
	my @externalCV = (URI->new($cvURI),$el->getAttribute('format'),$el->hasAttribute('doc')?URI->new($el->getAttribute('doc')):undef);
	bless(\@externalCV,$class);
}

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


package BP::Model::CV;

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
	CVID		=>	11	# The CV id (used by SQL and MongoDB uniqueness purposes)
};

# The anonymous controlled vocabulary counter, used for anonymous id generation
my $_ANONCOUNTER = 0;

# This is the empty constructor
sub new() {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	# The CV symbolic name, the CV type, the array of CV uri, the CV local filename, the CV local format, the annotations, the documentation paragraphs, the CV (hash and array), aliases (array), XML element of cv-file element
	my $cvAnnot = BP::Model::AnnotationSet->new();
	my $cvDesc = BP::Model::DescriptionSet->new();
	my @structCV=(undef,undef,undef,undef,undef,$cvAnnot,$cvDesc,undef,undef,[],undef,undef);
	
	$structCV[BP::Model::CV::CVKEYS] = [];
	# Hash shared by terms and term-aliases
	$structCV[BP::Model::CV::CVHASH] = {};
	
	return bless(\@structCV,$class);
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
			$_[0]->[BP::Model::CV::CVID] = '_anonCV_'.$_ANONCOUNTER;
			$_ANONCOUNTER++;
		}
	}
	
	return $_[0]->[BP::Model::CV::CVID];
}

# With this method we check the locality of the CV
sub isLocal() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvkey = shift;
	
	return scalar(@{$self->order})>0;
}

# With this method a term or a term-alias is validated
# TODO: fetch on demand the CV is it is not materialized
sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $cvkey = shift;
	
	return exists($self->CV->{$cvkey});
}

# With this method a reference to a validator is given
sub dataChecker {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return sub($) {
		$self->isValid($_[0]);
	};
}

# addTerm parameters:
#	term: a BP::Model::CV::Term instance, which can be a term or an alias
sub addTerm($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $term = shift;

	# Let's initialize
	foreach my $key (@{$term->keys}) {
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
		$cvline =~ s/\s*!.*$//;
		
		# And removing the trailing modifiers, because we don't need that info
		$cvline =~ s/\s*\{.*\}\s*$//;
		
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

# printOboKeyVal parameters:
#	O: the output file handle
#	key: the key name
#	val: the value
sub printOboKeyVal($$$) {
	$_[0]->print($_[1],': ',$_[2],"\n");
}

# This method serializes the BP::Model::CV instance into a OBO structure
# serialize parameters:
#	O: the output file handle
#	comments: the comments to put
sub OBOserialize($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
	printOboKeyVal($O,'format-version','1.2');
	my @timetoks = localtime();
	printOboKeyVal($O,'date',sprintf('%02d:%02d:%04d %02d:%02d',$timetoks[3],$timetoks[4]+1,$timetoks[5]+1900,$timetoks[2],$timetoks[1]));
	printOboKeyVal($O,'auto-generated-by','BP::Model $Id$');
	
	# Are there descriptions?
	foreach my $desc (@{$self->description}) {
		printOboKeyVal($O,'remark',toOBO($desc));
	}
	
	# And now, print each one of the terms
	my $CVhash = $self->CV;
	foreach my $term (@{$self->order},@{$self->aliasOrder}) {
		$CVhash->{$term}->OBOserialize($O);
	}
}

sub _jsonId() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	return 'cv:'.$self->id;
}

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
}

1;

package BP::Model::ConceptType;

# Prototypes of static methods
sub parseConceptTypeLineage($$;$);

# This is an static method.
# parseConceptTypeLineage parameters:
#	ctypeElem: a XML::LibXML::Element 'dcc:concept-type' node
#	model: a BP::Model instance where the concept type was defined
#	ctypeParent: an optional 'BP::Model::ConceptType' instance, which is the
#		parent concept type
# returns an array of 'BP::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseConceptTypeLineage($$;$) {
	my $ctypeElem = shift;
	my $model = shift;
	# Optional parameter, the conceptType parent
	my $ctypeParent = undef;
	$ctypeParent = shift  if(scalar(@_) > 0);
	
	# The returning values array
	my $me = BP::Model::ConceptType->parseConceptType($ctypeElem,$model,$ctypeParent);
	my @retval = ($me);
	
	# Now, let's find subtypes
	foreach my $subtypes ($ctypeElem->getChildrenByTagNameNS(BP::Model::dccNamespace,'subtypes')) {
		foreach my $childCTypeElem ($subtypes->childNodes()) {
			next  unless($childCTypeElem->nodeType == XML::LibXML::XML_ELEMENT_NODE && $childCTypeElem->localname() eq 'concept-type');
			
			# Parse subtypes and store them!
			push(@retval,BP::Model::ConceptType::parseConceptTypeLineage($childCTypeElem,$model,$me));
		}
		last;
	}
	
	return @retval;
}

# This is the constructor.
# parseConceptType parameters:
#	ctypeElem: a XML::LibXML::Element 'dcc:concept-type' node
#	model: a BP::Model instance where the concept type was defined
#	ctypeParent: an optional 'BP::Model::ConceptType' instance, which is the
#		parent concept type
# returns an array of 'BP::Model::ConceptType' instances, which are the concept type
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
			# Let's link the BP::Model::Collection
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
			$ctype[1] = $ctypeParent->goesToCollection;
			$ctype[2] = $ctypeParent->path;
		} else {
			Carp::croak("A concept type must have a storage state of physical or virtual collection");
		}
	}
	
	# Let's parse the columns
	$ctype[4] = BP::Model::ColumnSet->parseColumnSet($ctypeElem,defined($ctypeParent)?$ctypeParent->columnSet:undef,$model);
	
	# And the index declarations
	$ctype[5] = BP::Model::Index::parseIndexes($ctypeElem);
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
sub goesToCollection {
	return $_[0]->[1];
}

# It can be either a BP::Model::Collection instance
# or a string
sub path {
	return $_[0]->[2];
}

# collection, a BP::Model::Collection instance
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
# or a BP::Model::ConceptType instance
sub parent {
	return $_[0]->[3];
}

# columnSet
# It returns a BP::Model::ColumnSet instance, with all the column declarations
sub columnSet {
	return $_[0]->[4];
}

# It returns a reference to an array full of BP::Model::Index instances
sub indexes {
	return $_[0]->[5];
}

1;


package BP::Model::CompoundType;

# This is the constructor.
# parseCompoundType parameters:
#	compoundTypeElem: a XML::LibXML::Element 'dcc:compound-type' node
#	model: a BP::Model instance where the concept type was defined
# returns an array of 'BP::Model::ConceptType' instances, which are the concept type
# and its descendants.
sub parseCompoundType($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $compoundTypeElem = shift;
	my $model = shift;
	my $columnName = shift;
	
	# compound-type name (could be anonymous)
	# columnSet
	# separators
	# template
	# dataMangler
	# isValid
	my @compoundType = (undef,undef,undef,undef,undef,undef);
	
	# If it has name, then it has to have either a collection name or a key name
	# either inline or inherited from the ancestors
	$compoundType[0] = $compoundTypeElem->getAttribute('name')  if($compoundTypeElem->hasAttribute('name'));
	
	# Let's parse the columns
	my $columnSet = BP::Model::ColumnSet->parseColumnSet($compoundTypeElem,undef,$model);
	$compoundType[1] = $columnSet;
	
	my @seps = ();
	foreach my $sepDecl ($compoundTypeElem->getChildrenByTagNameNS(BP::Model::dccNamespace,'sep')) {
		push(@seps,$sepDecl->textContent());
	}
	$compoundType[2] = \@seps;
	
	# Now, let's create a string template
	my $template = $columnSet->columnNames->[0];
	foreach my $pos (0..$#seps) {
		$template .= $seps[$pos];
		$template .= $columnSet->columnNames->[$pos+1];
	}
	
	$compoundType[3] = $template;
	
	# the data mangler!
	my @colMangler = map { [$_, $columnSet->columns->{$_}->columnType->dataMangler] } @{$columnSet->columnNames};
	my @colChecker = map { $columnSet->columns->{$_}->columnType->dataChecker } @{$columnSet->columnNames};
	$compoundType[4] = sub {
		my %result = ();
		
		my $input = $_[0];
		my $idx = 0;
		foreach my $sep (@seps) {
			my $limit = index($input,$sep);
			if($limit!=-1) {
				$result{$colMangler[$idx][0]} = $colMangler[$idx][1]->(substr($input,0,$limit));
				$input = substr($input,$limit+length($sep));
			} else {
				Carp::croak('Data mangler of '.$template.' complained parsing '.$_[0].' on facet '.$colMangler[$idx][0]);
			}
			$idx++;
		}
		# And last one
		$result{$colMangler[$idx][0]} = $colMangler[$idx][1]->($input);
		
		return  \%result;
	};
	
	# And the subref for the dataChecker method
	$compoundType[5] = sub {
		my $input = $_[0];
		my $idx = 0;
		foreach my $sep (@seps) {
			my $limit = index($input,$sep);
			if($limit!=-1) {
				return undef  unless($colChecker[$idx]->(substr($input,0,$limit)));
				$input = substr($input,$limit+length($sep));
			} else {
				return undef;
			}
			$idx++;
		}
		# And last one
		return $colChecker[$idx]->($input);
	};
	
	# The returning values array
	return bless(\@compoundType,$class);
}

# compound type name (could be anonymous), so it would return undef
sub name {
	return $_[0]->[0];
}

# columnSet
# It returns a BP::Model::ColumnSet instance, with all the column declarations inside this compound type
sub columnSet {
	return $_[0]->[1];
}

sub seps {
	return $_[0]->[2];
}

sub template {
	return $_[0]->[3];
}

sub tokens {
	return $_[0]->[1]->columnNames;
}

sub dataMangler {
	return $_[0]->[4];
}

sub dataChecker {
	return $_[0]->[5];
}

sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $val = shift;
	
	return $self->dataChecker->($val);
}

1;


package BP::Model::ColumnType;

use constant {
	TYPE	=>	0,
	USE	=>	1,
	RESTRICTION	=>	2,
	DEFAULT	=>	3,
	ARRAYSEPS	=>	4,
	ALLOWEDNULLS	=>	5,
	DATAMANGLER	=>	6,
	DATACHECKER	=>	7,
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

# This is the constructor.
# parseColumnType parameters:
#	containerDecl: a XML::LibXML::Element containing 'dcc:column-type' nodes, which
#		defines a column type. Only the first one is parsed.
#	model: a BP::Model instance, used to validate.
#	columnName: The column name, used for error messages
# returns a BP::Model::ColumnType instance, with all the information related to
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
	# data mangler
	# data checker
	my @nullValues = ();
	my @columnType = (undef,undef,undef,undef,undef,\@nullValues);
	
	# Let's parse the column type!
	foreach my $colType ($containerDecl->getChildrenByTagNameNS(BP::Model::dccNamespace,'column-type')) {
		#First, the item type
		my $itemType = $colType->getAttribute('item-type');
		
		my $refItemType = $model->getItemType($itemType);
		Carp::croak("unknown type '$itemType' for column $columnName")  unless(defined($refItemType));
		
		$columnType[BP::Model::ColumnType::TYPE] = $itemType;
		
		# Column use
		my $columnKind = $colType->getAttribute('column-kind');
		# Idref equals 0; required, 1; desirable, -1; optional, -2
		if(exists((BP::Model::ColumnType::STR2TYPE)->{$columnKind})) {
			$columnType[BP::Model::ColumnType::USE] = (BP::Model::ColumnType::STR2TYPE)->{$columnKind};
		} else {
			Carp::croak("Column $columnName has a unknown kind: $columnKind");
		}
		
		# Content restrictions (children have precedence over attributes)
		# Let's save allowed null values
		foreach my $null ($colType->getChildrenByTagNameNS(BP::Model::dccNamespace,'null')) {
			my $val = $null->textContent();
			
			if($model->isValidNull($val)) {
				# Let's save the default value
				push(@nullValues,$val);
			} else {
				Carp::croak("Column $columnName uses an unknown default value: $val");
			}
		}
		
		# First, is it a compound type?
		my @compChildren = $colType->getChildrenByTagNameNS(BP::Model::dccNamespace,'compound-type');
		if(defined($refItemType->[BP::Model::ColumnType::DATATYPEMANGLER]) && (scalar(@compChildren)>0 || $colType->hasAttribute('compound-type'))) {
			Carp::croak("Column $columnName does not use a compound type, but it was declared");
		} elsif(!defined($refItemType->[BP::Model::ColumnType::DATATYPEMANGLER]) && scalar(@compChildren)==0 && !$colType->hasAttribute('compound-type')) {
			Carp::croak("Column $columnName uses a compound type, but it was not declared");
		}
		
		# Second, setting the restrictions
		my $restriction = undef;
		if(scalar(@compChildren)>0) {
			$restriction = BP::Model::CompoundType->parseCompoundType($compChildren[0],$model);
		} elsif($colType->hasAttribute('compound-type')) {
			my $compoundType = $model->getCompoundType($colType->getAttribute('compound-type'));
			if(defined($compoundType)) {
				$restriction = $compoundType;
			} else {
				Carp::croak("Column $columnName tried to use undeclared compound type ".$colType->getAttribute('compound-type'));
			}
		} else {
			my @cvChildren = $colType->getChildrenByTagNameNS(BP::Model::dccNamespace,'cv');
			if(scalar(@cvChildren)>0) {
				$restriction = BP::Model::CV->parseCV($cvChildren[0],$model);
			} elsif($colType->hasAttribute('cv')) {
				my $namedCV = $model->getNamedCV($colType->getAttribute('cv'));
				if(defined($namedCV)) {
					$restriction = $namedCV;
				} else {
					Carp::croak("Column $columnName tried to use undeclared CV ".$colType->getAttribute('cv'));
				}
			} else {
				my @patChildren = $colType->getChildrenByTagNameNS(BP::Model::dccNamespace,'pattern');
				if(scalar(@patChildren)>0) {
					$restriction = BP::Model::__parse_pattern($patChildren[0]);
				} elsif($colType->hasAttribute('pattern')) {
					my $PAT = $model->getNamedPattern($colType->getAttribute('pattern'));
					if(defined($PAT)) {
						$restriction = $PAT;
					} else {
						Carp::croak("Column $columnName tried to use undeclared pattern ".$colType->getAttribute('pattern'));
					}
				}
			}
		}
		$columnType[BP::Model::ColumnType::RESTRICTION] = $restriction;
		
		
		# Setting up the data checker
		my $dataChecker = \&__true;
		
		if(defined($restriction) && ref($restriction)) {
			# We are covering here both compound type checks and CV checks
			if($restriction->can('dataChecker')) {
				$dataChecker = $restriction->dataChecker;
			} elsif(ref($restriction) eq 'Regexp') {
				$dataChecker = sub { $_[0] =~ $restriction };
			}
		} else {
			# Simple type checks
			$dataChecker = $refItemType->[BP::Model::ColumnType::DATATYPECHECKER]  if(defined($refItemType->[BP::Model::ColumnType::DATATYPECHECKER]));
		}
		
		# Setting up the data mangler
		my $dataMangler = (defined($restriction) && $restriction->can('dataMangler')) ? $restriction->dataMangler : $refItemType->[BP::Model::ColumnType::DATATYPEMANGLER];
		
		# Default value
		my $defval = $colType->hasAttribute('default')?$colType->getAttribute('default'):undef;
		# Default values must be rechecked once all the columns are available
		$columnType[BP::Model::ColumnType::DEFAULT] = (defined($defval) && substr($defval,0,2) eq '$$') ? \substr($defval,2): $defval;
		
		# Array separators
		$columnType[BP::Model::ColumnType::ARRAYSEPS] = undef;
		if($colType->hasAttribute('array-seps')) {
			my $arraySeps = $colType->getAttribute('array-seps');
			if(length($arraySeps) > 0) {
				my %sepVal = ();
				my @seps = split(//,$arraySeps);
				foreach my $sep (@seps) {
					if(exists($sepVal{$sep})) {
						Carp::croak("Column $columnName has repeated the array separator $sep!")
					}
					
					$sepVal{$sep}=undef;
				}
				$columnType[BP::Model::ColumnType::ARRAYSEPS] = $arraySeps;
				
				# Altering the data mangler in order to handle multidimensional matrices
				my $itemDataMangler = $dataMangler;
				$dataMangler = sub {
					my @splittedVal = ($_[0]);
					
					my $result = [$_[0]];
					my @frags = ($result);
					my $countdown = $#seps;
					foreach my $sep (@seps) {
						my @newFrags = ();
						foreach my $frag (@frags) {
							foreach my $value (@{$frag}) {
								my(@newVals)=split($sep,$value);
								if($countdown==0) {
									# Last step, so data mangling!!!!
									foreach my $newVal (@newVals) {
										$newVal = $itemDataMangler->($newVal);
									}
								}
								my $newFrag = \@newVals;
								
								$value = $newFrag;
								push(@newFrags,$newFrag);
							}
						}
						if($countdown>0) {
							@frags = @newFrags;
							$countdown--;
						}
					}
					
					# The real result is here
					return $result->[0];
				};
				
				# Altering the data checker in order to handle multidimensional matrices
				my $itemDataChecker = $dataChecker;
				$dataChecker = sub {
					my @splittedVal = ($_[0]);
					
					my $result = [$_[0]];
					my @frags = ($result);
					my $countdown = $#seps;
					foreach my $sep (@seps) {
						my @newFrags = ();
						foreach my $frag (@frags) {
							foreach my $value (@{$frag}) {
								my(@newVals)=split($sep,$value);
								if($countdown==0) {
									# Last step, so data mangling!!!!
									foreach my $newVal (@newVals) {
										return undef  unless($itemDataChecker->($newVal));
									}
								}
								my $newFrag = \@newVals;
								
								$value = $newFrag;
								push(@newFrags,$newFrag);
							}
						}
						if($countdown>0) {
							@frags = @newFrags;
							$countdown--;
						}
					}
					
					# The real result is here
					return 1;
				};
			}
		}
		
		# And now, the data mangler and checker
		$columnType[BP::Model::ColumnType::DATAMANGLER] = $dataMangler;
		$columnType[BP::Model::ColumnType::DATACHECKER] = $dataChecker;
		
		last;
	}
	
	return bless(\@columnType,$class);
}

# Item type
sub type {
	return $_[0]->[BP::Model::ColumnType::TYPE];
}

# column use (idref, required, optional)
# Idref equals 0; required, 1; optional, -1
sub use {
	return $_[0]->[BP::Model::ColumnType::USE];
}

# content restrictions. Either
# BP::Model::CompoundType
# BP::Model::CV
# Pattern
sub restriction {
	return $_[0]->[BP::Model::ColumnType::RESTRICTION];
}

# default value
sub default {
	return $_[0]->[BP::Model::ColumnType::DEFAULT];
}

# array separators
sub arraySeps {
	return $_[0]->[BP::Model::ColumnType::ARRAYSEPS];
}

# An array of allowed null values
sub allowedNulls {
	return $_[0]->[BP::Model::ColumnType::ALLOWEDNULLS];
}

# A subroutine for data mangling from tabular file
sub dataMangler {
	return $_[0]->[BP::Model::ColumnType::DATAMANGLER];
}

# A subroutine for data checking from tabular file
sub dataChecker {
	return $_[0]->[BP::Model::ColumnType::DATACHECKER];
}

sub isValid($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $val = shift;
	
	return $self->dataChecker->($val);
}

sub setDefault($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $val = shift;
	
	$self->[BP::Model::ColumnType::DEFAULT] = $val;
}

# clone parameters:
#	relatedConcept: optional BP::Model::RelatedConcept instance, it signals whether to change cloned columnType
#		according to relatedConcept hints
# it returns a BP::Model::ColumnType instance
sub clone(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $relatedConcept = shift;
	
	Carp::croak('Input parameter must be a BP::Model::RelatedConcept')  if(defined($relatedConcept) && (ref($relatedConcept) eq '' || !$relatedConcept->isa('BP::Model::RelatedConcept')));
	
	# Cloning this object
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	if(defined($relatedConcept)) {
		if($relatedConcept->isPartial) {
			$retval->[BP::Model::ColumnType::USE] = BP::Model::ColumnType::DESIRABLE;
		}
		
		if($relatedConcept->arity eq 'M') {
			my $sep = $relatedConcept->mArySeparator;
			if(defined($retval->[BP::Model::ColumnType::ARRAYSEPS]) && index($retval->[BP::Model::ColumnType::ARRAYSEPS],$sep)!=-1) {
				Carp::croak("Cloned column has repeated the array separator $sep!");
			}
			
			if(defined($retval->[BP::Model::ColumnType::ARRAYSEPS])) {
				$retval->[BP::Model::ColumnType::ARRAYSEPS] = $sep . $retval->[BP::Model::ColumnType::ARRAYSEPS];
			} else {
				$retval->[BP::Model::ColumnType::ARRAYSEPS] = $sep;
			}
		}
	}
	
	return $retval;
}

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonColumnType = (
		'type'	=> $self->type,
		'use'	=> $self->use,
		'isArray'	=> defined($self->arraySeps) ? JSON::true : JSON::false,
	);
	
	if(defined($self->default)) {
		if(ref($self->default)) {
			$jsonColumnType{'defaultCol'} = $self->default->name;
		} else {
			$jsonColumnType{'default'} = $self->default;
		}
	}
	
	if(defined($self->restriction)) {
		if($self->restriction->isa('BP::Model::CV')) {
			$jsonColumnType{'cv'} = $self->restriction->_jsonId;
		} elsif($self->restriction->isa('BP::Model::CompoundType')) {
			$jsonColumnType{'columns'} = $self->restriction->columnSet->columns;
		} elsif($self->restriction->isa('Pattern')) {
			$jsonColumnType{'pattern'} = $self->restriction;
		}
	}
	
	return \%jsonColumnType;
}

1;


package BP::Model::Column;

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

# This is the constructor.
# parseColumn parameters:
#	colDecl: a XML::LibXML::Element 'dcc:column' node, which defines
#		a column
#	model: a BP::Model instance, used to validate
# returns a BP::Model::Column instance, with all the information related to
# types, restrictions and enumerated values used by this column.
sub parseColumn($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $colDecl = shift;
	my $model = shift;
	
	# Column name, description, annotations, column type, is masked, related concept, related column from the concept
	my @column = (
		$colDecl->getAttribute('name'),
		BP::Model::DescriptionSet->parseDescriptions($colDecl),
		BP::Model::AnnotationSet->parseAnnotations($colDecl),
		BP::Model::ColumnType->parseColumnType($colDecl,$model,$colDecl->getAttribute('name')),
		undef,
		undef,
		undef,
		undef
	);
	
	return bless(\@column,$class);
}

# The column name
sub name {
	return $_[0]->[BP::Model::Column::NAME];
}

# The description, a BP::Model::DescriptionSet instance
sub description {
	return $_[0]->[BP::Model::Column::DESCRIPTION];
}

# Annotations, a BP::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[BP::Model::Column::ANNOTATIONS];
}

# It returns a BP::Model::ColumnType instance
sub columnType {
	return $_[0]->[BP::Model::Column::COLUMNTYPE];
}

# If this column is masked (because it is a inherited idref on a concept hosted in a hash)
# it will return true, otherwise undef
sub isMasked {
	return $_[0]->[BP::Model::Column::ISMASKED];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a BP::Model::Concept instance
# Otherwise, it will return undef
sub refConcept {
	return $_[0]->[BP::Model::Column::REFCONCEPT];
}

# If this column is part of a foreign key pointing
# to a concept, this method will return a BP::Model::Column instance
# which correlates to
# Otherwise, it will return undef
sub refColumn {
	return $_[0]->[BP::Model::Column::REFCOLUMN];
}

# If this column is part of a foreign key pointing
# to a concept using related-to, this method will return a BP::Model::RelatedConcept
# instance which correlates to
# Otherwise, it will return undef
sub relatedConcept {
	return $_[0]->[BP::Model::Column::RELATED_CONCEPT];
}

# clone parameters:
#	doMask: optional, it signals whether to mark cloned column
#		as masked, so it should not be considered for value storage in the database.
# it returns a BP::Model::Column instance
sub clone(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $doMask = shift;
	
	# Cloning this object
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	# Cloning the description and the annotations
	$retval->[BP::Model::Column::DESCRIPTION] = $self->description->clone;
	$retval->[BP::Model::Column::ANNOTATIONS] = $self->annotations->clone;
	
	$retval->[BP::Model::Column::ISMASKED] = ($doMask)?1:undef;
	
	return $retval;
}

# cloneRelated parameters:
#	refConcept: A BP::Model::Concept instance, which this column is related to.
#		The kind of relation could be inheritance, or 1:N
#	relatedConcept: optional, BP::Model::RelatedConcept, which contains the prefix to be set to the name when the column is cloned
#	doMask: optional, it signals whether to mark cloned column
#		as masked, so it should not be considered for value storage in the database.
#	weakAnnotations: optional, BP::Model::AnnotationSet
# it returns a BP::Model::Column instance
sub cloneRelated($;$$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $refConcept = shift;
	my $relatedConcept = shift;
	my $prefix = defined($relatedConcept)?$relatedConcept->keyPrefix:undef;
	my $doMask = shift;
	my $weakAnnotations = shift;
	
	# Cloning this object
	my $retval = $self->clone($doMask);
	
	# Adding the prefix
	$retval->[BP::Model::Column::NAME] = $prefix.$retval->[BP::Model::Column::NAME]  if(defined($prefix) && length($prefix)>0);
	
	# Adding the annotations from the related concept
	$retval->annotations->addAnnotations($relatedConcept->annotations)  if(defined($relatedConcept));
	# And from the weak-concepts annotations
	$retval->annotations->addAnnotations($weakAnnotations)  if(defined($weakAnnotations));
	
	# And adding the relation info
	# to this column
	$retval->[BP::Model::Column::REFCONCEPT] = $refConcept;
	$retval->[BP::Model::Column::REFCOLUMN] = $self;
	$retval->[BP::Model::Column::RELATED_CONCEPT] = $relatedConcept;
	
	# Does this column become optional due the participation?
	# Does this column become an array due the arity?
	if(defined($relatedConcept) && ($relatedConcept->isPartial || $relatedConcept->arity eq 'M')) {
		# First, let's clone the concept type, to avoid side effects
		$retval->[BP::Model::Column::COLUMNTYPE] = $self->columnType->clone($relatedConcept);
	}
	
	return $retval;
}

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonColumn = (
		'name'	=> $self->name,
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		'restrictions'	=> $self->columnType
	);
	
	$jsonColumn{'refers'} = join('.',$self->refConcept->conceptDomain->name, $self->refConcept->name, $self->refColumn->name)  if(defined($self->refColumn));
	
	return \%jsonColumn;
}

1;


package BP::Model::ColumnSet;

# This is the constructor.
# parseColumnSet parameters:
#	container: a XML::LibXML::Element node, containing 'dcc:column' elements
#	parentColumnSet: a BP::Model::ColumnSet instance, which is the parent.
#	model: a BP::Model instance, used to validate the columns.
# returns a BP::Model::ColumnSet instance with all the BP::Model::Column instances (including
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
	# Hash of BP::Model::Column instances
	my @columnSet = (\@idColumnNames,\@columnNames,\%columnDecl);
	
	my @checkDefault = ();
	foreach my $colDecl ($container->childNodes()) {
		next  unless($colDecl->nodeType == XML::LibXML::XML_ELEMENT_NODE && $colDecl->localname() eq 'column');
		
		my $column = BP::Model::Column->parseColumn($colDecl,$model);
		
		# We want to keep the original column order as far as possible
		if(exists($columnDecl{$column->name})) {
			if($columnDecl{$column->name}->columnType->use eq BP::Model::ColumnType::IDREF) {
				Carp::croak('It is not allowed to redefine column '.$column->name.'. It is an idref one!');
			}
		} else {
			push(@columnNames,$column->name);
			# Is it a id column?
			push(@idColumnNames,$column->name)  if($column->columnType->use eq BP::Model::ColumnType::IDREF);
		}
		
		$columnDecl{$column->name}=$column;
		push(@checkDefault,$column)  if(defined($column->columnType->default) && ref($column->columnType->default));
	}
	
	# And now, second pass, where we check the consistency of default values
	foreach my $column (@checkDefault) {
		if(exists($columnDecl{${$column->columnType->default}})) {
			my $defColumn = $columnDecl{${$column->columnType->default}};
			
			if($defColumn->columnType->use >= BP::Model::ColumnType::IDREF) {
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

# This is a constructor
# combineColumnSets parameters:
#	dontCroak: A flag which must be set to true when we want to skip
#		idref column redefinitions, instead of complaining about
#	columnSets: An array of BP::Model::ColumnSet instances
# It returns a columnSet which is the combination of the input ones. The
# order of the columns is preserved the most.
# It is not allowed to override IDREF columns
sub combineColumnSets($@) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my($dontCroak,$firstColumnSet,@columnSets) = @_;
	
	# First column set is the seed
	my @columnNames = @{$firstColumnSet->columnNames};
	my @idColumnNames = @{$firstColumnSet->idColumnNames};
	my %columnDecl = %{$firstColumnSet->columns};
	
	# Array with the idref column names
	# Array with column names (all)
	# Hash of BP::Model::Column instances
	my @columnSet = (\@idColumnNames,\@columnNames,\%columnDecl);
	
	# And now, the next ones!
	foreach my $columnSet (@columnSets) {
		my $p_columns = $columnSet->columns;
		foreach my $columnName (@{$columnSet->columnNames}) {
			# We want to keep the original column order as far as possible
			if(exists($columnDecl{$columnName})) {
				# Same column, so skip!
				next  if($columnDecl{$columnName} == $p_columns->{$columnName});
				if($columnDecl{$columnName}->columnType->use eq BP::Model::ColumnType::IDREF) {
					next  if($dontCroak);
					Carp::croak('It is not allowed to redefine column '.$columnName.'. It is an idref one!');
				}
			} else {
				push(@columnNames,$columnName);
				# Is it a id column?
				push(@idColumnNames,$columnName)  if($p_columns->{$columnName}->columnType->use eq BP::Model::ColumnType::IDREF);
			}
			$columnDecl{$columnName} = $p_columns->{$columnName};
		}
	}
	
	return bless(\@columnSet,$class);
}

# Cloning facility
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	# A cheap way to clone itself
	return ref($self)->combineColumnSets(1,$self);
}

# Reference to an array with the idref column names
sub idColumnNames {
	return $_[0]->[0];
}

# Array with column names (all)
sub columnNames {
	return $_[0]->[1];
}

# Hash of BP::Model::Column instances
sub columns {
	return $_[0]->[2];
}

# idColumns parameters:
#	idConcept: The BP::Model::Concept instance owning the id columns
#	doMask: Are the columns masked for storage?
#	weakAnnotations: BP::Model::AnnotationSet from weak-concepts.
# It returns a BP::Model::ColumnSet instance, with the column declarations
# corresponding to columns with idref restriction
sub idColumns($;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $idConcept = shift;
	my $doMask = shift;
	my $weakAnnotations = shift;
	
	my @columnNames = @{$self->idColumnNames};
	my $p_columns = $self->columns;
	my %columns = map { $_ => $p_columns->{$_}->cloneRelated($idConcept,undef,$doMask,$weakAnnotations) } @columnNames;
	
	my @columnSet = (
		\@columnNames,
		\@columnNames,
		\%columns
	);
	
	return bless(\@columnSet,ref($self));
}

# relatedColumns parameters:
#	myConcept: A BP::Model::Concept instance, which this columnSet belongs
#		The kind of relation could be inheritance, or 1:N
#	relatedConcept: A BP::Model::RelatedConcept instance
#		(which contains the optional prefix to be set to the name when the columns are cloned)
# It returns a BP::Model::ColumnSet instance, with the column declarations
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
#	inputColumnSet: A BP::Model::ColumnSet instance which contains
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
			if($doAddIDREF && $inputColumn->columnType->use eq BP::Model::ColumnType::IDREF) {
				push(@{$p_idColumnNames},$inputColumnName);
			}
		}
		
		$p_columnsHash->{$inputColumnName} = $inputColumn;
	}
}

# resolveDefaultCalculatedValues parameters:
#	(none)
# the method resolves the references of default values to other columns
sub resolveDefaultCalculatedValues() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_columns = $self->columns;
	foreach my $column (values(%{$p_columns})) {
		if(defined($column->columnType->default) && ref($column->columnType->default) eq 'SCALAR') {
			my $defCalColumnName = ${$column->columnType->default};
			
			if(exists($p_columns->{$defCalColumnName})) {
				$column->columnType->setCalculatedDefault($p_columns->{$defCalColumnName});
			} else {
				Carp::croak('Unknown column '.$defCalColumnName.' use for default value of column '.$column->name);
			}
		}
	}
}

1;


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
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'BP::Model::SimpleType') {
			my $typeObject = $model->getItemType($2);
			if(defined($typeObject)) {
				my $type = $typeObject->[BP::Model::ColumnType::TYPEPATTERN];
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
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'BP::Model::CV') {
			my $CV = $model->getNamedCV($2);
			if(defined($CV)) {
				$pattern .= '(.+)';
				
				# Check the value against the CV
				push(@parts,$CV);
			} else {
				Carp::croak("Unknown controlled vocabulary '$2' used in filename-format '$formatString'");
			}
		} elsif(FileTypeSymbolPrefixes->{$1} eq 'BP::Model::Annotation') {
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
		if($part->isa('BP::Model::CV')) {
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $conceptDomain = shift;
	
	# The concept domain is registered, then
	$self->registeredConceptDomains->{$conceptDomain->name} = $conceptDomain;
}

1;


package BP::Model::ConceptDomain;

# This is the constructor.
# parseConceptDomain parameters:
#	conceptDomainDecl: a XML::LibXML::Element 'dcc:concept-domain' element
#	model: a BP::Model instance used to validate the concepts, columsn, etc...
# it returns a BP::Model::ConceptDomain instance, with all the concept domain
# structures and data
sub parseConceptDomain($$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(ref($class));
	
	my $conceptDomainDecl = shift;
	my $model = shift;
	
	# concept domain name
	# full name of the concept domain
	# Filename Pattern for the filenames
	# An array with the concepts under this concept domain umbrella
	# the concept hash
	# The is abstract flag
	# The descriptions
	# The annotations
	my @concepts = ();
	my %conceptHash = ();
	my @conceptDomain = (
		$conceptDomainDecl->getAttribute('domain'),
		$conceptDomainDecl->getAttribute('fullname'),
		undef,
		\@concepts,
		\%conceptHash,
		($conceptDomainDecl->hasAttribute('is-abstract') && ($conceptDomainDecl->getAttribute('is-abstract') eq 'true'))?1:undef,
		BP::Model::DescriptionSet->parseDescriptions($conceptDomainDecl),
		BP::Model::AnnotationSet->parseAnnotations($conceptDomainDecl),
	);
	
	# Does the filename-pattern exist?
	my $filenameFormatName = $conceptDomainDecl->getAttribute('filename-format');
		
	my $fpattern = $model->getFilenamePattern($filenameFormatName);
	unless(defined($fpattern)) {
		Carp::croak("Concept domain $conceptDomain[0] uses the unknown filename format $filenameFormatName");
	}
	
	$conceptDomain[2] = $fpattern;
	
	# Last, chicken and egg problem, part 1
	my $retConceptDomain = bless(\@conceptDomain,$class);

	# And now, next method handles parsing of embedded concepts
	# It must register the concepts in the concept domain as soon as possible
	BP::Model::Concept::ParseConceptContainer($conceptDomainDecl,$retConceptDomain,$model);
	
	# Last, chicken and egg problem, part 2
	# This step must be delayed, because we need a enumeration of all the concepts
	# just at this point
	$retConceptDomain->filenamePattern->registerConceptDomain($retConceptDomain);
	
	return $retConceptDomain;
}

# concept domain name
sub name {
	return $_[0]->[0];
}

# full name of the concept domain
sub fullname {
	return $_[0]->[1];
}

# Filename Pattern for the filenames
# A BP::Model::FilenamePattern instance
sub filenamePattern {
	return $_[0]->[2];
}

# An array with the concepts under this concept domain umbrella
# An array of BP::Model::Concept instances
sub concepts {
	return $_[0]->[3];
}

# A hash with the concepts under this concept domain umbrella
# A hash of BP::Model::Concept instances
sub conceptHash {
	return $_[0]->[4];
}

# It returns 1 or undef, so it tells whether the whole concept domain is abstract or not
sub isAbstract {
	return $_[0]->[5];
}

# An instance of a BP::Model::DescriptionSet, holding the documentation
# for this Conceptdomain
sub description {
	return $_[0]->[6];
}

# A BP::Model::AnnotationSet instance, with all the annotations
sub annotations {
	return $_[0]->[7];
}

# registerConcept parameters:
#	concept: a BP::Model::Concept instance, which is going to be registered in the concept domain
sub registerConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $concept = shift;
	
	push(@{$self->concepts},$concept);
	$self->conceptHash->{$concept->name} = $concept;
}

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my %jsonConceptDomain = (
		'_id'	=> $self->name,
		'name'	=> $self->name,
		'fullname'	=> $self->fullname,
		'isAbstract'	=> $self-> isAbstract ? JSON::true : JSON::false,
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		# 'filenamePattern'
		'concepts'	=> $self->conceptHash
	);
	
	return \%jsonConceptDomain;
}

1;


package BP::Model::Concept;

# Prototypes of static methods
sub ParseConceptContainer($$$;$);

# Static method
# ParseConceptContainer parameters:
#	conceptContainerDecl: A XML::LibXML::Element 'dcc:concept-domain'
#		or 'dcc:weak-concepts' instance
#	conceptDomain: A BP::Model::ConceptDomain instance, where this concept
#		has been defined.
#	model: a BP::Model instance used to validate the concepts, columsn, etc...
#	idConcept: An optional, identifying BP::Model::Concept instance of
#		all the (weak) concepts to be parsed from the container
# it returns an array of BP::Model::Concept instances, which are all the
# concepts and weak concepts inside the input concept container
sub ParseConceptContainer($$$;$) {
	my $conceptContainerDecl = shift;
	my $conceptDomain = shift;
	my $model = shift;
	my $idConcept = shift;	# This is optional (remember!)
	
	# Let's get the annotations inside the concept container
	my $weakAnnotations = BP::Model::AnnotationSet->parseAnnotations($conceptContainerDecl);
	foreach my $conceptDecl ($conceptContainerDecl->getChildrenByTagNameNS(BP::Model::dccNamespace,'concept')) {
		# Concepts self register on the concept domain!
		my $concept = BP::Model::Concept->parseConcept($conceptDecl,$conceptDomain,$model,$idConcept,$weakAnnotations);
		
		# There should be only one!
		foreach my $weakContainerDecl ($conceptDecl->getChildrenByTagNameNS(BP::Model::dccNamespace,'weak-concepts')) {
			BP::Model::Concept::ParseConceptContainer($weakContainerDecl,$conceptDomain,$model,$concept);
			last;
		}
	}
}

# This is the constructor
# parseConcept paramereters:
#	conceptDecl: A XML::LibXML::Element 'dcc:concept' instance
#	conceptDomain: A BP::Model::ConceptDomain instance, where this concept
#		has been defined.
#	model: a BP::Model instance used to validate the concepts, columsn, etc...
#	idConcept: An optional, identifying BP::Model::Concept instance of
#		the concept to be parsed from conceptDecl
#	weakAnnotations: The weak annotations for the columns of the identifying concept
# it returns an array of BP::Model::Concept instances, the first one
# corresponds to this concept, and the other ones are the weak-concepts
sub parseConcept($$$;$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $conceptDecl = shift;
	my $conceptDomain = shift;
	my $model = shift;
	my $idConcept = shift;	# This is optional (remember!)
	my $weakAnnotations = shift;	# This is also optional (remember!)

	my $conceptName = $conceptDecl->getAttribute('name');
	my $conceptFullname = $conceptDecl->getAttribute('fullname');
	
	my $parentConceptDomainName = undef;
	my $parentConceptName = undef;
	my $parentConceptDomain = undef;
	my $parentConcept = undef;
	
	# There must be at most one
	foreach my $baseConcept ($conceptDecl->getChildrenByTagNameNS(BP::Model::dccNamespace,'extends')) {
		$parentConceptDomainName = $baseConcept->hasAttribute('domain')?$baseConcept->getAttribute('domain'):undef;
		$parentConceptName = $baseConcept->getAttribute('concept');
		
		$parentConceptDomain = $conceptDomain;
		if(defined($parentConceptDomainName)) {
			$parentConceptDomain = $model->getConceptDomain($parentConceptDomainName);
			Carp::croak("Concept domain $parentConceptDomainName with concept $parentConceptName does not exist!")  unless(defined($parentConceptDomain));
		} else {
			# Fallback name
			$parentConceptDomainName = $parentConceptDomain->name;
		}
		
		Carp::croak("Concept $parentConceptName does not exist in concept domain ".$parentConceptDomainName)  unless(exists($parentConceptDomain->conceptHash->{$parentConceptName}));
		$parentConcept = $parentConceptDomain->conceptHash->{$parentConceptName};
		last;
	}
	
	my @conceptBaseTypes = ();
	push(@conceptBaseTypes,@{$parentConcept->baseConceptTypes})  if(defined($parentConcept));
	
	# Now, let's get the base concept types
	my @baseConceptTypesDecl = $conceptDecl->getChildrenByTagNameNS(BP::Model::dccNamespace,'base-concept-type');
	Carp::croak("Concept $conceptFullname ($conceptName) has no base type (no dcc:base-concept-type)!")  if(scalar(@baseConceptTypesDecl)==0 && !defined($parentConcept));

	my @basetypes = ();
	foreach my $baseConceptTypeDecl (@baseConceptTypesDecl) {
		my $basetypeName = $baseConceptTypeDecl->getAttribute('name');
		my $basetype = $model->getConceptType($basetypeName);
		Carp::croak("Concept $conceptFullname ($conceptName) is based on undefined base type $basetypeName")  unless(defined($basetype));
		
		foreach my $conceptBase (@conceptBaseTypes) {
			if($conceptBase eq $basetype) {
				$basetype = undef;
				last;
			}
		}
		
		# Only saving new basetypes, skipping old ones to avoid strange effects
		if(defined($basetype)) {
			push(@basetypes,$basetype);
			push(@conceptBaseTypes,$basetype);
		}
	}
	# First, let's process the basetypes columnSets
	my $baseColumnSet = (scalar(@basetypes) > 1)?BP::Model::ColumnSet->combineColumnSets(1,map { $_->columnSet } @basetypes):((scalar(@basetypes) == 1)?$basetypes[0]->columnSet:undef);
	
	# And now, let's process the inherited columnSets, along with the basetypes ones
	if(defined($baseColumnSet)) {
		# Let's combine!
		if(defined($parentConcept)) {
			# Restrictive mode, with croaks
			$baseColumnSet = BP::Model::ColumnSet->combineColumnSets(undef,$parentConcept->columnSet,$baseColumnSet);
		}
	} elsif(defined($parentConcept)) {
		# Should we clone this, to avoid potentian side effects?
		$baseColumnSet = $parentConcept->columnSet;
	} else {
		# This shouldn't happen!!
		Carp::croak("No concept types and no parent concept for $conceptFullname ($conceptName)");
	}
	
	# Preparing the columns
	my $columnSet = BP::Model::ColumnSet->parseColumnSet($conceptDecl,$baseColumnSet,$model);
	
	# Adding the ones from the identifying concept
	# (and later from the related stuff)
	$columnSet->addColumns($idConcept->idColumns(! $idConcept->goesToCollection,$weakAnnotations),1)  if(defined($idConcept));
	
	# This array will contain the names of the related concepts
	my @related = ();
	
	# This hash will contain the named related concepts
	my %relPos = ();
	
	# Let's resolve inherited relations topic
	if(defined($parentConcept)) {
		# First, cloning
		@related = map { $_->clone() } @{$parentConcept->relatedConcepts};
		
		# Second, reparenting the RelatedConcept objects
		# Third, updating inheritable related concepts
		my $parentDomainChanges = $parentConceptDomain ne $conceptDomain;
		my $pos = 0;
		foreach my $relatedConcept (@related) {
			# Setting the name of the concept domain, in the cases where it was relative
			$relatedConcept->setConceptDomainName($parentConceptDomainName)  if($parentDomainChanges && !defined($relatedConcept->conceptDomainName));
			
			# Resetting the name of the related concept
			my $relatedConceptDomainName = defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$parentConceptDomainName;
			if($relatedConcept->isInheritable && $relatedConcept->conceptName eq $parentConceptName && $relatedConceptDomainName eq $parentConceptDomainName) {
				$relatedConcept->setConceptName($conceptName);
			}
			
			# Registering it (if it could be substituted)
			$relPos{$relatedConcept->id} = $pos  if(defined($relatedConcept->id));
			$pos ++;
		}
		
	}
	
	# Saving the related concepts (the ones explicitly declared within this concept)
	foreach my $relatedDecl ($conceptDecl->getChildrenByTagNameNS(BP::Model::dccNamespace,'related-to')) {
		my $parsedRelatedConcept = BP::Model::RelatedConcept->parseRelatedConcept($relatedDecl);
		if(defined($parsedRelatedConcept->id) && exists($relPos{$parsedRelatedConcept->id})) {
			# TODO Validation of a legal substitution
			$related[$relPos{$parsedRelatedConcept->id}] = $parsedRelatedConcept;
		} else {
			push(@related,$parsedRelatedConcept);
		}
	}
	
	# Let's resolve the default values which depend on the context (i.e. the value of other columns)
	$columnSet->resolveDefaultCalculatedValues();
	
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
		\@conceptBaseTypes,
		$conceptDomain,
		BP::Model::DescriptionSet->parseDescriptions($conceptDecl),
		BP::Model::AnnotationSet->parseAnnotations($conceptDecl,defined($parentConcept)?$parentConcept->annotations:undef),
		$columnSet,
		$idConcept,
		\@related,
		$parentConcept
	);
	
	my $me = bless(\@thisConcept,$class);
	
	# Registering on our concept domain
	$conceptDomain->registerConcept($me);
	
	# The weak concepts must be processed outside (this constructor does not mind them)
	return  $me;
}

# name
sub name {
	return $_[0]->[0];
}

# fullname
sub fullname {
	return $_[0]->[1];
}

# A reference to the array of BP::Model::ConceptType instances basetypes
sub baseConceptTypes {
	return $_[0]->[2];
}

# The BP::Model::ConceptType instance basetype is the first element of the array
# It returns undef if there is no one
sub baseConceptType {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $p_arr = $self->baseConceptTypes;
	return (scalar(@{$p_arr})>0)?$p_arr->[0]:undef;
}

# It tells whether the concept data should go to a collection or not
sub goesToCollection {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $baseConceptType = $self->baseConceptType;
	return defined($baseConceptType) ? $baseConceptType->goesToCollection : undef;
}

# If goesToCollection is true, it returns a BP::Model::Collection instance
sub collection {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $baseConceptType = $self->baseConceptType;
	return (defined($baseConceptType) && $baseConceptType->goesToCollection) ? $baseConceptType->collection : undef;
}

# If goesToCollection is undef, it returns a string with the key
sub key {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $baseConceptType = $self->baseConceptType;
	return (defined($baseConceptType) && !$baseConceptType->goesToCollection) ? $baseConceptType->key : undef;
}

# The BP::Model::ConceptDomain instance where this concept is defined
sub conceptDomain {
	return $_[0]->[3];
}

# A BP::Model::DescriptionSet instance, with all the descriptions
sub description {
	return $_[0]->[4];
}

# A BP::Model::AnnotationSet instance, with all the annotations
sub annotations {
	return $_[0]->[5];
}

# A BP::Model::ColumnSet instance with all the columns (including the inherited ones) of this concept
sub columnSet {
	return $_[0]->[6];
}

# A BP::Model::Concept instance, which represents the identifying concept of this one
sub idConcept {
	return $_[0]->[7];
}

# related conceptNames, an array of BP::Model::RelatedConcept (trios concept domain name, concept name, prefix)
sub relatedConcepts {
	return $_[0]->[8];
}

# A BP::Model::Concept instance, which represents the concept which has been extended
sub parentConcept {
	return $_[0]->[9];
}

# refColumns parameters:
#	relatedConcept: The BP::Model::RelatedConcept instance which rules this (with the optional prefix to put on cloned idref columns)
# It returns a BP::Model::ColumnSet instance with clones of all the idref columns
# referring to this object and with a possible prefix.
sub refColumns($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $relatedConcept = shift;
	
	return $self->columnSet->relatedColumns($self,$relatedConcept);
}

# idColumns parameters:
#	doMask: Are the columns masked for storage?
#	weakAnnotations: BP::Model::AnnotationSet from weak-concepts
sub idColumns(;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $doMask = shift;
	my $weakAnnotations = shift;
	
	return $self->columnSet->idColumns($self,$doMask,$weakAnnotations);
}

sub _jsonId() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $id = join('.',$self->conceptDomain->name, $self->name);
	
	return $id;
}

sub TO_JSON() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $id = $self->_jsonId;
	my %jsonConcept = (
		'_id'	=> $id,
		'name'	=> $self->name,
		'fullname'	=> $self->fullname,
		'description'	=> $self->description,
		'annotations'	=> $self->annotations,
		'columns'	=> $self->columnSet->columns,
		# TOBEFINISHED
	);
	
	$jsonConcept{'extends'} = $self->parentConcept->_jsonId   if(defined($self->parentConcept));
	$jsonConcept{'identifiedBy'} = $self->idConcept->_jsonId   if(defined($self->idConcept));
	if(scalar(@{$self->relatedConcepts})>0) {
		my %relT = map { $_->concept->_jsonId => undef } @{$self->relatedConcepts};
		$jsonConcept{'relatedTo'} = [ keys(%relT) ];
	}
	
	# Now, giving absolute _id to the columns
	#foreach my $val (values(%{$jsonConcept{'columns'}})) {
	#	$val->{'_id'} = join('.',$id,$val->name);
	#}
	
	return \%jsonConcept;
}

1;


package BP::Model::RelatedConcept;

# This is the constructor.
# parseRelatedConcept parameters:
#	relatedDecl: A XML::LibXML::Element 'dcc:related-concept' instance
# It returns a BP::Model::RelatedConcept instance
sub parseRelatedConcept($) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	my $relatedDecl = shift;
	
	return bless([
		($relatedDecl->hasAttribute('domain'))?$relatedDecl->getAttribute('domain'):undef ,
		$relatedDecl->getAttribute('concept') ,
		($relatedDecl->hasAttribute('prefix'))?$relatedDecl->getAttribute('prefix'):undef ,
		undef,
		undef,
		($relatedDecl->hasAttribute('arity') && $relatedDecl->getAttribute('arity') eq 'M')?'M':1,
		($relatedDecl->hasAttribute('m-ary-sep'))?$relatedDecl->getAttribute('m-ary-sep'):',',
		($relatedDecl->hasAttribute('partial-participation') && $relatedDecl->getAttribute('partial-participation') eq 'true')?1:undef,
		($relatedDecl->hasAttribute('inheritable') && $relatedDecl->getAttribute('inheritable') eq 'true')?1:undef,
		BP::Model::AnnotationSet->parseAnnotations($relatedDecl),
		$relatedDecl->hasAttribute('id')?$relatedDecl->getAttribute('id'):undef,
	],$class);
}

sub conceptDomainName {
	return $_[0]->[0];
}

sub conceptName {
	return $_[0]->[1];
}

sub keyPrefix {
	return $_[0]->[2];
}

# It returns a BP::Model::Concept instance
sub concept {
	return $_[0]->[3];
}

# It returns a BP::Model::ColumnSet with the remote columns used for this relation
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

# It returns 1 or undef
sub isInheritable {
	return $_[0]->[8];
}

# It returns a BP::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[9];
}

# The id of this relation. If returns undef, it is an anonymous relation
sub id {
	return $_[0]->[10];
}

# setRelatedConcept parameters:
#	concept: the BP::Model::Concept instance being referenced
#	columnSet: the columns inherited from the concept, already with the key prefix
sub setRelatedConcept($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $concept = shift;
	my $columnSet = shift;
	
	Carp::croak('Parameter must be either a BP::Model::Concept or undef')  unless(!defined($concept) || (ref($concept) && $concept->isa('BP::Model::Concept')));
	Carp::croak('Parameter must be either a BP::Model::ColumnSet or undef')  unless(!defined($columnSet) || (ref($columnSet) && $columnSet->isa('BP::Model::ColumnSet')));
	
	$self->[3] = $concept;
	$self->[4] = $columnSet;
}

# clone creates a new BP::Model::RelatedConcept
sub clone() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my @cloneData = @{$self};
	my $retval = bless(\@cloneData,ref($self));
	
	return $retval;
}

#	newConceptDomainName: the new concept domain to point to
sub setConceptDomainName($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $newConceptDomainName = shift;
	
	$self->[0] = $newConceptDomainName;
}

# setConceptName parameters:
#	newConceptName: the new concept name to point to
#	newConceptDomainName: the new concept domain to point to
sub setConceptName($;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $newConceptName = shift;
	my $newConceptDomainName = shift;
	
	$self->[0] = $newConceptDomainName;
	$self->[1] = $newConceptName;
}

1;
