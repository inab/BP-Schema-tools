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

# Early subpackage constant declarations
package DCC::Model::CV;

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

# Main package
package DCC::Model;
#use version 0.77;
#our $VERSION = qv('0.2.0');

use constant DCCSchemaFilename => 'bp-schema.xsd';
use constant dccNamespace => 'http://www.blueprint-epigenome.eu/dcc/schema';

# The pattern matching the contents for this type, and whether it is not a numeric type or yes
use constant {
	TYPEPATTERN	=>	0,
	ISNOTNUMERIC	=>	1
};

use constant ItemTypes => {
	'string'	=> [1,1],	# With this we avoid costly checks
	'text'		=> [1,1],	# With this we avoid costly checks
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
		my $conceptDomain = DCC::Model::ConceptDomain->parseConceptDomain($conceptDomainDecl,$model);
		
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

1;


package DCC::Model::DescriptionSet;

# This is the empty constructor.
sub new() {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	return bless([],$class);
}

# This is the constructor.
# parseDescriptions parameters:
#	container: a XML::LibXML::Element container of 'dcc:description' elements
# returns a DCC::Model::DescriptionSet array reference, containing the contents of all
# the 'dcc:description' XML elements found
sub parseDescriptions($) {
	my $self = shift;
	
	my $container = shift;
	
	# Dual instance/class method behavior
	unless(ref($self)) {
		my $class = $self;
		$self = $class->new();
	}
	
	foreach my $description ($container->getChildrenByTagNameNS(DCC::Model::dccNamespace,'description')) {
		my @dChildren = $description->nonBlankChildNodes();
		
		my $value = undef;
		# We only save the nodeset when 
		foreach my $dChild (@dChildren) {
			next  unless($dChild->nodeType == XML::LibXML::XML_ELEMENT_NODE);
			
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

1;


package DCC::Model::AnnotationSet;

# This is the empty constructor.
#	seedAnnotationSet: an optional DCC::Model::AnnotationSet used as seed
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
#	seedAnnotationSet: an optional DCC::Model::AnnotationSet used as seed
# It returns a DCC::Model::AnnotationSet hash reference, containing the contents of all the
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
	
	foreach my $annotation ($container->getChildrenByTagNameNS(DCC::Model::dccNamespace,'annotation')) {
		unless(exists($p_hash->{$annotation->getAttribute('key')})) {
			push(@{$p_order},$annotation->getAttribute('key'));
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

# This method adds the annotations from an existing DCC::Model::AnnotationSet instance
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

1;

package DCC::Model::CV::Term;

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
#		DCC::Model::CV::Term instances where this instance can
#		find its parents.
#	doRecover: If true, it tries to recover from unknown parents,
#		removing them
#	p_visited: a reference to an array, which is the pool of
#		DCC::Model::CV::Term keys which are still being
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

# This method serializes the DCC::Model::CV instance into a OBO structure
# serialize parameters:
#	O: the output file handle
sub OBOserialize($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $O = shift;
	
	# We need this
	print $O "[Term]\n";
	DCC::Model::CV::printOboKeyVal($O,'id',$self->key);
	DCC::Model::CV::printOboKeyVal($O,'name',$self->name);
	
	# The alterative ids
	my $first = 1;
	foreach my $alt_id (@{$self->keys}) {
		# Skipping the first one, which is the main id
		if(defined($first)) {
			$first=undef;
			next;
		}
		DCC::Model::CV::printOboKeyVal($O,'alt_id',$alt_id);
	}
	if(defined($self->parents)) {
		my $propLabel = ($self->isAlias)?'union_of':'is_a';
		foreach my $parKey (@{$self->parents}) {
			DCC::Model::CV::printOboKeyVal($O,$propLabel,$parKey);
		}
	}
	print $O "\n";
}


package DCC::Model::CV::External;

# This is the constructor
# parseCVExternal parameters:
#	el: a XML::LibXML::Element 'dcc:cv-uri' node
#	model: a DCC::Model instance
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


package DCC::Model::CV;

# The CV symbolic name, the CV filename, the annotations, the documentation paragraphs, the CV (hash and keys), and the aliases (hash and keys)
use constant {
	CVNAME	=>	0,
	CVKIND	=>	1,
	CVURI	=>	2,
	CVLOCALPATH	=>	3,
	CVLOCALFORMAT	=>	4,
	CVANNOT	=>	5,
	CVDESC	=>	6,
	CVHASH	=>	7,
	CVKEYS	=>	8,
	CVALKEYS	=>	9,
	CVXMLEL		=>	10
};

# This is the empty constructor
sub new() {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	# The CV symbolic name, the CV type, the array of CV uri, the CV local filename, the CV local format, the annotations, the documentation paragraphs, the CV (hash and array), aliases (array), XML element of cv-file element
	my $cvAnnot = DCC::Model::AnnotationSet->new();
	my $cvDesc = DCC::Model::DescriptionSet->new();
	my @structCV=(undef,undef,undef,undef,undef,$cvAnnot,$cvDesc,undef,undef,[],undef);
	
	$structCV[DCC::Model::CV::CVKEYS] = [];
	# Hash shared by terms and term-aliases
	$structCV[DCC::Model::CV::CVHASH] = {};
	
	return bless(\@structCV,$class);
}

# This is the constructor and/or the parser
# parseCV parameters:
#	cv: a XML::LibXML::Element 'dcc:cv' node
#	model: a DCC::Model instance
# returns a DCC::Model::CV array reference, with all the controlled vocabulary
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
	
	$self->[DCC::Model::CV::CVNAME] = $cv->getAttribute('name')  if($cv->hasAttribute('name'));
	
	foreach my $el ($cv->childNodes()) {
		next  unless($el->nodeType == XML::LibXML::XML_ELEMENT_NODE);
		
		if($el->localname eq 'e') {
			unless(defined($self->[DCC::Model::CV::CVKIND])) {
				$self->[DCC::Model::CV::CVKIND] = ($cv->localname eq DCC::Model::CV::NULLVALUES) ? DCC::Model::CV::NULLVALUES : DCC::Model::CV::INLINE;
			}
			$self->addTerm(DCC::Model::CV::Term->new($el->getAttribute('v'),$el->textContent()));
		} elsif($el->localname eq 'cv-uri') {
			unless(defined($self->[DCC::Model::CV::CVKIND])) {
				$self->[DCC::Model::CV::CVKIND] = DCC::Model::CV::URIFETCHED;
				$self->[DCC::Model::CV::CVURI] = [];
			}
			
			# As we are not fetching the content, we are not initializing neither cvHash nor cvKeys references
			push(@{$self->[DCC::Model::CV::CVURI]},DCC::Model::CV::External->parseCVExternal($el,$self));
			
		} elsif($el->localname eq 'cv-file') {
			my $cvPath = $el->textContent();
			$cvPath = $model->sanitizeCVpath($cvPath);
			
			my $cvFormat = ($el->hasAttribute('format') && $el->getAttribute('format') eq 'obo')?DCC::Model::CV::CVFORMAT_OBO : DCC::Model::CV::CVFORMAT_CVFORMAT;
			
			# Local fetch
			$self->[DCC::Model::CV::CVKIND] = DCC::Model::CV::CVLOCAL  unless(defined($self->[DCC::Model::CV::CVKIND]));
			$self->[DCC::Model::CV::CVLOCALPATH] = $cvPath;
			$self->[DCC::Model::CV::CVLOCALFORMAT] = $cvFormat;
			# Saving it for a possible storage in a bpmodel
			$self->[DCC::Model::CV::CVXMLEL] = $el;
			
			my $CVH = $model->openCVpath($cvPath);
			if($cvFormat == DCC::Model::CV::CVFORMAT_CVFORMAT) {
				$self->__parseCVFORMAT($CVH,$model);
			} elsif($cvFormat == DCC::Model::CV::CVFORMAT_OBO) {
				$self->__parseOBO($CVH,$model);
			}
			
			$CVH->close();
			# We register the local CVs, even the local dumps of the remote CVs
			$model->registerCV($self);
		} elsif($el->localname eq 'term-alias') {
			my $alias = DCC::Model::CV::Term->parseAlias($el);
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
	return $_[0]->[DCC::Model::CV::CVNAME];
}

# The kind of CV
sub kind {
	return $_[0]->[DCC::Model::CV::CVKIND];
}

# Ref to an array of DCC::Model::CV::External, holding these values (it could be undef)
sub uri {
	return $_[0]->[DCC::Model::CV::CVURI];
}

# Filename holding these values (optional)
sub localFilename {
	return $_[0]->[DCC::Model::CV::CVLOCALPATH];
}

# Format of the filename holding these values (optional)
sub localFormat {
	return $_[0]->[DCC::Model::CV::CVLOCALFORMAT];
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

# addTerm parameters:
#	term: a DCC::Model::CV::Term instance, which can be a term or an alias
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
#	model: a DCC::Model instance, where the digestion of this file is going to be registered.
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
			$self->addTerm(DCC::Model::CV::Term->new($key,$value));
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
				$self->addTerm(DCC::Model::CV::Term->new($keys,$name,defined($parents)?$parents:$union,(defined($union) && !defined($parents))?1:undef));
				
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
	$self->addTerm(DCC::Model::CV::Term->new($keys,$name,defined($parents)?$parents:$union,(defined($union) && !defined($parents))?1:undef))  if(defined($keys));
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

# This method serializes the DCC::Model::CV instance into a OBO structure
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
	printOboKeyVal($O,'auto-generated-by','DCC::Model $Id$');
	
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

1;

package DCC::Model::ConceptType;

# Prototypes of static methods
sub parseConceptTypeLineage($$;$);

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


package DCC::Model::CompoundType;

# TODO: Compound type refactor in the near future, so work for filename patterns
# can be reused

# This is the constructor.
# new parameters:
#	template: The template string, to be processed
#	seps: The tokens which delimite the template tokens
#	columnName: The name of the column (for error messages purposes)
sub new($$$) {
	my $class = shift;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(ref($class));
	
	# tokens, separators
	my $template = shift;
	my $seps = shift;
	my $columnName = shift;
	
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
	return bless(\@compoundDecl,$class);
}

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
				$columnType[DCC::Model::ColumnType::RESTRICTION] = DCC::Model::CompoundType->new($colType->getAttribute('compound-template'),$colType->getAttribute('compound-seps'),$columnName);
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

# content restrictions. Either
# DCC::Model::CompoundType
# DCC::Model::CV
# Pattern
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
	
	# Cloning the description and the annotations
	$retval->[DCC::Model::Column::DESCRIPTION] = $self->description->clone;
	$retval->[DCC::Model::Column::ANNOTATIONS] = $self->annotations->clone;
	
	$retval->[DCC::Model::Column::ISMASKED] = ($doMask)?1:undef;
	
	return $retval;
}

# cloneRelated parameters:
#	refConcept: A DCC::Model::Concept instance, which this column is related to.
#		The kind of relation could be inheritance, or 1:N
#	relatedConcept: optional, DCC::Model::RelatedConcept, which contains the prefix to be set to the name when the column is cloned
#	doMask: optional, it signals whether to mark cloned column
#		as masked, so it should not be considered for value storage in the database.
#	weakAnnotations: optional, DCC::Model::AnnotationSet
# it returns a DCC::Model::Column instance
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
	$retval->[DCC::Model::Column::NAME] = $prefix.$retval->[DCC::Model::Column::NAME]  if(defined($prefix) && length($prefix)>0);
	
	# Adding the annotations from the related concept
	$retval->annotations->addAnnotations($relatedConcept->annotations)  if(defined($relatedConcept));
	# And from the weak-concepts annotations
	$retval->annotations->addAnnotations($weakAnnotations)  if(defined($weakAnnotations));
	
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

# This is a constructor
# combineColumnSets parameters:
#	dontCroak: A flag which must be set to true when we want to skip
#		idref column redefinitions, instead of complaining about
#	columnSets: An array of DCC::Model::ColumnSet instances
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
	# Hash of DCC::Model::Column instances
	my @columnSet = (\@idColumnNames,\@columnNames,\%columnDecl);
	
	# And now, the next ones!
	foreach my $columnSet (@columnSets) {
		my $p_columns = $columnSet->columns;
		foreach my $columnName (@{$columnSet->columnNames}) {
			# We want to keep the original column order as far as possible
			if(exists($columnDecl{$columnName})) {
				if($columnDecl{$columnName}->columnType->use eq DCC::Model::ColumnType::IDREF) {
					next  if($dontCroak);
					Carp::croak('It is not allowed to redefine column '.$columnName.'. It is an idref one!');
				}
			} else {
				push(@columnNames,$columnName);
				# Is it a id column?
				push(@idColumnNames,$columnName)  if($p_columns->{$columnName}->columnType->use eq DCC::Model::ColumnType::IDREF);
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

# Hash of DCC::Model::Column instances
sub columns {
	return $_[0]->[2];
}

# idColumns parameters:
#	idConcept: The DCC::Model::Concept instance owning the id columns
#	doMask: Are the columns masked for storage?
#	weakAnnotations: DCC::Model::AnnotationSet from weak-concepts.
# It returns a DCC::Model::ColumnSet instance, with the column declarations
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

# This is the constructor.
# parseConceptDomain parameters:
#	conceptDomainDecl: a XML::LibXML::Element 'dcc:concept-domain' element
#	model: a DCC::Model instance used to validate the concepts, columsn, etc...
# it returns a DCC::Model::ConceptDomain instance, with all the concept domain
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
	my @concepts = ();
	my %conceptHash = ();
	my @conceptDomain = (
		$conceptDomainDecl->getAttribute('domain'),
		$conceptDomainDecl->getAttribute('fullname'),
		undef,
		\@concepts,
		\%conceptHash,
		($conceptDomainDecl->hasAttribute('is-abstract') && ($conceptDomainDecl->getAttribute('is-abstract') eq 'true'))?1:undef,
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
	DCC::Model::Concept::parseConceptContainer($conceptDomainDecl,$retConceptDomain,$model);
	
	# Last, chicken and egg problem, part 2
	# This step must be delayed, because we need a enumeration of all the concepts
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
# A DCC::Model::FilenamePattern instance
sub filenamePattern {
	return $_[0]->[2];
}

# An array with the concepts under this concept domain umbrella
# An array of DCC::Model::Concept instances
sub concepts {
	return $_[0]->[3];
}

# A hash with the concepts under this concept domain umbrella
# A hash of DCC::Model::Concept instances
sub conceptHash {
	return $_[0]->[4];
}

# It returns 1 or undef, so it tells whether the whole concept domain is abstract or not
sub isAbstract {
	return $_[0]->[5];
}

# registerConcept parameters:
#	concept: a DCC::Model::Concept instance, which is going to be registered in the concept domain
sub registerConcept($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $concept = shift;
	
	push(@{$self->concepts},$concept);
	$self->conceptHash->{$concept->name} = $concept;
}

1;


package DCC::Model::Concept;

# Prototypes of static methods
sub parseConceptContainer($$$;$);

# parseConceptContainer paramereters:
#	conceptContainerDecl: A XML::LibXML::Element 'dcc:concept-domain'
#		or 'dcc:weak-concepts' instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, where this concept
#		has been defined.
#	model: a DCC::Model instance used to validate the concepts, columsn, etc...
#	idConcept: An optional, identifying DCC::Model::Concept instance of
#		all the (weak) concepts to be parsed from the container
# it returns an array of DCC::Model::Concept instances, which are all the
# concepts and weak concepts inside the input concept container
sub parseConceptContainer($$$;$) {
	my $conceptContainerDecl = shift;
	my $conceptDomain = shift;
	my $model = shift;
	my $idConcept = shift;	# This is optional (remember!)
	
	# Let's get the annotations inside the concept container
	my $weakAnnotations = DCC::Model::AnnotationSet->parseAnnotations($conceptContainerDecl);
	foreach my $conceptDecl ($conceptContainerDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'concept')) {
		# Concepts self register on the concept domain!
		my $concept = DCC::Model::Concept->parseConcept($conceptDecl,$conceptDomain,$model,$idConcept,$weakAnnotations);
		
		# There should be only one!
		foreach my $weakContainerDecl ($conceptDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'weak-concepts')) {
			DCC::Model::Concept::parseConceptContainer($weakContainerDecl,$conceptDomain,$model,$concept);
			last;
		}
	}
}

# This is the constructor
# parseConcept paramereters:
#	conceptDecl: A XML::LibXML::Element 'dcc:concept' instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, where this concept
#		has been defined.
#	model: a DCC::Model instance used to validate the concepts, columsn, etc...
#	idConcept: An optional, identifying DCC::Model::Concept instance of
#		the concept to be parsed from conceptDecl
#	weakAnnotations: The weak annotations for the columns of the identifying concept
# it returns an array of DCC::Model::Concept instances, the first one
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
	foreach my $baseConcept ($conceptDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'extends')) {
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
	my @baseConceptTypesDecl = $conceptDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'base-concept-type');
	Carp::croak("Concept $conceptFullname ($conceptName) has no base type (no dcc:base-concept-type)!")  if(scalar(@baseConceptTypesDecl)==0 && !defined($parentConcept));

	my @basetypes = ();
	foreach my $baseConceptType (@baseConceptTypesDecl) {
		my $basetypeName = $baseConceptType->getAttribute('name');
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
	my $baseColumnSet = (scalar(@basetypes) > 1)?DCC::Model::ColumnSet->combineColumnSets(1,map { $_->columnSet } @basetypes):((scalar(@basetypes) == 1)?$basetypes[0]->columnSet:undef);
	
	# And now, let's process the inherited columnSets, along with the basetypes ones
	if(defined($baseColumnSet)) {
		# Let's combine!
		if(defined($parentConcept)) {
			# Restrictive mode, with croaks
			$baseColumnSet = DCC::Model::ColumnSet->combineColumnSets(undef,$parentConcept->columnSet,$baseColumnSet);
		}
	} elsif(defined($parentConcept)) {
		# Should we clone this, to avoid potentian side effects?
		$baseColumnSet = $parentConcept->columnSet;
	} else {
		# This shouldn't happen!!
		Carp::croak("No concept types and no parent concept for $conceptFullname ($conceptName)");
	}
	
	# Preparing the columns
	my $columnSet = DCC::Model::ColumnSet->parseColumnSet($conceptDecl,$baseColumnSet,$model);
	
	# Adding the ones from the identifying concept
	# (and later from the related stuff)
	$columnSet->addColumns($idConcept->idColumns(! $idConcept->baseConceptType->isCollection,$weakAnnotations),1)  if(defined($idConcept));
	
	# This array will contain the names of the related concepts
	my @related = ();
	
	# Let's resolve inherited relations topic
	if(defined($parentConcept)) {
		# First, cloning
		@related = map { $_->clone() } @{$parentConcept->relatedConcepts};
		
		# Second, reparenting the RelatedConcept objects
		# Third, updating inheritable related concepts
		my $parentDomainChanges = $parentConceptDomain ne $conceptDomain;
		foreach my $relatedConcept (@related) {
			# Setting the name of the concept domain, in the cases where it was relative
			$relatedConcept->setConceptDomainName($parentConceptDomainName)  if($parentDomainChanges && !defined($relatedConcept->conceptDomainName));
			
			# Resetting the name of the related concept
			my $relatedConceptDomainName = defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$parentConceptDomainName;
			if($relatedConcept->isInheritable && $relatedConcept->conceptName eq $parentConceptName && $relatedConceptDomainName eq $parentConceptDomainName) {
				$relatedConcept->setConceptName($conceptName);
			}
		}
		
	}
	
	# Saving the related concepts (the ones explicitly declared within this concept)
	foreach my $relatedDecl ($conceptDecl->getChildrenByTagNameNS(DCC::Model::dccNamespace,'related-to')) {
		push(@related,DCC::Model::RelatedConcept->parseRelatedConcept($relatedDecl));
	}
	
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
		DCC::Model::DescriptionSet->parseDescriptions($conceptDecl),
		DCC::Model::AnnotationSet->parseAnnotations($conceptDecl,defined($parentConcept)?$parentConcept->annotations:undef),
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

# The DCC::Model::ConceptType instance basetype is the first element of the array
sub baseConceptType {
	return $_[0]->[2][0];
}

# A reference to the array of DCC::Model::ConceptType instances basetypes
sub baseConceptTypes {
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

# A DCC::Model::Concept instance, which represents the concept which has been extended
sub parentConcept {
	return $_[0]->[9];
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
#	weakAnnotations: DCC::Model::AnnotationSet from weak-concepts
sub idColumns(;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $doMask = shift;
	my $weakAnnotations = shift;
	
	return $self->columnSet->idColumns($self,$doMask,$weakAnnotations);
}

1;


package DCC::Model::RelatedConcept;

# This is the constructor.
# parseRelatedConcept parameters:
#	relatedDecl: A XML::LibXML::Element 'dcc:related-concept' instance
# It returns a DCC::Model::RelatedConcept instance
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
		DCC::Model::AnnotationSet->parseAnnotations($relatedDecl),
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

# It returns a DCC::Model::Concept instance
sub concept {
	return $_[0]->[3];
}

# It returns a DCC::Model::ColumnSet with the remote columns used for this relation
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

# It returns a DCC::Model::AnnotationSet instance
sub annotations {
	return $_[0]->[9];
}

# setRelatedConcept parameters:
#	concept: the DCC::Model::Concept instance being referenced
#	columnSet: the columns inherited from the concept, already with the key prefix
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

# clone creates a new DCC::Model::RelatedConcept
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