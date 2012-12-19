#!/usr/bin/perl -W

use strict;
use Carp;
use File::Basename;
use File::Spec;
use XML::LibXML;

package DCCModel;

use constant DCCSchemaFilename => 'bp-schema.xsd';
use constant dccNamespace => 'http://www.blueprint-epigenome.eu/dcc/schema';

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
	my %annotations = ();
	$self->{ANNOTATIONS}=\%annotations;
	foreach my $annotation ($modelRoot->getChildrenByTagNameNS(dccNamespace,'annotation')) {
		$annotations{$annotation->getAttribute('key')} = $annotation->textContent();
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
	
	# Now, the collection domain
	foreach my $colDom ($modelRoot->getChildrenByTagNameNS(dccdccNamespace,'collection-domain') {
		# TODO
		last;
	}
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
	
	return \@structCV;
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

1;