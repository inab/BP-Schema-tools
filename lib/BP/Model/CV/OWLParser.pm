#!/usr/bin/env perl

# This code is strongly based on OWL::Simple::Parser

=head1 NAME

OWL::Simple::Parser

=head1 SYNOPSIS

	use OWL::Simple::Parser;
	
	# load Experimental Factor Ontology (http://www.ebi.ac.uk/efo/efo.owl)
	my $parser = OWL::Simple::Parser->new(  owlfile => 'efo.owl',
			synonym_tag => 'efo:alternative_term',
			definition_tag => 'efo:definition' );
	
	# parse file
	$parser->parse();
	
	# iterate through all the classes
	for my $id (keys %{ $parser->class }){
		my $OWLClass = $parser->class->{$id};
		print $id . ' ' . $OWLClass->label . "\n";
		
		# list synonyms
		for my $syn (@{ $OWLClass->synonyms }){
			print "\tsynonym - $syn\n";
		}
		
		# list definitions
		for my $def (@{ $OWLClass->definitions }){
			print "\tdef - $def\n";
		}
		
		# list parents
		for my $parent (@{ $OWLClass->subClassOf }){
			print "\tsubClassOf - $parent\n";
		}
	}

=head1 DESCRIPTION

A simple OWL parser loading accessions, labels and synonyms and exposes them
as a collection of OWL::Simple::Class objects. 

This module wraps XML::Parser, which is a sequential event-driven XML parser that
can  potentially handle very large XML documents. The whole XML structure
is never loaded into memory completely, only the bits of interest.

In the constructor specify the owlfile to be loaded and two optional tags -
synonym_tag or definition_tag that define custom annotations in the ontology for 
synonyms and definitions respectively. Note both tags have to be fully 
specified exactly as in the OWL XML to be loaded, e.g. FULL_SYN for NCI Thesaurus 
or efo:alternative_term for EFO. 

=head2 METHODS

=over

=item class_count()

Number of classes loaded by the parser.

=item synonyms_count()

Number of synonyms loaded by the parser.

=item version()

Version of the ontology extracted from the owl:versionInfo.

=item class

Hash collection of all the OWL::Simple::Class objects

=back

=head1 AUTHOR

Tomasz Adamusiak <tomasz@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2010-2011 European Bioinformatics Institute. All Rights Reserved.

This module is free software; you can redistribute it and/or modify it 
under lGPLv3.

This software is provided "as is" without warranty of any kind.

=cut

package BP::Model::CV::OWLParser;

use base qw(XML::SAX::Base);
use Data::Dumper;

use BP::Model;
use BP::Model::CV;
use BP::Model::CV::Term;

use XML::SAX::ParserFactory;
use XML::LibXML::SAX;
use Log::Log4perl;

sub BEGIN {
	$XML::SAX::ParserPackage = "XML::LibXML::SAX";
	Log::Log4perl->easy_init( { level => $Log::Log4perl::WARN, layout => '%-5p - %m%n' } );
}

use constant RDF_NS => 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
use constant OWL_NS => 'http://www.w3.org/2002/07/owl#';
use constant OBO_NS => 'http://purl.obolibrary.org/obo/';

use constant {
	RDF_ABOUT	=> '{'.RDF_NS.'}about',
	RDF_RESOURCE	=> '{'.RDF_NS.'}resource',
	RDF_ID	=> '{'.RDF_NS.'}ID',
};

my %namespaces = (
	+RDF_NS	=>	'rdf',
	+OWL_NS	=>	'owl',
	+OBO_NS	=>	'obo',
	'http://www.w3.org/2000/01/rdf-schema#'	=>	'rdfs',
	'http://www.ebi.ac.uk/efo/'	=>	'efo',
);

sub new(\%) {
	my($self)=shift;
	my($class)=ref($self) || $self;
	
	Carp::croak((caller(0))[3].' is a class method!')  if(BP::Model::DEBUG && ref($class));
	
	$self = $class->SUPER::new()  unless(ref($self));
	
	my $params = shift;
	
	$params = {}  unless(ref($params) eq 'HASH');
	
	# This should be set at the beginning
	$self->{synonym_tag} = exists($params->{synonym_tag}) ? $params->{synonym_tag} : 'efo:alternative_term';
	$self->{definition_tag} = exists($params->{definition_tag}) ? $params->{definition_tag} : 'efo:definition';
	
	$self->{CV} = exists($params->{CV}) ? $params->{CV} : BP::Model::CV->new();
	
	# These should be reset on each parse
	$self->{namespaces} = \%namespaces;
	$self->{owlfile} = '';
	$self->{path} = '';
	$self->{restriction} = {};
	$self->{class_count} = 0;
	$self->{synonyms_count} = 0;
	$self->{version} = '';
	$self->{_classHash_} = {};
	$self->{LOG} = Log::Log4perl->get_logger(__PACKAGE__);
	$self->_newClassInstance();
	
	return $self;
}

sub _newClassInstance() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	$self->{_class} = {
		id	=>	undef,
		subClassOf	=>	[],
		part_of	=>	[],
		xrefs	=>	[],
		definitions	=>	[],
		synonyms	=>	[]
	};
}

# parse parameters:
#	owlH: The OWL file name or file handler to be parsed as OWL
# It returns the controlled vocabulary which has received the terms
sub parse($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $owlH = shift;
	
	$self->{owlfile} = $owlH  unless(ref($owlH));
	
	my $parser = XML::SAX::ParserFactory->parser( Handler => $self );
	
	$parser->parse_file($owlH);
	
	$self->{LOG}->debug("LOADED "
	  . $self->class_count
	  . ' CLASSES AND '
	  . $self->synonyms_count
	  . ' SYNONYMS from '
	  . $self->owlfile);
	
	return $self;
}

# Increments internal counter of classes and synonyms parser respectively.

sub incr_classes() {
	$_[0]->{class_count}++;
}

sub incr_synonyms() {
	$_[0]->{synonyms_count}++;
}

sub class_count() {
	return $_[0]->{class_count};
}

sub synonyms_count() {
	return $_[0]->{synonyms_count};
}

sub synonym_tag() {
	return $_[0]->{synonym_tag};
}

sub definition_tag() {
	return $_[0]->{definition_tag};
}

sub version() {
	return $_[0]->{version};
}

sub owlfile() {
	return $_[0]->{owlfile};
}

sub class() {
	return $_[0]->{_classHash_};
}

sub CV() {
	return $_[0]->{CV};
}

# Handler executed by XML::Parser. Adds current element to $path.
# $path is used characterData() to determine whtether node text should be
# added to class.
#
# Initializes a new OWLClass object and stores it in $class. This is later
# populated by other handlers.

sub path() {
	return $_[0]->{path};
}

sub start_element() {
	my($self,$el) = @_;
	
	my $element = $el->{Name};
	my $p_attrs = $el->{Attributes};
	if(exists($el->{NamespaceURI}) && exists($self->{namespaces}{$el->{NamespaceURI}})) {
		$element = $self->{namespaces}{$el->{NamespaceURI}}.':'.$el->{LocalName};
	}
	$self->{path} .= '/' . $element;    # add element to path
	
	my $path = $self->path;
	if( $path eq '/rdf:RDF/owl:Class' ) {
		$self->incr_classes();
		$self->{LOG}->info("Loaded " . $self->class_count . " classes from " . $self->owlfile )
		  if($self->class_count % 1000 == 0);
		
		
		if(exists($p_attrs->{+RDF_ABOUT}) || exists($p_attrs->{+RDF_ID})) {
			$self->_newClassInstance();
			
			my $idKey;
			if(exists($p_attrs->{+RDF_ID})) {
				$idKey = RDF_ID;
			} elsif(exists($p_attrs->{+RDF_ABOUT})) {
				$idKey = RDF_ABOUT;
			}
			$self->{_class}{id} = $p_attrs->{$idKey}{Value};
		}
	}
	
	# Imported terms are ... discarded
	elsif($path eq '/rdf:RDF/owl:Class/obo:IAO_0000412') {
		$self->{_class}{id} = undef;
	}

	# Two ways to match parents, either as rdf:resource attribute
	# on rdfs:subClassOf or rdf:about on nested rdfs:subClassOf/owl:Class
	elsif( $path eq '/rdf:RDF/owl:Class/rdfs:subClassOf' ) {
		push(@{ $self->{_class}{subClassOf} }, $p_attrs->{+RDF_RESOURCE}{Value})
		  if(exists($p_attrs->{+RDF_RESOURCE}));
	}
	elsif( $path eq '/rdf:RDF/owl:Class/rdfs:subClassOf/owl:Class' ) {
		push(@{ $self->{_class}{subClassOf} }, $p_attrs->{+RDF_ABOUT}{Value})
		  if(exists($p_attrs->{+RDF_ABOUT}));
	}

	# Here we try to match relations, e.g. part_of, derives_from, etc.
	elsif ( $element eq 'owl:Restriction' ) {
		$self->{restriction}{type}  = undef;
		$self->{restriction}{class} = [];
	}
	elsif ( $element eq 'owl:someValuesFrom' ) {
		my $idKey;
		if(exists($p_attrs->{+RDF_ABOUT})) {
			$idKey = RDF_ABOUT;
		} elsif(exists($p_attrs->{+RDF_RESOURCE})) {
			$idKey = RDF_RESOURCE;
		}
		push(@{ $self->{restriction}{class} }, $p_attrs->{$idKey}{Value})
		  if(defined($idKey));
	}

	# Regex as properties can be transitive, etc.
	elsif ( $element =~ /owl:\w+Property$/ ) {
		my $idKey;
		if(exists($p_attrs->{+RDF_RESOURCE})) {
			$idKey = RDF_RESOURCE;
		} elsif(exists($p_attrs->{+RDF_ABOUT})) {
			$idKey = RDF_ABOUT;
		}
		$self->{restriction}{type} = $p_attrs->{$idKey}{Value}
		  if(defined($idKey));
	}
	
	$self->SUPER::start_element($el);
}

# Handler executed by XML::Parser when node text is processed.
#
# For rdfs:label stores the value into $class->label otherwise
# class->annotation() this is then subsequently pushed into
# respective synonyms or definitions table when the 
# endElement() event is fired
# NOTE characterData can be called multiple times, before
# the end tag

sub characters {
	my($self,$cha) = @_;
	
	my $data = $cha->{Data};
	
	# Get rdfs:label
	my $path = $self->path;
	if($path eq '/rdf:RDF/owl:Class/rdfs:label' ) {
		$self->{_class}{label} = ( exists($self->{_class}{label}) ? $self->{_class}{label} : '' ) . $data;
	}

	# Get definition_citation or defintion
	elsif (
		$path =~ m!^/rdf:RDF/owl:Class/\w*:?\w*(definition|definition_citation)\w*!
		|| $path eq '/rdf:RDF/owl:Class/' . $self->definition_tag
		)
	{
		$self->{_class}{annotation} = (exists($self->{_class}{annotation}) ? $self->{_class}{annotation} : '') . $data;
	}
	
	# Get synonyms, either matching to anything with synonym or
	# alternative_term inside or custom tag from parameters
	elsif (
		   $path =~ m!^/rdf:RDF/owl:Class/\w*:?\w*(synonym|alternative_term)\w*!
		|| $path eq '/rdf:RDF/owl:Class/' . $self->synonym_tag )
	{
		$self->{_class}{annotation} = ( exists($self->{_class}{annotation}) ? $self->{_class}{annotation} : '' ) . $data;
		$self->{LOG}->warn( "Unparsable synonym detected for " . $self->{_class}{id} )
		  unless(defined($data));
		
		# detecting closing tag inside, NCIt fix
		# FIXME this is probably no longer necessary
		# once the synonym is concatenated, but have not checked
		#if ( $data =~ m!</! ) {
		#	($data) = $data =~ m!>(.*?)</!;    # match to first entry
		#}

	}
	
	# Extract version information
	elsif ( $path eq '/rdf:RDF/owl:Ontology/owl:versionInfo' ){
		$self->{version} .= $data;
	}
	
	$self->SUPER::characters($cha);
}

sub _shortKeyHelper($) {
	my $self = shift;
	
	# Let's generate the namespace uri and the short term
	my $term_uri = shift;
	
	my $index = -1;
	my $hashIndex = rindex($term_uri,'#');
	$index=$hashIndex  if($hashIndex > $index);
	
	my $equalIndex = rindex($term_uri,'=');
	$index=$equalIndex  if($equalIndex > $index);
	
	my $slashIndex = rindex($term_uri,'/');
	$index=$slashIndex  if($slashIndex > $index);
	
	my $namespace_uri = substr($term_uri,0,$index+1);
	my $shortTerm = substr($term_uri,$index+1);
	
	# Now, the short namespace name
	my $shortNS;
	my $joinPrefix;
	my $localTerm;
	my $underIndex = index($shortTerm,'_');
	
	my $p_namespaces = $self->CV->namespaces;
	
	my $bpCVNS;
	if($underIndex!=-1) {
		$shortNS = substr($shortTerm,0,$underIndex);
		$localTerm = substr($shortTerm,$underIndex+1);
		$joinPrefix = 1;
		
		$bpCVNS = $p_namespaces->{$shortNS}  if(exists($p_namespaces->{$shortNS}));
	} else {
		$localTerm = $shortTerm;
		
		# Now, let's guess the namespace
		foreach my $namespace (values(%{$p_namespaces})) {
			if($namespace->ns_uri eq $namespace_uri) {
				$bpCVNS = $namespace;
				last;
			}
		}
	}
	
	# Do we have to create a namespace?
	unless(defined($bpCVNS)) {
		$shortNS = '_gen_'.class_count()  unless(defined($shortNS));
		
		$bpCVNS = BP::Model::CV::Namespace->new($namespace_uri,$shortNS);
		$p_namespaces->{$shortNS} = $bpCVNS;
	}
	
	my $shortKey = $joinPrefix ? ($bpCVNS->ns_name.':'.$localTerm) : $localTerm;
	
	return wantarray?($shortKey,$bpCVNS):$shortKey;
}

# Handler executed by XML::Parser when the closing tag
# is encountered. For owl:Class it pushes it into the class hash as it was
# processed by characterData() already and the parser is ready to
# process a new owl:Class.
#
# Also strips the closing tag from $path.

sub end_element() {
	my($self,$el) = @_;
	
	# Normalizing the element
	my $element = $el->{Name};
	if(exists($el->{NamespaceURI}) && exists($self->{namespaces}{$el->{NamespaceURI}})) {
		$element = $self->{namespaces}{$el->{NamespaceURI}}.':'.$el->{LocalName};
	}
	
	my $path = $self->path;
	# Reached end of class, add the class to hash
	if ( $path eq '/rdf:RDF/owl:Class' ) {
		if(defined($self->{_class}{id}) && $self->{_class}{id} ne OWL_NS."Thing") {
			if(exists($self->class->{ $self->{_class}{id} })) {
				$self->{LOG}->warn('Class ' . $self->{_class}{id} . ' possibly duplicated');
			} else {
				# Let's generate the namespace uri and the short term
				my($shortKey,$bpCVNS) = $self->_shortKeyHelper($self->{_class}{id});
				
				# And also for the parents
				my @parents = map { scalar($self->_shortKeyHelper($_)) } @{$self->{_class}{subClassOf}};
				
				my $term = BP::Model::CV::Term->new($shortKey,$self->{_class}{label},$bpCVNS,\@parents);
				
				$self->CV->addTerm($term);
			}

			$self->class->{ $self->{_class}{id} } = $self->{_class};
		}
	}

	# Reached end of the relationship tag, add to appropriate array
	# Currently supports only part_of, and even that poorly.
	# FIXME circular references
	elsif ( $element eq 'owl:Restriction' ) {
		$self->{LOG}->warn("UNDEFINED RESTRICTION " . $self->{_class}{id})
		  unless(defined($self->{restriction}{type}));
		if ( $self->{restriction}{type} =~ m!/part_of$! ) {
			for my $cls ( @{ $self->{restriction}{class} } ) {
				push @{ $self->{_class}{part_of} }, $cls;
			}
		}
	}

	# character data can be called multiple times
	# for a single element, so it's concatanated there
	# and saved here
	elsif ( $path =~ m!^/rdf:RDF/owl:Class/\w*:?\w*definition_citation$! ){
		push(@{ $self->{_class}{xrefs} }, $self->{_class}{annotation})  if($self->{_class}{annotation} ne '');
	}
	elsif ( $path =~ m!^/rdf:RDF/owl:Class/\w*:?\w*definition$!
		|| $path eq '/rdf:RDF/owl:Class/' . $self->definition_tag ){
		push(@{ $self->{_class}{definitions} }, $self->{_class}{annotation})  if($self->{_class}{annotation} ne '');
	}
	elsif ( $path =~ m!^/rdf:RDF/owl:Class/\w*:?\w*(synonym|alternative_term)\w*!
		|| $path eq '/rdf:RDF/owl:Class/' . $self->synonym_tag ){
		$self->incr_synonyms();
		push(@{ $self->{_class}{synonyms}}, $self->{_class}{annotation})  if($self->{_class}{annotation} ne '');;
	}
	#print Dumper($self->{_class})  unless(exists($self->{_class}{annotation}));
	# clear temp annotation
	$self->{_class}{annotation}='';

	#remove end element from path
	$self->{path} = substr($self->{path},0,rindex($self->{path},'/'));
	
	$self->SUPER::end_element($el);
}

1;
