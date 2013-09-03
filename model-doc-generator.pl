#!/usr/bin/perl -W

# TODO:
#	Column listing reordering (based on annotations)
#	Document allowed null values
#	Document column name used to fetch values

use strict;
use Carp;
use Cwd;
use TeX::Encode;
use Encode;
use File::Copy;
use File::Spec;
use File::Temp;

use FindBin;
use lib "$FindBin::Bin/lib";
use DCC::Model;

use constant PDFLATEX => 'xelatex';
#use constant TERMSLIMIT => 200;
use constant TERMSLIMIT => 10000;

# Global variable (using my because our could have too much scope)
my $RELEASE = 1;

sub latex_escape($);
sub latex_format($);
sub sql_escape($);
sub genSQL($$$);

# Original code obtained from:
# http://ommammatips.blogspot.com.es/2011/01/perl-function-for-latex-escape.html
sub latex_escape_internal($) {
	my $paragraph = shift;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([\$\#&%])/\\$1/g;
	
	# This one helps in hyphenation
	$paragraph =~ s/_/\\-\\_\\-/g;
	
	# "Less than" and "greater than"
	$paragraph =~ s/>/\\textgreater/g;
	$paragraph =~ s/</\\textless/g;
	
	# Replace ^ characters with \^{} so that $^F works okay
	$paragraph =~ s/(\^)/\\$1\{\}/g;
	
	# Replace tilde (~) with \texttt{\~{}}
	$paragraph =~ s/~/\\texttt\{\\~\{\}\}/g;
	
	# Now add the dollars around each \backslash
	$paragraph =~ s/(\\backslash)/\$$1\$/g;
	$paragraph =~ s/(\\textgreater)/\$$1\$/g;
	$paragraph =~ s/(\\textless)/\$$1\$/g;
	return $paragraph;
}

sub latex_escape($) {
	my $par = shift;
	
	my $paragraph = $par;
	# Let's serialize this nodeset
	if(ref($par) eq 'ARRAY') {
		$paragraph = join('',map { ($_->can('toString'))?$_->toString(0):$_ } @{$par});
	}
	
	# Replace a \ with $\backslash$
	# This is made more complicated because the dollars will be escaped
	# by the subsequent replacement. Easiest to add \backslash
	# now and then add the dollars
	$paragraph =~ s/\\/\\backslash/g;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([{}])/\\$1/g;
	
	return latex_escape_internal($paragraph);
}

sub latex_format($) {
	my $par = shift;
	
	my $paragraph = $par;
	# Let's serialize this nodeset
	if(ref($par) eq 'ARRAY') {
		$paragraph = join('',map { $_->toString(0)} @{$par});
	}
	
	# Replace a \ with $\backslash$
	# This is made more complicated because the dollars will be escaped
	# by the subsequent replacement. Easiest to add \backslash
	# now and then add the dollars
	$paragraph =~ s/\\/\\backslash/g;
	
	# Must be done after escape of \ since this command adds latex escapes
	# Replace characters that can be escaped
	$paragraph =~ s/([{}])/\\$1/g;
	
	# HTML content
	$paragraph =~ s/\<br *\/?\>/\n\n/g;
	# (?:(?!PATTERN).)*
	$paragraph =~ s/\<i\>((?:(?!\<\/i\>).)*)\<\/i\>/\\textit{$1}/msg;
	$paragraph =~ s/\<b\>((?:(?!\<\/b\>).)*)\<\/b\>/\\textbf{$1}/msg;
	$paragraph =~ s/\<tt\>((?:(?!\<\/tt\>).)*)\<\/tt\>/\\texttt{$1}/msg;
	$paragraph =~ s/\<u\>((?:(?!\<\/u\>).)*)\<\/u\>/\\textunderscore{$1}/msg;
	$paragraph =~ s/\<a\s+href=["']([^>'"]+)['"]\>((?:(?!\<\/a\>).)*)\<\/a\>/\\href{$1}{$2}/msg;
	
	# XHTML content
	$paragraph =~ s/\<[^:]+:br *\/\>/\n\n/g;
	$paragraph =~ s/\<[^:]+:i\>((?:(?!\<\/[^:]+:i\>).)*)\<\/[^:]+:i\>/\\textit{$1}/msg;
	$paragraph =~ s/\<[^:]+:b\>((?:(?!\<\/[^:]+:b\>).)*)\<\/[^:]+:b\>/\\textbf{$1}/msg;
	$paragraph =~ s/\<[^:]+:tt\>((?:(?!\<\/[^:]+:tt\>).)*)\<\/[^:]+:tt\>/\\texttt{$1}/msg;
	$paragraph =~ s/\<[^:]+:u\>((?:(?!\<\/[^:]+:u\>).)*)\<\/[^:]+:u\>/\\textunderscore{$1}/msg;
	$paragraph =~ s/\<[^:]+:a\s+href=["']([^>'"]+)['"]\>((?:(?!\<\/[^:]+:a\>).)*)\<\/[^:]+:a\>/\\href{$1}{$2}/msg;
	
	# Encoded entities &lt; and &gt;
	$paragraph =~ s/&lt;/\</g;
	$paragraph =~ s/&gt;/\>/g;
	
	return latex_escape_internal($paragraph);
}

sub entryName($;$) {
	my($concept,$conceptDomainName)=@_;
	
	$conceptDomainName = $concept->conceptDomain->name  unless(defined($conceptDomainName));
	
	return $conceptDomainName.'_'.$concept->name;
}

sub labelPrefix($) {
	my($concept)=@_;
	
	return $concept->conceptDomain->name.'.'.$concept->name.'.';
}

sub labelColumn($$) {
	my($conceptOrLabelPrefix,$column)=@_;
	
	$conceptOrLabelPrefix = labelPrefix($conceptOrLabelPrefix)  if(ref($conceptOrLabelPrefix));
	
	return $conceptOrLabelPrefix.$column->name;
}

my %ABSTYPE2SQL = (
	'string' => 'VARCHAR(1024)',
	'text' => 'TEXT',
	'integer' => 'INTEGER',
	'decimal' => 'DOUBLE PRECISION',
	'boolean' => 'BOOL',
	'timestamp' => 'DATETIME',
	'duration' => 'VARCHAR(128)',
	'compound' => 'TEXT',
);

my %ABSTYPE2SQLKEY = %ABSTYPE2SQL;

$ABSTYPE2SQLKEY{'string'} = 'VARCHAR(128)';

my %COLKIND2ABBR = (
	DCC::Model::ColumnType::IDREF	=>	'I',
	DCC::Model::ColumnType::REQUIRED	=>	'R',
	DCC::Model::ColumnType::DESIRABLE	=>	'D',
	DCC::Model::ColumnType::OPTIONAL	=>	'O'
);

# fancyColumnOrdering parameters:
#	concept: a DCC::Concept instance
# It returns an array with the column names from the concept in a fancy
# order, based on several criteria, like annotations
sub fancyColumnOrdering1($) {
	my($concept)=@_;
	
	my @colorder = ();
	
	my $columnSet = $concept->columnSet;

	# First, the idref columns, alphabetically ordered
	my @idcolorder=sort(@{$columnSet->idColumnNames});

	# And then, the others
	my %idcols = map { $_ => undef } @idcolorder;
	foreach my $columnName (@{$columnSet->columnNames}) {
		push(@colorder,$columnName)  unless(exists($idcols{$columnName}));
	}
	
	return (@idcolorder,sort(@colorder));
}

# parseOrderingHints parameters:
#	a XML::LibXML::Element, type 'dcc:ordering-hints'
# It returns the ordering hints (at this moment, undef or the block where it appears)
sub parseOrderingHints($) {
	my($ordHints) = @_;
	
	my $retvalBlock = undef;
	if(ref($ordHints) && $ordHints->isa('XML::LibXML::Element')
		&& $ordHints->namespaceURI eq DCC::Model::dccNamespace
		&& $ordHints->localname eq 'ordering-hints'
	) {
		foreach my $block ($ordHints->getChildrenByTagNameNS(DCC::Model::dccNamespace,'block')) {
			$retvalBlock = $block->textContent;
			last;
		}
	}
	
	return ($retvalBlock);
}

# fancyColumnOrdering parameters:
#	concept: a DCC::Concept instance
# It returns an array with the column names from the concept in a fancy
# order, based on several criteria, like annotations
sub fancyColumnOrdering($) {
	my($concept)=@_;
	
	my $columnSet = $concept->columnSet;

	# First, the idref columns, alphabetically ordered
	my @first = ();
	my @middle = ();
	my @last = ();
	foreach my $columnName (@{$columnSet->idColumnNames}) {
		my $column = $columnSet->columns->{$columnName};
		
		my $p_set = \@middle;
		if(exists($column->annotations->hash->{ordering})) {
			my($block) = parseOrderingHints($column->annotations->hash->{ordering});
			
			if($block eq 'bottom') {
				$p_set = \@last;
			} elsif($block eq 'top') {
				$p_set = \@first;
			}
		}
		
		push(@{$p_set},$columnName);
	}
	
	my @idcolorder = (@first,@middle,@last);
	# Resetting for next use
	@first = ();
	@middle = ();
	@last = ();

	# And then, the others
	my %idcols = map { $_ => undef } @idcolorder;
	foreach my $columnName (@{$columnSet->columnNames}) {
		next  if(exists($idcols{$columnName}));
		
		my $column = $columnSet->columns->{$columnName};
		
		my $p_set = \@middle;
		if(exists($column->annotations->hash->{ordering})) {
			my($block) = parseOrderingHints($column->annotations->hash->{ordering});
			
			if($block eq 'bottom') {
				$p_set = \@last;
			} elsif($block eq 'top') {
				$p_set = \@first;
			}
		}
		
		push(@{$p_set},$columnName);
	}
	
	my @colorder = (@first,@middle,@last);
	
	
	return (@idcolorder,@colorder);
}

sub sql_escape($) {
	my $par = shift;
	$par =~ s/'/''/g;
	return '\''.$par.'\'';
}

# genSQL parameters:
#	model: a DCC::Model instance, with the parsed model.
#	the path to the SQL output file
#	the path to the SQL script which joins 
sub genSQL($$$) {
	my($model,$outfileSQL,$outfileTranslateSQL) = @_;
	
	# Needed later for CV dumping et al
	my @cvorder = ();
	my %cvdump = ();
	my $chunklines = 4096;
	my $descType = $ABSTYPE2SQL{'string'};
	my $aliasType = $ABSTYPE2SQL{'boolean'};
	
	if(open(my $SQL,'>:utf8',$outfileSQL)) {
		print $SQL '-- File '.basename($outfileSQL)."\n";
		print $SQL '-- Generated from '.$model->projectName.' '.$model->versionString."\n";
		print $SQL '-- '.localtime()."\n";
		
		# Needed later for foreign keys
		my @fks = ();
		
		# Let's iterate over all the concept domains and their concepts
		my $p_TYPES = $model->types;
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			# Skipping abstract concept domains
			next  if($RELEASE && $conceptDomain->isAbstract);
			
			my $conceptDomainName = $conceptDomain->name;
			
			my %pcon = ();
			foreach my $concept (@{$conceptDomain->concepts}) {
				#my $conceptName = $concept->name;
				my $basename = entryName($concept,$conceptDomainName);

				my %fkselemrefs = ();
				my @fkselem = ($basename,$concept,\%fkselemrefs);
				my $fksinit = undef;

				print $SQL "\n-- ",$concept->fullname;
				print $SQL "\nCREATE TABLE $basename (";
				
				
				my @colorder = fancyColumnOrdering($concept);
				my $columnSet = $concept->columnSet;
				my $gottable=undef;
				
				foreach my $column (@{$columnSet->columns}{@colorder}) {
					# Is it involved in a foreign key outside the relatedConcept system?
					if(defined($column->refColumn) && !defined($column->relatedConcept)) {
						$fksinit = 1;
						
						my $refConcept = $column->refConcept;
						my $refBasename = entryName($refConcept);
						
						$fkselemrefs{$refBasename} = [$refConcept,[]]  unless(exists($fkselemrefs{$refBasename}));
						
						push(@{$fkselemrefs{$refBasename}[1]}, $column);
					}
					
					# Let's print
					print $SQL ','  if(defined($gottable));
					
					my $columnType = $column->columnType;
					my $SQLtype = ($columnType->use == DCC::Model::ColumnType::IDREF || defined($column->refColumn))?$ABSTYPE2SQLKEY{$columnType->type}:$ABSTYPE2SQL{$columnType->type};
					# Registering CVs
					if(defined($columnType->restriction) && $columnType->restriction->isa('DCC::Model::CV')) {
						# At the end is a key outside here, so assuring it is using the right size
						# due restrictions on some SQL (cough, cough, MySQL, cough, cough) implementations
						$SQLtype = $ABSTYPE2SQLKEY{$columnType->type};
						my $CV = $columnType->restriction;
						
						my $cvname = $CV->name;
						#$cvname = $basename.'_'.$column->name  unless(defined($cvname));
						# Perl reference trick to get a number
						$cvname = 'anon_'.($CV+0)  unless(defined($cvname));
						
						# Second position is the SQL type
						# Third position holds the columns which depend on this CV
						unless(exists($cvdump{$cvname})) {
							$cvdump{$cvname} = [$CV,$p_TYPES->{$columnType->type}[DCC::Model::ColumnType::ISNOTNUMERIC],$SQLtype,[]];
							push(@cvorder,$cvname);
						}
						
						# Saving the column and table name for further use
						push(@{$cvdump{$cvname}[3]},[$column->name,$basename]);
					}
					
					print $SQL "\n\t",$column->name,' ',$SQLtype;
					print $SQL ' NOT NULL'  if($columnType->use >= DCC::Model::ColumnType::IDREF);
					if(defined($columnType->default) && ref($columnType->default) eq '') {
						my $default = $columnType->default;
						$default = sql_escape($default)  if($p_TYPES->{$columnType->type}[DCC::Model::ColumnType::ISNOTNUMERIC]);
						print $SQL ' DEFAULT ',$default;
					}
					$gottable = 1;
				}
				
				push(@fks,\@fkselem)  if(defined($fksinit));
				
				# Declaring a primary key (if any!)
				my @idColumnNames = @{$columnSet->idColumnNames};
				
				if(scalar(@idColumnNames)>0) {
					print $SQL ",\n\tPRIMARY KEY (".join(',',@idColumnNames).')';
					$pcon{$basename} = undef;
				}
				
				print $SQL "\n);\n\n";
			}
			
			## Now, the FK restrictions from inheritance
			#foreach my $concept (@{$conceptDomain->concepts}) {
			#	my $basename = entryName($concept,$conceptDomainName);
			#	if(defined($concept->idConcept)) {
			#		my $idConcept = $concept->idConcept;
			#		my $refColnames = $idConcept->columnSet->idColumnNames;
			#		my $idBasename = entryName($idConcept,$conceptDomainName);
			#		
			#		# Referencing only concepts with keys
			#		if(exists($pcon{$idBasename})) {
			#			print $SQL "\n-- ",$concept->fullname, " foreign keys";
			#			print $SQL "\nALTER TABLE $basename ADD FOREIGN KEY (",join(',',@{$refColnames}),")";
			#			print $SQL "\nREFERENCES $idBasename(".join(',',@{$refColnames}).");\n";
			#		}
			#	}
			#}
		}
		
		# Now, the CVs and the columns using them
		if(open(my $TSQL,'>:utf8',$outfileTranslateSQL)) {
			print $TSQL '-- File '.basename($outfileTranslateSQL)."\n";
			print $TSQL '-- Generated from '.$model->projectName.' '.$model->versionString."\n";
			print $TSQL '-- '.localtime()."\n";
			
			foreach my $cvname (@cvorder) {
				my($CV,$doEscape,$SQLtype,$p_columnRefs) = @{$cvdump{$cvname}};
				
				# First, the tables
				print $SQL <<CVEOF;
			
-- $cvname controlled vocabulary tables and data
CREATE TABLE ${cvname}_CV (
	idkey $SQLtype NOT NULL,
	descr $descType NOT NULL,
	isalias $aliasType NOT NULL,
	PRIMARY KEY (idkey)
);

CREATE TABLE ${cvname}_CVkeys (
	cvkey $SQLtype NOT NULL,
	idkey $SQLtype NOT NULL,
	PRIMARY KEY (cvkey),
	FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey)
);

CREATE TABLE ${cvname}_CVparents (
	idkey $SQLtype NOT NULL,
	cvkey $SQLtype NOT NULL,
	FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys(cvkey)
);

CREATE TABLE ${cvname}_CVancestors (
	idkey $SQLtype NOT NULL,
	cvkey $SQLtype NOT NULL,
	FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys(cvkey)
);

CVEOF

				# Second, the data
				my $first = 0;
				foreach my $key  (@{$CV->order},@{$CV->aliasOrder}) {
					my $term = $CV->CV->{$key};
					if($first==0) {
						print $SQL <<CVEOF;
INSERT INTO ${cvname}_CV VALUES
CVEOF
					}
					print $SQL (($first>0)?",\n":''),'(',join(',',($doEscape)?sql_escape($term->key):$term->key,sql_escape($term->name),($term->isAlias)?'TRUE':'FALSE'),')';
					
					$first++;
					if($first>=$chunklines) {
						print $SQL "\n;\n\n";
						$first=0;
					}
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				$first = 0;
				foreach my $key  (@{$CV->order},@{$CV->aliasOrder}) {
					my $term = $CV->CV->{$key};
					my $ekey = ($doEscape)?sql_escape($term->key):$term->key;
					
					foreach my $akey (@{$term->keys}) {
						if($first==0) {
							print $SQL <<CVEOF;
INSERT INTO ${cvname}_CVkeys VALUES
CVEOF
						}
						print $SQL (($first>0)?",\n":''),'(',join(',',($doEscape)?sql_escape($akey):$akey,$ekey),')';
						
						$first++;
						if($first>=$chunklines) {
							print $SQL "\n;\n\n";
							$first=0;
						}
					}
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				$first = 0;
				foreach my $key  (@{$CV->order},@{$CV->aliasOrder}) {
					my $term = $CV->CV->{$key};
					my $ekey = ($doEscape)?sql_escape($term->key):$term->key;
					
					next  unless(defined($term->parents) && scalar(@{$term->parents})>0);
					
					foreach my $pkey (@{$term->parents}) {
						if($first==0) {
							print $SQL <<CVEOF;
INSERT INTO ${cvname}_CVparents VALUES
CVEOF
						}
						print $SQL (($first>0)?",\n":''),'(',join(',',$ekey,($doEscape)?sql_escape($pkey):$pkey),')';
						
						$first++;
						if($first>=$chunklines) {
							print $SQL "\n;\n\n";
							$first=0;
						}
					}
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				$first = 0;
				foreach my $key  (@{$CV->order},@{$CV->aliasOrder}) {
					my $term = $CV->CV->{$key};
					my $ekey = ($doEscape)?sql_escape($term->key):$term->key;
					
					next  unless(defined($term->ancestors) && scalar(@{$term->ancestors})>0);
					
					foreach my $pkey (@{$term->ancestors}) {
						if($first==0) {
							print $SQL <<CVEOF;
INSERT INTO ${cvname}_CVancestors VALUES
CVEOF
						}
						print $SQL (($first>0)?",\n":''),'(',join(',',$ekey,($doEscape)?sql_escape($pkey):$pkey),')';
						
						$first++;
						if($first>=$chunklines) {
							print $SQL "\n;\n\n";
							$first=0;
						}
					}
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				# Third, the references
				# And the translation script
				foreach my $p_columnRef (@{$p_columnRefs}) {
					my($columnName,$tableName)=@{$p_columnRef};
#CREATE INDEX ${tableName}_${columnName}_CVindex ON $tableName ($columnName);
#
					print $SQL <<CVEOF;
ALTER TABLE $tableName ADD FOREIGN KEY ($columnName)
REFERENCES ${cvname}_CVkeys(cvkey);

CVEOF
					print $TSQL <<TCVEOF;
ALTER TABLE $tableName ADD COLUMN ${columnName}_term $descType;

UPDATE $tableName , ${cvname}_CVkeys , ${cvname}_CV
SET
	${tableName}.${columnName}_term = ${cvname}_CV.descr
WHERE
	${tableName}.${columnName}_term IS NULL AND
	${tableName}.${columnName} = ${cvname}_CVkeys.cvkey
	AND ${cvname}_CVkeys.idkey = ${cvname}_CV.idkey
;
TCVEOF
				}
			}
			close($TSQL);
		} else {
			Carp::croak("Unable to create output file $outfileTranslateSQL");
		}
		
		# Now, the FK restrictions from identification relations
		foreach my $p_fks (@fks) {
			my($basename,$concept,$p_fkconcept) = @{$p_fks};
			
			print $SQL "\n-- ",$concept->fullname, " foreign keys from inheritance";
			my $cycle = 1;
			foreach my $relatedBasename (keys(%{$p_fkconcept})) {
				my $p_columns = $p_fkconcept->{$relatedBasename}[1];
				
				#print $SQL "\nCREATE INDEX ${basename}_ID${cycle}_${relatedBasename} ON $basename (",join(',',map { $_->name } @{$p_columns}),");\n";
				print $SQL "\nALTER TABLE $basename ADD FOREIGN KEY (",join(',',map { $_->name } @{$p_columns}),")";
				print $SQL "\nREFERENCES $relatedBasename(".join(',',map { $_->refColumn->name } @{$p_columns}).");\n";
				$cycle++;
			}
		}

		# And now, the FK restrictions from related concepts
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			next  if($RELEASE && $conceptDomain->isAbstract);
			
			my $conceptDomainName = $conceptDomain->name;
			
			foreach my $concept (@{$conceptDomain->concepts}) {
				#my $conceptName = $concept->name;
				my $basename = entryName($concept,$conceptDomainName);
				
				# Let's visit each concept!
				if(scalar(@{$concept->relatedConcepts})>0) {
					print $SQL "\n-- ",$concept->fullname, " foreign keys from related-to";
					my $cycle = 1;
					foreach my $relatedConcept (@{$concept->relatedConcepts}) {
						# Skipping foreign keys to abstract concepts
						next  if($RELEASE && $relatedConcept->concept->conceptDomain->isAbstract);
						
						my $refBasename = entryName($relatedConcept->concept,(defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName));
						my @refColumns = values(%{$relatedConcept->columnSet->columns});
						#print $SQL "\nCREATE INDEX ${basename}_FK${cycle}_${refBasename} ON $basename (",join(',',map { $_->name } @refColumns),");\n";
						print $SQL "\nALTER TABLE $basename ADD FOREIGN KEY (",join(',',map { $_->name } @refColumns),")";
						print $SQL "\nREFERENCES $refBasename(".join(',',map { $_->refColumn->name } @refColumns).");\n";
						$cycle++;
					}
				}
			}
		}
		
		close($SQL);
	} else {
		Carp::croak("Unable to create output file $outfileSQL");
	}
	
}

use constant {
	REL_TEMPLATES_DIR	=>	'doc-templates',
	PACKAGES_TEMPLATE_FILE	=>	'packages.latex',
	FONTS_TEMPLATE_FILE	=>	'fonts.latex',
	COVER_TEMPLATE_FILE	=>	'cover.latex',
	FRONTMATTER_TEMPLATE_FILE	=>	'frontmatter.latex',
	ICONS_DIR	=>	'icons',
	FIGURE_PREAMBLE_FILE	=>	'figure-preamble.latex'
};

# assemblePDF parameters:
#	templateDir: The LaTeX template dir to be used to generate the PDF
#	model: DCC::Model instance
#	bpmodelFile: The model in bpmodel format
#	bodyFile: The temporal file where the generated documentation has been written.
#	outputFile: The output PDF file.
#	outputSH: Shell script to rebuild documentation
sub assemblePDF($$$$$$) {
	my($templateDir,$model,$bpmodelFile,$bodyFile,$outputFile,$outputSH) = @_;
	
	my $masterTemplate = File::Spec->catfile($FindBin::Bin,basename($0,'.pl').'.latex');
	unless(-f $masterTemplate && -r $masterTemplate) {
		die "ERROR: Unable to find master template LaTeX file $masterTemplate\n";
	}
	
	foreach my $relfile (PACKAGES_TEMPLATE_FILE,FONTS_TEMPLATE_FILE,COVER_TEMPLATE_FILE,FRONTMATTER_TEMPLATE_FILE) {
		my $templateFile = File::Spec->catfile($templateDir,$relfile);
		
		unless(-f $templateFile && -r $templateFile) {
			die "ERROR: Unable to find readable template LaTeX file $relfile in dir $templateDir\n";
		}
	}

	my($bodyDir,$bodyName);
	if(-f $bodyFile && -r $bodyFile) {
		my $absbody = File::Spec->rel2abs($bodyFile);
		$bodyDir = dirname($absbody);
		$bodyName = basename($absbody);
	} else {
		die "ERROR: Unable to find readable body LaTeX file $bodyFile\n";
	}
	
	my $docsDir = $model->documentationDir();
	
	my $annotationSet = $model->annotations;
	my $annotations = $annotationSet->hash;
	
	# The name of the overview file should come from here
	my $overviewFile = $annotations->{overviewDoc};
	
	unless(File::Spec->file_name_is_absolute($overviewFile)) {
		$overviewFile = File::Spec->catfile($docsDir,$overviewFile);
	}
	
	my($overviewDir,$overviewName);
	if(-f $overviewFile && -r $overviewFile) {
		my $absoverview = File::Spec->rel2abs($overviewFile);
		$overviewDir = dirname($absoverview);
		$overviewName = basename($absoverview);
	} else {
		die "ERROR: Unable to find readable overview LaTeX file $overviewFile (declared in model!)\n";
	}
	
	# Storing the document generation parameters
	my @params = map {
		my $str = encode('latex',$annotations->{$_});
		$str =~ s/\{\}//g;
		['ANNOT'.$_,$str]
	} keys(%{$annotations});
	push(@params,['projectName',$model->projectName],['schemaVer',$model->versionString],['modelSHA',$model->modelSHA1],['CVSHA',$model->CVSHA1],['schemaSHA',$model->schemaSHA1]);
	
	# Final slashes in directories are VERY important for subimports!!!!!!!! (i.e. LaTeX is dumb)
	push(@params,['INCLUDEtemplatedir',$templateDir.'/']);
	push(@params,['INCLUDEoverviewdir',$overviewDir.'/'],['INCLUDEoverviewname',$overviewName]);
	push(@params,['INCLUDEbodydir',$bodyDir.'/'],['INCLUDEbodyname',$bodyName]);
	push(@params,
		['INCLUDEpackagesFile',PACKAGES_TEMPLATE_FILE],
		['INCLUDEfontsFile',FONTS_TEMPLATE_FILE],
		['INCLUDEcoverFile',COVER_TEMPLATE_FILE],
		['INCLUDEfrontmatterFile',FRONTMATTER_TEMPLATE_FILE]
	);
	
	my(undef,undef,$bpmodelFilename) = File::Spec->splitpath($bpmodelFile);
	push(@params,['BPMODELfilename',latex_escape($bpmodelFilename)]);
	push(@params,['BPMODELpath',$bpmodelFile]);
	
	# Setting the jobname and the jobdir, pruning the .pdf extension from the output
	my $absjob = File::Spec->rel2abs($outputFile);
	my $jobDir = dirname($absjob);
	my $jobName = basename($absjob,'.pdf');

	
	# And now, let's prepare the command line
	my @pdflatexParams = (
		'-interaction=batchmode',
#		'-synctex=1',
#		'-shell-escape',
		'-jobname',$jobName,
		'-output-directory',$jobDir,
		join(' ',map { '\gdef\\'.$_->[0].'{'.$_->[1].'}' } @params).' \input{'.$masterTemplate.'}'
	);
	
	print STDERR "[DOCGEN] => ",join(' ',PDFLATEX,@pdflatexParams),"\n";
	
	if(defined($outputSH)) {
		if(open(my $SH,'>',$outputSH)) {
			my $commandLine = join(' ',map {
				my $res = $_;
				if($res =~ /['" ()\$\\]/) {
					$res =~ s/'/'"'"'/g;
					$res = "'".$res."'";
				}
				$res
			} @pdflatexParams);
			my $workingDir = cwd();
			if($workingDir =~ /['" ()\$\\]/) {
				$workingDir =~ s/'/'"'"'/g;
				$workingDir = "'".$workingDir."'";
			}
			print $SH <<EOFSH;
#!/bin/sh

cd $workingDir
${\PDFLATEX} $commandLine
EOFSH
			close($SH);
		} else {
			warn "ERROR: Unable to create shell script $outputSH\n";
		}
	}
	
	# exit 0;
	
	my $follow = 1;
	foreach my $it (1..5) {
		if(system(PDFLATEX,'-draftmode',@pdflatexParams)!=0) {
			$follow = undef;
			last;
		}
	}
	if(defined($follow)) {
		system(PDFLATEX,@pdflatexParams);
	}
}

my $TOCFilename = 'toc.latex';
my $NotesFilename = 'notes.latex';

my %COMMANDS = (
	'file' => 'subsection',
	'featureType' => undef,
	'fileType' => undef,
	'altname' => undef
);

my %CVCOMMANDS = (
	'file' => 'section',
	'header' => undef,
	'disposition' => undef,
	'version' => undef,
);

sub printDescription($$;$) {
	my($O,$text,$command)=@_;
	
	if(defined($command) && length($command)>0) {
		my $latex;
		if(exists($COMMANDS{$command})) {
			$latex = $COMMANDS{$command};
		} elsif(substr($command,0,5) eq 'LATEX') {
			$latex = substr($command,5);
		}
		print $O "\\$latex\{",latex_format($text),"\}\n"  if(defined($latex));
	} else {
		print $O latex_format($text),"\n\n";
	}
}

# processInlineCVTable parameters:
#	CV: a DCC::Model::CV instance
# it returns a LaTeX formatted table
sub processInlineCVTable($) {
	my($CV)=@_;
	
	my $output='';
	
	# First, the embedded documentation
	foreach my $documentation (@{$CV->description}) {
		$output .= latex_format($documentation)."\n";
	}
	# TODO: process the annotations
	my $inline = $CV->kind ne DCC::Model::CV::URIFETCHED || (exists($CV->annotations->hash->{'disposition'}) && $CV->annotations->hash->{disposition} eq 'inline');
	
	$output .= "\n";
	# We have the values. Do we have to print them?
	if($CV->isLocal && $inline) {
		$output .= '\begin{tabularx}{0.5\columnwidth}{>{\textbf\bgroup\texttt\bgroup}r<{\egroup\egroup}@{ $\mapsto$ }>{\raggedright\arraybackslash}X}'."\n";
		#$output .= '\begin{tabular}{r@{ = }l}'."\n";

		my $CVhash = $CV->CV;
		foreach my $key (@{$CV->order}) {
			$output .= join(' & ',latex_escape($key),latex_escape($CVhash->{$key}->name))."\\\\\n";
		}
		$output .= '\end{tabularx}';
	}
	
	if($CV->kind eq DCC::Model::CV::URIFETCHED) {
		# Is it an external CV
		$output .= '\textit{(See ';
		
		my @extCVs = @{$CV->uri};
		my $extpos = 1;
		foreach my $extCV (@extCVs) {
			if($extpos > 1) {
				$output .= ($extpos == scalar(@extCVs))?' and ':', ';
			}
			my $uri = $extCV->uri;
			my $docURI = $extCV->docURI;
			my $cvuri = undef;
			# Do we have external documentation?
			if(defined($docURI)) {
				$cvuri = $docURI->as_string;
			} else {
				$cvuri = $uri->as_string;
			}
			
			$output .= '\url{'.$cvuri.'}';
			$extpos++;
		}
		$output .= ')}';
	}
	$output .= "\n";
	
	return $output;
}

# printCVTable parameters:
#	CV: A named DCC::Model::CV instance (the controlled vocabulary)
#	O: The output filehandle where the documentation about the
#		controlled vocabulary is written.
sub printCVTable($$) {
	my($CV,$O)=@_;
	
	my $cvname = $CV->name;
	
	my $annotationSet = $CV->annotations;
	my $annotations = $annotationSet->hash;
	
	# The caption
	my $caption = undef;
	if(exists($annotations->{'file'})) {
		$caption = $annotations->{'file'};
	} else {
		$caption = "CV Table $cvname";
	}
	print $O "\\section{",latex_escape($caption),"} \\label{cvsec:$cvname}\n";
	
	print $O "\\textit{This controlled vocabulary has ".scalar(@{$CV->order})." terms".((scalar(@{$CV->aliasOrder})>0)?(" and ".scalar(@{$CV->aliasOrder})." aliases"):"")."}\\\\[2ex]\n"  if($CV->isLocal);
	
	my @header = ();
	
	# The header names used by the LaTeX table
	if(exists($annotations->{'header'})) {
		@header = split(/\t/,$annotations->{'header'},2);
	} else {
		@header = ('Key','Description');
	}
	$header[0] = latex_format($header[0]);
	$header[1] = latex_format($header[1]);
	
	# Printing embedded documentation
	foreach my $documentation (@{$CV->description}) {
		print $O latex_format($documentation),"\n";
	}
	
	# And annotations
	foreach my $annotName (@{$annotationSet->order}) {
		next  if($annotName eq 'file' || $annotName eq 'header');
		
		my $latex = undef;
		if(exists($CVCOMMANDS{$annotName})) {
			$latex = $CVCOMMANDS{$annotName};
		} elsif(substr($annotName,0,5) eq 'LATEX') {
			$latex = substr($annotName,5);
		}
		
		print $O "\\$latex\{",latex_format($annotations->{$annotName}),"\}\n"  if(defined($latex));
	}
	
	my $doPrintCV =  $CV->isLocal && (scalar(@{$CV->order}) <= TERMSLIMIT || (exists($CV->annotations->hash->{'showFetched'}) && $CV->annotations->hash->{showFetched} eq 'true'));
	
	if($doPrintCV || (scalar(@{$CV->aliasOrder}) > 0 && scalar(@{$CV->aliasOrder}) <= TERMSLIMIT)) {
		# Table header
		print $O <<EOF ;
\\renewcommand{\\cvKey}{$header[0]}
\\renewcommand{\\cvDesc}{$header[1]}
EOF
	}
	
	# We have the values. Do we have to print them?
	if($doPrintCV) {
		my $latexcaption = latex_format($caption);
		print $O <<'EOF';
{
	\arrayrulecolor{DarkOrange}
	\begin{longtable}[c]{|>{\tt}r|p{0.5\textwidth}|}
EOF
		print $O '\caption{'.$latexcaption.'} \label{cv:'.$cvname.'} \\\\'."\n";
		print $O <<'EOF';
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline\hline
\endfirsthead
\multicolumn{2}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline
\endhead
\hline
\multicolumn{2}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline
\endfoot
\endlastfoot
EOF
	
		my $CVhash = $CV->CV;
		foreach my $cvKey (@{$CV->order}) {
			print $O join(' & ',latex_escape($cvKey),latex_escape($CVhash->{$cvKey}->name)),'\\\\ \hline',"\n";
		}
		
		# Table footer
		print $O <<'EOF';
	\end{longtable}
}
EOF
	}
	
	if($CV->kind() eq DCC::Model::CV::URIFETCHED){
		# Remote CVs
		# Is it an external CV
		
		my @extCVs = @{$CV->uri};
		print $O "\n",'\textit{(See '.((scalar(@extCVs)>1)?'them':'it').' at ';
		my $extpos = 1;
		foreach my $extCV (@extCVs) {
			if($extpos > 1) {
				print $O ($extpos == scalar(@extCVs))?' and ':', ';
			}
			my $uri = $extCV->uri;
			my $docURI = $extCV->docURI;
			my $cvuri = undef;
			# Do we have external documentation?
			if(defined($docURI)) {
				$cvuri = $docURI->as_string;
			} else {
				$cvuri = $uri->as_string;
			}
			
			print $O '\url{'.$cvuri.'}';
			$extpos++;
		}
		print $O ')}';
	}
	
	# And now, the aliases
	if(scalar(@{$CV->aliasOrder}) > 0 && scalar(@{$CV->aliasOrder}) <= TERMSLIMIT) {
		my $latexcaption = latex_format($caption);
		print $O <<'EOF';
{
	\arrayrulecolor{DarkOrange}
	\begin{longtable}[c]{|>{\tt}r|p{0.3\textwidth}|p{0.5\textwidth}|}
EOF
		print $O '\caption{'.$latexcaption.' aliases} \label{cv:'.$cvname.':alias} \\\\'."\n";
		print $O <<'EOF';
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Alias}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline\hline
\endfirsthead
\multicolumn{3}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Alias}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvKey}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{\cvDesc}}} \\
\hline
\endhead
\hline \multicolumn{3}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline
\endfoot
\endlastfoot
EOF
		
		my $aliashash = $CV->CV;
		foreach my $aliasKey (@{$CV->aliasOrder}) {
			my $alias = $aliashash->{$aliasKey};
			my $termStr = undef;
			
			if(scalar(@{$alias->parents}) > 1) {
#				$termStr = '\begin{tabular}{l}'.join(' \\\\ ',map { latex_escape($_) } @{$alias->parents}).'\end{tabular}';
				$termStr = join(', ',map { latex_escape($_) } @{$alias->parents});
			} else {
				$termStr = $alias->parents->[0];
			}
			
			my $descStr = latex_escape($alias->name);
			print $O join(' & ', latex_escape($aliasKey), $termStr, $descStr),'\\\\ \hline',"\n";
		}

		# Table footer
		print $O <<'EOF';
	\end{longtable}
}
EOF
	}
}

# parseColor parameters:
#	color: a XML::LibXML::Element instance, from the model, with an embedded color
# it returns an array of 2 or 4 elements, where the first one is the LaTeX color model
# (rgb, RGB or HTML), and the next elements are the components (1 compound for HTML, 3 for the others)
sub parseColor($) {
	my($color) = @_;
	
	my $colorModel = undef;
	my @colorComponents = ();
	
	if(ref($color) && $color->isa('XML::LibXML::Element')
		&& $color->namespaceURI eq DCC::Model::dccNamespace
		&& $color->localname eq 'color'
	) {
		my $colorText = $color->textContent();
		if($colorText =~ /^#([0-9a-fA-F]{3,6})$/) {
			my $component = $1;
			# Let's give it an upgrade
			if(length($component)==3) {
				$component = (substr($component,0,1) x 2) . (substr($component,1,1) x 2) . (substr($component,2,1) x 2);
			}
			$colorModel = 'HTML';
			push(@colorComponents,$component);
		} elsif($colorText =~ /^rgb\(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]),([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]),([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\)$/) {
			my $r = $1;
			my $g = $2;
			my $b = $3;
			
			$colorModel = 'RGB';
			push(@colorComponents,$r,$g,$b);
		} elsif($colorText =~ /^rgb\(([0-9]|[1-9][0-9]|100)%,([0-9]|[1-9][0-9]|100)%,([0-9]|[1-9][0-9]|100)%\)$/) {
			my $r = $1;
			my $g = $2;
			my $b = $3;
			
			$colorModel = 'rgb';
			push(@colorComponents,$r/100.0,$g/100.0,$b/100.0);
		}
	}
	
	return ($colorModel,@colorComponents);
}

# This method reads LaTeX files into strings, removing comment lines
# So it could harm any side-effect related to newlines and zero-length comments
sub readLaTeXTemplate($$) {
	my($templateAbsDocDir,$templateFile) = @_;
	
	my $template = '';
	if(open(my $LT,'<',File::Spec->catfile($templateAbsDocDir,$templateFile))) {
		while(my $line=<$LT>) {
			chomp($line);
			# Removing comments
			my $ipos = index($line,'%');
			$line = substr($line,0,$ipos)  if($ipos!=-1);
			$template .= $line."\n"  if(length($line) > 0);
		}
		
		close($LT);
	} else {
		die "ERROR: Unable to read template LaTeX file $templateFile\n";
	}
	
	return $template;
}

# genConceptGraphNode parameters:
#	concept: a DCC::Model::Concept instance
#	p_defaultColorDef: a reference to the default color
#	templateAbsDocDir: The absolute path to the template directory
# It returns the DOT line of the node and the color of this concept
sub genConceptGraphNode($\@$) {
	my($concept,$p_defaultColorDef,$templateAbsDocDir)=@_;
	
	my $conceptDomain = $concept->conceptDomain;
	
	my $entry = entryName($concept);
	my $color = $entry;
	
	my $p_colorDef = $p_defaultColorDef;
	if(exists($concept->annotations->hash->{color})) {
		my @colorDef = parseColor($concept->annotations->hash->{color});
		$p_colorDef = \@colorDef;
	}
	
	my $columnSet = $concept->columnSet;
	my %idColumnNames = map { $_ => undef } @{$concept->columnSet->idColumnNames};
	
	my $latexAttribs = '\graphicspath{{'.File::Spec->catfile($templateAbsDocDir,ICONS_DIR).'/}} \arrayrulecolor{Black} \begin{tabular}{ c l }  \multicolumn{2}{c}{\textbf{\hyperref[tab:'.$entry.']{\Large{}'.latex_escape(exists($concept->annotations->hash->{'altkey'})?$concept->annotations->hash->{'altkey'}:$concept->fullname).'}}} \\\\ \hline ';
	
	my @colOrder = fancyColumnOrdering($concept);
	my $labelPrefix = labelPrefix($concept);
	
	my %partialFKS = ();
	$latexAttribs .= join(' \\\\ ',map {
		my $column = $_;
		if(defined($column->refColumn) && !defined($column->relatedConcept) && $column->refConcept->conceptDomain eq $conceptDomain) {
			my $refEntry = entryName($column->refConcept);
			$partialFKS{$refEntry} = (defined($concept->idConcept) && $column->refConcept eq $concept->idConcept)?1:undef  unless(exists($partialFKS{$refEntry}));
		}
		my $formattedColumnName = latex_escape($column->name);
		
		my $colType = $column->columnType->use;
		my $isId = exists($idColumnNames{$column->name});
		my $icon = undef;
		if($colType eq DCC::Model::ColumnType::DESIRABLE || $colType eq DCC::Model::ColumnType::OPTIONAL) {
			$formattedColumnName = '\textcolor{gray}{'.$formattedColumnName.'}';
		}
		if($colType eq DCC::Model::ColumnType::DESIRABLE || $isId) {
			$formattedColumnName = '\textbf{'.$formattedColumnName.'}';
		}
		# Hyperlinking only to concrete concepts
		my $refConcreteConcept = defined($column->refConcept) && (!$RELEASE || !$column->refConcept->conceptDomain->isAbstract);
		if($refConcreteConcept) {
#			if(defined($column->refConcept) && defined($concept->idConcept) && $column->refConcept eq $concept->idConcept) {
			$formattedColumnName = '\textit{'.$formattedColumnName.'}';
		}
		
		if($isId) {
			$icon = ($refConcreteConcept)?'fkpk':'pk';
		} elsif($refConcreteConcept) {
			$icon = 'fk';
		}
		
		my $image = defined($icon)?('\includegraphics[height=1.6ex]{'.$icon.'.pdf}'):'';
		if($refConcreteConcept) {
			$image = '\hyperref[column:'.labelColumn($column->refConcept,$column->refColumn).']{'.$image.'}';
		}
		
		$image.' & \hyperref[column:'.labelColumn($labelPrefix,$column).']{'.$formattedColumnName.'}'
	} @{$columnSet->columns}{@colOrder});
	
	$latexAttribs .= ' \end{tabular}';
	
	my $doubleBorder = defined($concept->idConcept)?',double distance=2pt':'';
	my $dotline = <<DEOF;
$entry [texlbl="$latexAttribs",style="top color=$color,rounded corners,drop shadow$doubleBorder",margin="0,0"];
DEOF

	return ($dotline, $entry, \%partialFKS, $color => $p_colorDef);
}

# genConceptDomainGraph parameters:
#	model: A DCC::Model instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, from the model
#	figurePrefix: path prefix for the generated figures in .dot and in .latex
#	templateAbsDocDir: Directory of the template being used
#	p_colors: Hash of colors for each concept (to be filled)
sub genConceptDomainGraph($$$$\%) {
	my($model,$conceptDomain,$figurePrefix,$templateAbsDocDir,$p_colors)=@_;
	
	my @defaultColorDef = ('rgb',0,1,0);
	if(exists($conceptDomain->annotations->hash->{defaultColor})) {
		@defaultColorDef = parseColor($conceptDomain->annotations->hash->{defaultColor});
	} elsif(exists($model->annotations->hash->{defaultColor})) {
		@defaultColorDef = parseColor($model->annotations->hash->{defaultColor});
	}

#	node [shape=box,style="rounded corners,drop shadow"];

	my $conceptDomainName = $conceptDomain->name;
	my $dotfile = $figurePrefix . '-domain-'.$conceptDomainName.'.dot';
	my $latexfile = $figurePrefix . '-domain-'.$conceptDomainName.'.latex';
	my $standalonelatexfile = $figurePrefix . '-domain-'.$conceptDomainName.'-standalone.latex';
	if(open(my $DOT,'>:utf8',$dotfile)) {
		print $DOT <<DEOF;
digraph G {
	rankdir=LR;
	node [shape=box];
	edge [arrowhead=none];
	
DEOF
		
		my %fks = ();
		my @firstRank = ();
		# First, the concepts
		foreach my $concept (@{$conceptDomain->concepts}) {
			my($dotline,$entry,$p_partialFKS,$color,$p_colorDef) = genConceptGraphNode($concept,@defaultColorDef,$templateAbsDocDir);
			
			print $DOT $dotline;
			foreach my $refEntry (keys(%{$p_partialFKS})) {
				$fks{$refEntry}{$entry} = $p_partialFKS->{$refEntry};
			}
			$p_colors->{$color} = $p_colorDef;
			
			if(!defined($concept->idConcept) || $concept->idConcept->conceptDomain != $conceptDomain) {
				if(!defined($concept->parentConcept) || $concept->parentConcept->conceptDomain != $conceptDomain) {
					push(@firstRank,$entry);
				}
			}
		}
		
		# Let's print the rank
		print $DOT <<DEOF;
	{ rank=same; @firstRank }
DEOF
		
		# Then, their relationships
		print $DOT <<DEOF;
	
	node [shape=diamond, texlbl="Identifies"];
	
DEOF
		# The FK restrictions from identification relations
		my $relnode = 1;
		my $doubleBorder = 'double distance=2pt';
		foreach my $idEntry (keys(%fks)) {
			my $dEntry = 'ID_'.$idEntry;
			print $DOT <<DEOF;
	
	${dEntry}_$relnode [style="top color=$idEntry,drop shadow,$doubleBorder"];
	$idEntry -> ${dEntry}_$relnode  [label="1"];
DEOF
			foreach my $entry (keys(%{$fks{$idEntry}})) {
				print $DOT <<DEOF;
	${dEntry}_$relnode -> $entry [label="N",style="$doubleBorder"];
DEOF
			}
			$relnode++;
		}
		
		print $DOT <<DEOF;
	
	node [shape=diamond];
	
DEOF
		# The FK restrictions from related concepts
		foreach my $concept (@{$conceptDomain->concepts}) {
			# Let's visit each concept!
			if(scalar(@{$concept->relatedConcepts})>0) {
				my $entry = entryName($concept,$conceptDomainName);
				foreach my $relatedConcept (@{$concept->relatedConcepts}) {
					# Not showing the graphs to abstract concept domains
					next  if($RELEASE && $relatedConcept->concept->conceptDomain->isAbstract);
					
					my $refEntry = entryName($relatedConcept->concept,(defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName));
					
					my $refEntryLine = '';
					
					my $dEntry = $entry.'_'.$refEntry;
					my $port = '';
					my $eport = '';
					my $wport = '';
					if(defined($relatedConcept->conceptDomainName) && $relatedConcept->conceptDomainName ne $conceptDomainName) {
						$port = ':n';
						$eport = ':e';
						$wport = ':w';
						$refEntryLine = $refEntry.' [shape="box",style="top color='.$refEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$refEntry.']{\Large{}'.latex_escape(exists($relatedConcept->concept->annotations->hash->{'altkey'})?$relatedConcept->concept->annotations->hash->{'altkey'}:$relatedConcept->concept->fullname).'}}"];';
					} elsif($relatedConcept->concept eq $concept) {
						$eport = ':n';
						$wport = ':s';
					}
					
					my $arity = $relatedConcept->arity;
					my $doubleBorder = $relatedConcept->isPartial()?'':'double distance=2pt';
					
					my $texlbl = 'Relationship';
					if(defined($relatedConcept->keyPrefix)) {
						$texlbl = '\parbox{3cm}{\centering '.$texlbl.' \linebreak \textit{\small('.latex_escape($relatedConcept->keyPrefix).')}}';
					}
					
					print $DOT <<DEOF;
	
	${dEntry}_$relnode [style="top color=$refEntry,drop shadow",texlbl="$texlbl"];
	$refEntryLine
	$refEntry$port -> ${dEntry}_$relnode$wport [label="$arity"];
	${dEntry}_$relnode$eport -> $entry$port [label="N",style="$doubleBorder"];
DEOF
					$relnode++;
				}
			}
		}
		
		# And the specialization relations
		my %parentConceptNodes = ();
		my $parentEntryLines = '';
		foreach my $concept (@{$conceptDomain->concepts}) {
			# There is one, so..... let's go!
			if($concept->parentConcept) {
				# Not showing the graphs to abstract concept domains
				my $parentConcept = $concept->parentConcept;
				next  if($RELEASE && $parentConcept->conceptDomain->isAbstract);
				
				my $entry = entryName($concept,$conceptDomainName);
				my $parentEntry = entryName($parentConcept);
				my $extendsEntry = $parentEntry.'__extends';
				if(!exists($parentConceptNodes{$parentEntry})) {
					# Let's create the node, if external
					if($parentConcept->conceptDomain!=$conceptDomain) {
						$parentEntryLines .= $parentEntry.' [shape="box",style="top color='.$parentEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$parentEntry.']{\Large{}'.latex_escape(exists($parentConcept->annotations->hash->{'altkey'})?$parentConcept->annotations->hash->{'altkey'}:$parentConcept->fullname).'}}"];'."\n";
					}
					
					# Let's create the (d) node, along with its main arc
					$parentEntryLines .= $extendsEntry.' [shape="triangle",margin="0",style="top color='.$parentEntry.',drop shadow",texlbl="\texttt{d}"];'."\n";
					$parentEntryLines .= $extendsEntry.' -> '.$parentEntry.' [style="double distance=2pt"];'."\n";
					
					$parentConceptNodes{$parentEntry} = undef;
				}
				
				# Let's create the arc to the (d) node
				$parentEntryLines .= $entry.' -> '.$extendsEntry."\n\n";
			}
		}
		print $DOT $parentEntryLines  if(length($parentEntryLines)>0);
		
		# And now, let's close the graph
		print $DOT <<DEOF;
}
DEOF
		close($DOT);
	}
	
	# Prepare the colors
	my $figpreamble = join('',map {
		my $color = $_;
		my($colorModel,@components) = @{$p_colors->{$color}};
		'\definecolor{'.$color.'}{'.$colorModel.'}{'.join(',',@components).'}';
	} keys(%{$p_colors}));
	
	# The moment to call dot2tex
	my $docpreamble = readLaTeXTemplate($FindBin::Bin,FIGURE_PREAMBLE_FILE);
	$docpreamble =~ tr/\n/ /;
	my $fontpreamble = readLaTeXTemplate($templateAbsDocDir,FONTS_TEMPLATE_FILE);
	$fontpreamble =~ tr/\n/ /;
	my @params = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble='.$docpreamble.' '.$fontpreamble,
		'--figpreamble='.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
		'--figonly',
# This backend kills relationships
#		'-ftikz',
		'-o',$latexfile,
		$dotfile
	);
	my @standAloneParams = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble='.$docpreamble.' '.$fontpreamble.' '.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
#		'--prog=circo',
# This backend kills relationships
#		'-ftikz',
		'-o',$standalonelatexfile,
		$dotfile
	);
	system(@params);
	system(@standAloneParams);

	return ($latexfile,$figpreamble);
}

# genModelGraph parameters:
#	model: A DCC::Model instance
#	figurePrefix: path prefix for the generated figures in .dot and in .latex
#	templateAbsDocDir: Directory of the template being used
#	p_colors: Hash of colors for each concept (to be filled)
sub genModelGraph($$$\%) {
	my($model,$figurePrefix,$templateAbsDocDir,$p_colors)=@_;
	
	my @defaultColorDef = ('rgb',0,1,0);
	my @blackColor = ('HTML','000000');
	if(exists($model->annotations->hash->{defaultColor})) {
		@defaultColorDef = parseColor($model->annotations->hash->{defaultColor});
	}
#	node [shape=box,style="rounded corners,drop shadow"];

	my $dotfile = $figurePrefix . '-model.dot';
	my $latexfile = $figurePrefix . '-model.latex';
	my $standalonelatexfile = $figurePrefix . '-model-standalone.latex';
	if(open(my $DOT,'>:utf8',$dotfile)) {
		print $DOT <<DEOF;
digraph G {
	rankdir=LR;
	node [shape=box];
	edge [arrowhead=none];
	
DEOF
		
		# With this, we do not need xcolor
		#$colors{Black} = \@blackColor;
		my $relnode = 1;
		# First, the concepts
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			# Skipping abstract concept domains
			next  if($RELEASE && $conceptDomain->isAbstract);
			
			my @defaultConceptDomainColorDef = @defaultColorDef;
			if(exists($conceptDomain->annotations->hash->{defaultColor})) {
				@defaultConceptDomainColorDef = parseColor($conceptDomain->annotations->hash->{defaultColor});
			}

			my $conceptDomainName = $conceptDomain->name;
			my $conceptDomainFullName = $conceptDomain->fullname;
			print $DOT <<DEOF;
	subgraph cluster_$conceptDomainName {
		label="$conceptDomainFullName"
DEOF
			my %fks = ();
			my @firstRank = ();
			foreach my $concept (@{$conceptDomain->concepts}) {
				my($dotline,$entry,$p_partialFKS,$color,$p_colorDef) = genConceptGraphNode($concept,@defaultConceptDomainColorDef,$templateAbsDocDir);
				
				print $DOT "\t",$dotline;
				foreach my $refEntry (keys(%{$p_partialFKS})) {
					$fks{$refEntry}{$entry} = $p_partialFKS->{$refEntry};
				}
				$p_colors->{$color} = $p_colorDef;
			
				if(!defined($concept->idConcept) || $concept->idConcept->conceptDomain != $conceptDomain) {
					if(!defined($concept->parentConcept) || $concept->parentConcept->conceptDomain != $conceptDomain) {
						push(@firstRank,$entry);
					}
				}
			}
			
		
			# Let's print the rank
			print $DOT <<DEOF;
		{ rank=same; @firstRank }
DEOF
			# Then, their relationships
			print $DOT <<DEOF;
		
		node [shape=diamond, texlbl="Identifies"];
		
DEOF
			# The FK restrictions from identification relations
			my $doubleBorder = 'double distance=2pt';
			foreach my $idEntry (keys(%fks)) {
				my $dEntry = 'ID_'.$idEntry;
				print $DOT <<DEOF;
			
		${dEntry}_$relnode [style="top color=$idEntry,drop shadow,$doubleBorder"];
		$idEntry -> ${dEntry}_$relnode  [label="1"];
DEOF
				foreach my $entry (keys(%{$fks{$idEntry}})) {
					print $DOT <<DEOF;
		${dEntry}_$relnode -> $entry [label="N",style="$doubleBorder"];
DEOF
				}
				$relnode++;
			}
			
			print $DOT <<DEOF;
		
		node [shape=diamond];
		
DEOF
			# The FK restrictions from related concepts
			foreach my $concept (@{$conceptDomain->concepts}) {
				my $conceptDomainName = $conceptDomain->name;
				my $entry = entryName($concept,$conceptDomainName);
				# Let's visit each concept!
				if(scalar(@{$concept->relatedConcepts})>0) {
					foreach my $relatedConcept (@{$concept->relatedConcepts}) {
						# Skipping relationships to abstract concept domains
						next  if($RELEASE && $relatedConcept->concept->conceptDomain->isAbstract);
						
						my $refEntry = entryName($relatedConcept->concept,(defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName));
						
						my $refEntryLine = '';
						
						my $dEntry = $entry.'_'.$refEntry;
#						my $port = '';
#						my $eport = '';
#						my $wport = '';
#						if(defined($relatedConcept->conceptDomainName) && $relatedConcept->conceptDomainName ne $conceptDomainName) {
#							$port = ':n';
#							$eport = ':e';
#							$wport = ':w';
#							$refEntryLine = $refEntry.' [shape="box",style="top color='.$refEntry.',rounded corners,drop shadow",texlbl="\textbf{\hyperref[tab:'.$refEntry.']{\Large{}'.latex_escape(exists($relatedConcept->concept->annotations->hash->{'altkey'})?$relatedConcept->concept->annotations->hash->{'altkey'}:$relatedConcept->concept->fullname).'}}"];';
#						} elsif($relatedConcept->concept eq $concept) {
#							$eport = ':n';
#							$wport = ':s';
#						}
						
						my $arity = $relatedConcept->arity;
						my $doubleBorder = $relatedConcept->isPartial()?'':'double distance=2pt';
						
						my $texlbl = 'Relationship';
						if(defined($relatedConcept->keyPrefix)) {
							$texlbl = '\parbox{3cm}{\centering '.$texlbl.' \linebreak \textit{\small('.latex_escape($relatedConcept->keyPrefix).')}}';
						}
						
						print $DOT <<DEOF;
		
		${dEntry}_$relnode [style="top color=$refEntry,drop shadow",texlbl="$texlbl"];
		$refEntry -> ${dEntry}_$relnode [label="$arity"];
		${dEntry}_$relnode -> $entry [label="N",style="$doubleBorder"];
DEOF
						$relnode++;
					}
				}
			}
			# And now, let's close the subgraph
			print $DOT <<DEOF;
	}
DEOF
		}
		
		# And the specialization relations!!!
		my %parentConceptNodes = ();
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			# Skipping abstract concept domains
			next  if($RELEASE && $conceptDomain->isAbstract);
			
			my $conceptDomainName = $conceptDomain->name;
			my $parentEntryLines = '';
			foreach my $concept (@{$conceptDomain->concepts}) {
				# There is one, so..... let's go!
				if($concept->parentConcept) {
					# Not showing the graphs to abstract concept domains
					my $parentConcept = $concept->parentConcept;
					next  if($RELEASE && $parentConcept->conceptDomain->isAbstract);
					
					my $entry = entryName($concept,$conceptDomainName);
					my $parentEntry = entryName($parentConcept);
					my $extendsEntry = $parentEntry.'__extends';
					if(!exists($parentConceptNodes{$parentEntry})) {
						# Let's create the (d) node, along with its main arc
						$parentEntryLines .= $extendsEntry.' [shape="triangle",margin="0",style="top color='.$parentEntry.',drop shadow",texlbl="\texttt{d}"];'."\n";
						$parentEntryLines .= $extendsEntry.' -> '.$parentEntry.' [style="double distance=2pt"];'."\n";
						
						$parentConceptNodes{$parentEntry} = undef;
					}
					
					# Let's create the arc to the (d) node
					$parentEntryLines .= $entry.' -> '.$extendsEntry."\n\n";
				}
			}
			print $DOT $parentEntryLines  if(length($parentEntryLines)>0);
		}

		# And now, let's close the graph
		print $DOT <<DEOF;
}
DEOF
		close($DOT);
	}
	
	# Prepare the colors
	my $figpreamble = join('',map {
		my $color = $_;
		my($colorModel,@components) = @{$p_colors->{$color}};
		'\definecolor{'.$color.'}{'.$colorModel.'}{'.join(',',@components).'}';
	} keys(%{$p_colors}));
	
	# The moment to call dot2tex
	my $docpreamble = readLaTeXTemplate($FindBin::Bin,FIGURE_PREAMBLE_FILE);
	$docpreamble =~ tr/\n/ /;
	my $fontpreamble = readLaTeXTemplate($templateAbsDocDir,FONTS_TEMPLATE_FILE);
	$fontpreamble =~ tr/\n/ /;
	my @params = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble='.$docpreamble.' '.$fontpreamble,
		'--figpreamble='.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
		'--figonly',
# This backend kills relationships
#		'-ftikz',
		'-o',$latexfile,
		$dotfile
	);
	my @standAloneParams = (
		'dot2tex',
		'--usepdflatex',
		'--docpreamble='.$docpreamble.' '.$fontpreamble.' '.$figpreamble,
		'--autosize',
#		'--nominsize',
		'-c',
		'--preproc',
#		'--prog=circo',
# This backend kills relationships
#		'-ftikz',
		'-o',$standalonelatexfile,
		$dotfile
	);
	system(@params);
	system(@standAloneParams);
	
	return ($latexfile,$figpreamble);
}

# printConceptDomain parameters:
#	model: A DCC::Model instance
#	conceptDomain: A DCC::Model::ConceptDomain instance, from the model
#	figurePrefix: The prefix path for the generated figures (.dot, .latex, etc...)
#	templateAbsDocDir: Directory of the template being used
#	O: The filehandle where to print the documentation about the concept domain
#	p_colors: The colors to be taken into account when the graphs are generated
sub printConceptDomain($$$$$\%) {
	my($model,$conceptDomain,$figurePrefix,$templateAbsDocDir,$O,$p_colors)=@_;
	
	
	my $latexDefaultValue = exists($model->nullCV->annotations->hash->{default})?'\textbf{\textit{\color{black}'.latex_escape($model->nullCV->annotations->hash->{default}).'}}':'the default value';
	
	# The title
	# TODO: consider using encode('latex',...) for the content
	my $conceptDomainName = $conceptDomain->name;
	my $conceptDomainFullname = $conceptDomain->fullname;
	print $O '\\section{'.latex_format($conceptDomainFullname).'}\\label{fea:'.$conceptDomainName."}\n";
	
	# Printing embedded documentation in the model
	foreach my $documentation (@{$conceptDomain->description}) {
		printDescription($O,$documentation);
	}
	my $cDomainAnnotationSet = $conceptDomain->annotations;
	my $cDomainAnnotations = $cDomainAnnotationSet->hash;
	foreach my $annotKey (@{$cDomainAnnotationSet->order}) {
		printDescription($O,$cDomainAnnotations->{$annotKey},$annotKey);
	}
	# The additional documentation
	# The concept domain holds its additional documentation in a subdirectory with
	# the same name as the concept domain
	my $domainDocDir = File::Spec->catfile($model->documentationDir,$conceptDomainName);
	my $docfile = File::Spec->catfile($domainDocDir,$TOCFilename);
	
	if(-f $docfile && -r $docfile){
		# Full paths, so import instead of subimport
		print $O "\\import*\{$domainDocDir/\}\{$TOCFilename\}\n";
	}
	
	# generate dot graph representing the concept domain
	my($conceptDomainLatexFile,$figDomainPreamble) = genConceptDomainGraph($model,$conceptDomain,$figurePrefix,$templateAbsDocDir,%{$p_colors});
	
	my(undef,$absLaTeXDir,$relLaTeXFile) = File::Spec->splitpath($conceptDomainLatexFile);
	
	#my $gdef = '\graphicspath{{'.File::Spec->catfile($templateAbsDocDir,ICONS_DIR).'/}}';
	print $O <<GEOF;
\\par
$figDomainPreamble
{%
%\\begin{figure*}[!h]
\\centering
%\\resizebox{.95\\linewidth}{!}{
\\maxsizebox{.95\\textwidth}{.4\\textheight}{
\\hypersetup{
	linkcolor=Black
}
%\\input{$conceptDomainLatexFile}
\\import*{$absLaTeXDir/}{$relLaTeXFile}
}
\\captionof{figure}{$conceptDomainFullname Sub-Schema}
%\\end{figure*}
}
GEOF
	
	# Let's iterate over the concepts of this concept domain
	foreach my $concept (@{$conceptDomain->concepts}) {
		my $columnSet = $concept->columnSet;
		my %idColumnNames = map { $_ => undef } @{$concept->columnSet->idColumnNames};
		
		# The embedded documentation of each concept
		my $caption = latex_format($concept->fullname);
		print $O '\\subsection{'.$caption."}\n";
		foreach my $documentation (@{$concept->description}) {
			printDescription($O,$documentation);
		}
		my $annotationSet = $concept->annotations;
		my $annotations = $annotationSet->hash;
		foreach my $annotKey (@{$annotationSet->order}) {
			printDescription($O,$annotations->{$annotKey},$annotKey);
		}
		
		# The relation to the extended concept
		if($concept->parentConcept && (!$concept->parentConcept->conceptDomain->isAbstract || !$RELEASE)) {
			print $O '\textit{This concept extends \hyperref[tab:'.entryName($concept->parentConcept).']{'.$concept->parentConcept->fullname.'}}'."\n";
		}
		
		# The table header
		my $entry = entryName($concept,$conceptDomainName);
#	\begin{tabularx}{\linewidth}{|>{\setlength{\hsize}{.5\hsize}\raggedright\arraybackslash}X|c|c|p{0.5\textwidth}|}
		print $O <<'EOF';
{
	\arrayrulecolor{DarkOrange}
EOF
#		print $O '\topcaption{'.$caption.'\label{tab:'.$entry.'}}'."\n";
#		print $O <<'EOF';
#\tablefirsthead{%
#\hline
#\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
#\hline\hline}
#\tablehead{%
#\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
#\hline
#\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
#\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
#\hline}
#\tabletail{%
#\hline
#\multicolumn{4}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
#\hline}
#
#	\begin{supertabular}{|l|c|c|p{0.5\textwidth}|}
#EOF
		print $O <<'EOF';
	\begin{longtable}[c]{|>{\maxsizebox{3.5cm}{!}\bgroup}l<{\egroup}|>{\texttt\bgroup}c<{\egroup}|>{\texttt\bgroup}c<{\egroup}|>{\raggedright\arraybackslash}p{0.5\textwidth}|}
EOF
		print $O '\caption{'.$caption.'\label{tab:'.$entry.'}} \\\\'."\n";
		print $O <<'EOF';
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
\hline\hline
\endfirsthead
\multicolumn{4}{c}{{\bfseries \tablename\ \thetable{} -- continued from previous page}} \\
\hline
\multicolumn{1}{|c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Name}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Type}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Need}}} &
\multicolumn{1}{c|}{\cellcolor{DarkOrange}\textcolor{white}{\textbf{Description / Values}}} \\
\hline
\endhead
\hline
\multicolumn{4}{|r|}{\textcolor{gray}{\textit{Continued on next page}}} \\
\hline
\endfoot
\hline
\endlastfoot
EOF
		
		# Determining the order of the columns
		my @colorder = fancyColumnOrdering($concept);
		
		# Now, let's print the documentation of each column
		foreach my $column (@{$columnSet->columns}{@colorder}) {
			my @descriptionItems = ();
			# Preparing the documentation
			my $description='';
			foreach my $documentation (@{$column->description}) {
				$description = "\n\n" if(length($description)>0);
				$description .= latex_format($documentation);
			}
			push(@descriptionItems,$description);
			
			# Only references to concepts is non abstract concept domains
			if(defined($column->refColumn) && (!$RELEASE || !$column->refConcept->conceptDomain->isAbstract)) {
				my $related = '\\textcolor{gray}{Relates to \\textit{\\hyperref[column:'.labelColumn($column->refConcept,$column->refColumn).']{'.latex_escape($column->refConcept->fullname.' ('.$column->refColumn->name.')').'}}}';
				push(@descriptionItems,$related);
			}
			
			# The comment about the values
			my $values='';
			if(exists($column->annotations->hash->{values})) {
				$values = '\\textit{'.latex_format($column->annotations->hash->{values}).'}';
			}
			
			my $columnType = $column->columnType;
			
			# The default value
			if(defined($columnType->default)) {
				my $related = '\textcolor{gray}{If it is set to '.$latexDefaultValue.', the default value for this column is '.(ref($columnType->default)?'from ':'').'\textbf{\texttt{\color{black}'.(ref($columnType->default)?('\hyperref[column:'.labelColumn($concept,$columnType->default).']{'.latex_escape($columnType->default->name).'}'):latex_escape($columnType->default)).'}}}';
				push(@descriptionItems,$related);
			}
			
			# Now the possible CV
			my $restriction = $columnType->restriction;
			if(ref($restriction) eq 'DCC::Model::CV') {
				# Is it an anonymous CV?
				my $numterms = scalar(@{$restriction->order});
				if($numterms < 20 && (!defined($restriction->name) || (exists($restriction->annotations->hash->{'disposition'}) && $restriction->annotations->hash->{disposition} eq 'inline'))) {
					$values .= processInlineCVTable($restriction);
				} else {
					my $cv = $restriction->name;
					$values .= "\n".'\textit{(See \hyperref[cvsec:'.$cv.']{'.(($restriction->kind() ne DCC::Model::CV::URIFETCHED)?'CV':'external CV description').' \ref*{cvsec:'.$cv.'}})}';
				}
			}
			
			### HACK ###
			if(ref($restriction) eq 'DCC::Model::CompoundType') {
				my $rColumnSet = $restriction->columnSet;
				foreach my $rColumnName (@{$rColumnSet->columnNames}) {
					my $rColumn = $rColumnSet->columns->{$rColumnName};
					my $rRestr = $rColumn->columnType->restriction;
					
					if(defined($rRestr) && ref($rRestr) eq 'DCC::Model::CV') {
						$values .= "\n".'\textit{\texttt{\textbf{'.$rColumnName.'}}}';
						# Is it an anonymous CV?
						my $numterms = scalar(@{$rRestr->order});
						if($numterms < 20 && (!defined($rRestr->name) || (exists($rRestr->annotations->hash->{'disposition'}) && $rRestr->annotations->hash->{disposition} eq 'inline'))) {
							$values .= "\n".processInlineCVTable($rRestr);
						} else {
							my $cv = $rRestr->name;
							$values .= '$\mapsto$ \textit{(See \hyperref[cvsec:'.$cv.']{'.(($rRestr->kind() ne DCC::Model::CV::URIFETCHED)?'CV':'external CV description').' \ref*{cvsec:'.$cv.'}})}';
						}
					}
				}
			}
			
			push(@descriptionItems,$values)  if(length($values)>0);
			
			# What it goes to the column type column
			my @colTypeLines = ('\textbf{'.latex_escape($columnType->type.('[]' x length($columnType->arraySeps))).'}');
			
			push(@colTypeLines,'\textit{\maxsizebox{2cm}{!}{'.latex_escape($restriction->template).'}}')  if(ref($restriction) eq 'DCC::Model::CompoundType');
			
			push(@colTypeLines,'\textcolor{gray}{\maxsizebox{2cm}{!}{(array seps \textbf{\color{black}'.latex_escape($columnType->arraySeps).'})}}')  if(defined($columnType->arraySeps));
			
			#push(@colTypeLines,'\textcolor{gray}{\maxsizebox{2cm}{!}{(default \textbf{\color{black}'.(ref($columnType->default)?('\hyperref[column:'.labelColumn($concept,$columnType->default).']{'.latex_escape($columnType->default->name).'}'):latex_escape($columnType->default)).'})}}')  if(defined($columnType->default));
			
			# Stringify it!
			my $colTypeStr = (scalar(@colTypeLines)>1)?
#					'\begin{tabular}{l}'.join(' \\\\ ',map { latex_escape($_) } @colTypeLines).'\end{tabular}'
#					'\begin{minipage}[t]{8em}'.join(' \\\\ ',@colTypeLines).'\end{minipage}'
					'\pbox[t]{10cm}{\relax\ifvmode\centering\fi'."\n".join(' \\\\ ',@colTypeLines).'}'
					:
					$colTypeLines[0]
			;
			
			print $O join(' & ',
				'\label{column:'.labelColumn($concept,$column).'}'.latex_escape($column->name),
				$colTypeStr,
				$COLKIND2ABBR{($columnType->use!=DCC::Model::ColumnType::IDREF || exists($idColumnNames{$column->name}))?$columnType->use:DCC::Model::ColumnType::REQUIRED},
				join("\n\n",@descriptionItems)
			),'\\\\ \hline',"\n";
		}
		# End of the table!
		print $O <<'EOF';
	\end{longtable}
}
EOF
	}
	
	# And at last, optional notes about this!
	my $notesfile = File::Spec->catfile($domainDocDir,$NotesFilename);
	
	if(-f $notesfile && -r $notesfile){
		print $O '\\subsection{Further notes on '.latex_format($conceptDomain->fullname)."}\n";
		# Full paths, so import instead of subimport
		print $O "\\import*\{$domainDocDir/\}\{$NotesFilename\}\n";
	}
	
}

# Flag validation
my $onlySQL = undef;
while(scalar(@ARGV)>0 && substr($ARGV[0],0,2) eq '--') {
	my $flag = shift(@ARGV);
	if($flag eq '--sql') {
		$onlySQL = 1;
	} elsif($flag eq '--showAbstract') {
		$RELEASE = undef;
	} else {
		print STDERR "Unknown flag $flag. This program takes as input: optional --sql or --showAbstract flags, the model (in XML or BPModel formats), the documentation template directory and the output file\n";
		exit 1;
	}
}

if(scalar(@ARGV)>=3) {
	my($modelFile,$templateDocDir,$out)=@ARGV;
	
	binmode(STDERR,':utf8');
	binmode(STDOUT,':utf8');
	my $model = undef;
	
	# Preparing the absolute template documentation directory
	# and checking whether it exists
	my $templateAbsDocDir = $templateDocDir;
	unless(File::Spec->file_name_is_absolute($templateDocDir)) {
		$templateAbsDocDir = File::Spec->catfile($FindBin::Bin,REL_TEMPLATES_DIR,$templateDocDir);
	}
	
	unless(-d $templateAbsDocDir) {
		print STDERR "ERROR: Template directory $templateDocDir (treated as $templateAbsDocDir) does not exist!\n";
		exit 2;
	}
	
	eval {
		$model = DCC::Model->new($modelFile);
	};
	
	if($@) {
		print STDERR "ERROR: Model loading and validation failed. Reason: ".$@,"\n";
		exit 2;
	}
	
	# In case $out is a directory, then fill-in the other variables
	my $outfilePDF = undef;
	my $outfileSQL = undef;
	my $outfileTranslateSQL = undef;
	my $outfileBPMODEL = undef;
	my $outfileLaTeX = undef;
	my $outfileSH = undef;
	my $figurePrefix = undef;
	if(-d $out) {
		my (undef,undef,undef,$day,$month,$year) = localtime();
		# Doing numerical adjustments
		$year += 1900;
		$month ++;
		my $thisdate = sprintf("%d%.2d%.2d",$year,$month,$day);
		
		my $outfileRoot = File::Spec->catfile($out,join('-',$model->projectName,'data_model',$model->versionString,$thisdate));
		$outfilePDF = $outfileRoot . '.pdf';
		$outfileSQL = $outfileRoot . '.sql';
		$outfileTranslateSQL = $outfileRoot . '_CVtrans.sql';
		$outfileBPMODEL = $outfileRoot . '.bpmodel';
		$figurePrefix = File::Spec->file_name_is_absolute($outfileRoot)?$outfileRoot:File::Spec->rel2abs($outfileRoot);
	} else {
		$outfilePDF = $out;
		$outfileSQL = $out.'.sql';
		$outfileTranslateSQL = $out.'_CVtrans.sql';
		$outfileLaTeX = $out.'.latex';
		$outfileSH = $outfileLaTeX.'.sh';
		$outfileBPMODEL = $out . '.bpmodel';
		$figurePrefix = $out;
	}
	
	# Generating the bpmodel bundle (if it is reasonable)
	$model->saveBPModel($outfileBPMODEL)  if(defined($outfileBPMODEL));
	
	# Generating the SQL file for BioMart
	genSQL($model,$outfileSQL,$outfileTranslateSQL);
	
	# We finish here if only 
	exit 0  if($onlySQL);
	
	# Generating the graph model
	my %colors = ();
	my($modelgraphfile,$modelpreamble) = genModelGraph($model,$figurePrefix,$templateAbsDocDir,%colors);

	# Generating the document to be included in the template
	my $TO = undef;
	
	if(defined($outfileLaTeX)) {
		open($TO,'>:utf8',$outfileLaTeX) || die "ERROR: Unable to create output LaTeX file";
	} else {
		$TO = File::Temp->new();
		binmode($TO,':utf8');
		$outfileLaTeX = $TO->filename;
	}
	
	# Model graph
	my $modelName = latex_escape($model->projectName.' '.$model->schemaVer);
	my(undef,$absModelDir,$relModelFile) = File::Spec->splitpath($modelgraphfile);

	print $TO <<TEOF;
\\newpage
\\newlength{\\modelheight}
\\newlength{\\modelwidth}
\\newsavebox{\\modelbox}

\\savebox{\\modelbox}{%
\\hypersetup{
	linkcolor=Black
}
\\import*{$absModelDir/}{$relModelFile}
}
\\setlength{\\modelheight}{\\ht\\modelbox}
% Total height
\\addtolength{\\modelheight}{\\dp\\modelbox}
\\setlength{\\modelwidth}{\\wd\\modelbox}

\\ifthenelse{\\modelheight<\\modelwidth}{%then
% This is what must be done to center landscape figures
\\begin{landscape}
%\\parbox[c][\\textwidth][s]{\\linewidth}{%
\\vfill
\\centering
%\\resizebox{\\linewidth}{!}{
%\\usebox{\\modelbox}
%}
\\maxsizebox{.95\\textwidth}{.95\\textheight}{
\\usebox{\\modelbox}
}
\\captionof{figure}{Overview of $modelName data model}
\\vfill
%}
\\end{landscape}
}{%else
{
\\vfill
\\centering
%\\resizebox{\\linewidth}{!}{
%\\usebox{\\modelbox}
%}
\\maxsizebox{.95\\textwidth}{.95\\textheight}{
\\usebox{\\modelbox}
}
\\captionof{figure}{Overview of $modelName data model}
\\vfill
}
}
TEOF
	
	print $TO '\chapter{DCC Submission Tabular Formats}\label{ch:tabFormat}',"\n";
	
	# Let's iterate over all the concept domains and their concepts
	foreach my $conceptDomain (@{$model->conceptDomains}) {
		# Skipping abstract concept domains on documentation generation
		next  if($RELEASE && $conceptDomain->isAbstract);
		
		printConceptDomain($model,$conceptDomain,$figurePrefix,$templateAbsDocDir,$TO,%colors);
	}

	print $TO "\\appendix\n";
	print $TO "\\chapter{Controlled Vocabularies}\n";
	
	foreach my $CV (@{$model->namedCVs}) {
		unless(exists($CV->annotations->hash->{disposition}) && $CV->annotations->hash->{disposition} eq 'inline') {
			printCVTable($CV,$TO);
		}
	}
	# Flushing the temp file
	$TO->flush();
	
	# Now, let's generate the documentation!
	assemblePDF($templateAbsDocDir,$model,$outfileBPMODEL,$outfileLaTeX,$outfilePDF,$outfileSH);
} else {
	print STDERR "This program takes as input: optional --sql flag, the model (in XML or BPModel formats), the documentation template directory and the output file\n";
	exit 1;
}
