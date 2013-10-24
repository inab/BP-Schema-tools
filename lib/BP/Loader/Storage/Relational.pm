#!/usr/bin/perl -W

use strict;
use Carp;

use Config::IniFiles;
use File::Basename;
use File::Spec;

use BP::Model;

use MongoDB;

package BP::Loader::Storage::Relational;

use base qw(BP::Loader::Storage);

# Global variable (using "my" because "our" could have too much scope)
my $RELEASE = 1;

our $SECTION;
BEGIN {
	$SECTION = 'relational';
	$BP::Loader::Storage::storage_names{$SECTION}=__PACKAGE__;
};

my @DEFAULTS = (
	['file-prefix' => 'model']
#	['db' => undef],
#	['host' => undef],
#	['port' => 27017],
#	['batch-size' => 20000]
);

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

# Constructor parameters:
#	model: a BP::Model instance
#	config: a Config::IniFiles instance
sub new($$) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	# my $self  = $class->SUPER::new();
	my $self = {};
	bless($self,$class);
	
	$self->{model} = shift;
	
	my $config = shift;
	
	if(scalar(@DEFAULTS)>0) {
		if($config->SectionExists($SECTION)) {
			foreach my $param (@DEFAULTS) {
				my($key,$defval) = @{$param};
				
				if(defined($defval)) {
					$self->{$key} = $config->val($SECTION,$key,$defval);
				} elsif($config->exists($SECTION,$key)) {
					$self->{$key} = $config->val($SECTION,$key);
				} else {
					Carp::croak("ERROR: required parameter $key not found in section $SECTION");
				}
			}
		} else {
			Carp::croak("ERROR: Unable to read section $SECTION");
		}
	}
	
	return $self;
}

sub isHierarchical {
	return undef;
}

# setFilePrefix parameters:
#	newPrefix: the new prefix for the generated files
sub setFilePrefix($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	$self->{'file-prefix'} = shift;
}

sub __sql_escape($) {
	my $par = shift;
	$par =~ s/'/''/g;
	return '\''.$par.'\'';
}

sub __entryName($;$) {
	my($concept,$conceptDomainName)=@_;
	
	$conceptDomainName = $concept->conceptDomain->name  unless(defined($conceptDomainName));
	
	return $conceptDomainName.'_'.$concept->name;
}

# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
# It returns a list of absolute paths to the generated files
sub generateNativeModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $workingDir = shift;
	my $filePrefix = $self->{'file-prefix'};
	my $fullFilePrefix = File::Spec->catfile($workingDir,$filePrefix);
	
#	model: a BP::Model instance, with the parsed model.
	my $model = $self->{model};
#	the path to the SQL output file
	my $outfileSQL = $fullFilePrefix.'.sql';
#	the path to the SQL script which joins 
	my $outfileTranslateSQL = $fullFilePrefix.'_CVtrans.sql';
	
	# Needed later for CV dumping et al
	my @cvorder = ();
	my %cvdump = ();
	my $chunklines = 4096;
	my $descType = $ABSTYPE2SQL{'string'};
	my $aliasType = $ABSTYPE2SQL{'boolean'};
	
	if(open(my $SQL,'>:utf8',$outfileSQL)) {
		print $SQL '-- File '.File::Basename::basename($outfileSQL)."\n";
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
				my $basename = __entryName($concept,$conceptDomainName);

				my %fkselemrefs = ();
				my @fkselem = ($basename,$concept,\%fkselemrefs);
				my $fksinit = undef;

				print $SQL "\n-- ",$concept->fullname;
				print $SQL "\nCREATE TABLE $basename (";
				
				
				my @colorder = BP::Loader::Storage::_fancyColumnOrdering($concept);
				my $columnSet = $concept->columnSet;
				my $gottable=undef;
				
				foreach my $column (@{$columnSet->columns}{@colorder}) {
					# Is it involved in a foreign key outside the relatedConcept system?
					if(defined($column->refColumn) && !defined($column->relatedConcept)) {
						$fksinit = 1;
						
						my $refConcept = $column->refConcept;
						my $refBasename = __entryName($refConcept);
						
						$fkselemrefs{$refBasename} = [$refConcept,[]]  unless(exists($fkselemrefs{$refBasename}));
						
						push(@{$fkselemrefs{$refBasename}[1]}, $column);
					}
					
					# Let's print
					print $SQL ','  if(defined($gottable));
					
					my $columnType = $column->columnType;
					my $SQLtype = ($columnType->use == BP::Model::ColumnType::IDREF || defined($column->refColumn))?$ABSTYPE2SQLKEY{$columnType->type}:$ABSTYPE2SQL{$columnType->type};
					# Registering CVs
					if(defined($columnType->restriction) && $columnType->restriction->isa('BP::Model::CV')) {
						# At the end is a key outside here, so assuring it is using the right size
						# due restrictions on some SQL (cough, cough, MySQL, cough, cough) implementations
						$SQLtype = $ABSTYPE2SQLKEY{$columnType->type};
						my $CV = $columnType->restriction;
						
						my $cvname = $CV->id;
						#$cvname = $basename.'_'.$column->name  unless(defined($cvname));
						# Perl reference trick to get a number
						#$cvname = 'anon_'.($CV+0)  unless(defined($cvname));
						
						# Second position is the SQL type
						# Third position holds the columns which depend on this CV
						unless(exists($cvdump{$cvname})) {
							$cvdump{$cvname} = [$CV,$p_TYPES->{$columnType->type}[BP::Model::ColumnType::ISNOTNUMERIC],$SQLtype,[]];
							push(@cvorder,$cvname);
						}
						
						# Saving the column and table name for further use
						push(@{$cvdump{$cvname}[3]},[$column->name,$basename]);
					}
					
					print $SQL "\n\t",$column->name,' ',$SQLtype;
					print $SQL ' NOT NULL'  if($columnType->use >= BP::Model::ColumnType::IDREF);
					if(defined($columnType->default) && ref($columnType->default) eq '') {
						my $default = $columnType->default;
						$default = __sql_escape($default)  if($p_TYPES->{$columnType->type}[BP::Model::ColumnType::ISNOTNUMERIC]);
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
			#	my $basename = __entryName($concept,$conceptDomainName);
			#	if(defined($concept->idConcept)) {
			#		my $idConcept = $concept->idConcept;
			#		my $refColnames = $idConcept->columnSet->idColumnNames;
			#		my $idBasename = __entryName($idConcept,$conceptDomainName);
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
			print $TSQL '-- File '.File::Basename::basename($outfileTranslateSQL)."\n";
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
					print $SQL (($first>0)?",\n":''),'(',join(',',($doEscape)?__sql_escape($term->key):$term->key,__sql_escape($term->name),($term->isAlias)?'TRUE':'FALSE'),')';
					
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
					my $ekey = ($doEscape)?__sql_escape($term->key):$term->key;
					
					foreach my $akey (@{$term->keys}) {
						if($first==0) {
							print $SQL <<CVEOF;
INSERT INTO ${cvname}_CVkeys VALUES
CVEOF
						}
						print $SQL (($first>0)?",\n":''),'(',join(',',($doEscape)?__sql_escape($akey):$akey,$ekey),')';
						
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
					my $ekey = ($doEscape)?__sql_escape($term->key):$term->key;
					
					next  unless(defined($term->parents) && scalar(@{$term->parents})>0);
					
					foreach my $pkey (@{$term->parents}) {
						if($first==0) {
							print $SQL <<CVEOF;
INSERT INTO ${cvname}_CVparents VALUES
CVEOF
						}
						print $SQL (($first>0)?",\n":''),'(',join(',',$ekey,($doEscape)?__sql_escape($pkey):$pkey),')';
						
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
					my $ekey = ($doEscape)?__sql_escape($term->key):$term->key;
					
					next  unless(defined($term->ancestors) && scalar(@{$term->ancestors})>0);
					
					foreach my $pkey (@{$term->ancestors}) {
						if($first==0) {
							print $SQL <<CVEOF;
INSERT INTO ${cvname}_CVancestors VALUES
CVEOF
						}
						print $SQL (($first>0)?",\n":''),'(',join(',',$ekey,($doEscape)?__sql_escape($pkey):$pkey),')';
						
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
				my $basename = __entryName($concept,$conceptDomainName);
				
				# Let's visit each concept!
				if(scalar(@{$concept->relatedConcepts})>0) {
					print $SQL "\n-- ",$concept->fullname, " foreign keys from related-to";
					my $cycle = 1;
					foreach my $relatedConcept (@{$concept->relatedConcepts}) {
						# Skipping foreign keys to abstract concepts
						next  if($RELEASE && $relatedConcept->concept->conceptDomain->isAbstract);
						
						my $refBasename = __entryName($relatedConcept->concept,(defined($relatedConcept->conceptDomainName)?$relatedConcept->conceptDomainName:$conceptDomainName));
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
	
	return [$outfileSQL, $outfileTranslateSQL];
}

1;
