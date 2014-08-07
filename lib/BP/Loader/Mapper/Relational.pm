#!/usr/bin/perl -W

use strict;
use Carp;

use Config::IniFiles;
use DBI;
use File::Basename;
use File::Spec;
use JSON;
use Tie::IxHash;
use XML::LibXML;

use BP::Model;

package BP::Loader::Mapper::Relational;

use Scalar::Util qw(blessed);

use base qw(BP::Loader::Mapper);

our $SECTION;
BEGIN {
	$SECTION = 'relational';
	$BP::Loader::Mapper::storage_names{$SECTION}=__PACKAGE__;
};

use constant {
	CONF_DB	=>	'db',
	CONF_HOST	=>	'host',
	CONF_PORT	=>	'port',
	CONF_DBUSER	=>	'user',
	CONF_DBPASS	=>	'pass',
	CONF_DIALECT	=>	'sql-dialect',
};

my @DEFAULTS = (
	[BP::Loader::Mapper::Relational::CONF_DIALECT => 'mysql'],
	['batch-size' => 4096],
	[BP::Loader::Mapper::Relational::CONF_DB	=> undef ],
	[BP::Loader::Mapper::Relational::CONF_HOST	=> '' ],
	[BP::Loader::Mapper::Relational::CONF_PORT	=> '' ],
	[BP::Loader::Mapper::Relational::CONF_DBUSER	=> '' ],
	[BP::Loader::Mapper::Relational::CONF_DBPASS	=> '' ],
);

# Static methods to prepare the data to write (data mangling)
sub __bypass($) {
	$_[0]
}

sub __boolean2dbi($) {
	defined($_[0])?($_[0]?1:0):undef;
}

# It should be set in ODBC TIMESTAMP format
sub __timestamp2dbi($) {
	defined($_[0])?($_[0]->ymd.' '.$_[0]->hms):undef;
}

my %ABSTYPE2SQL = (
	BP::Model::ColumnType::STRING_TYPE	=> ['VARCHAR(1024)',DBI::SQL_VARCHAR,\&__bypass],
	BP::Model::ColumnType::TEXT_TYPE	=> ['TEXT',DBI::SQL_VARCHAR,\&__bypass],
	BP::Model::ColumnType::INTEGER_TYPE	=> ['INTEGER',DBI::SQL_INTEGER,\&__bypass],
	BP::Model::ColumnType::DECIMAL_TYPE	=> ['DOUBLE PRECISION',DBI::SQL_DOUBLE,\&__bypass],
	BP::Model::ColumnType::BOOLEAN_TYPE	=> ['BOOL',DBI::SQL_BOOLEAN,\&__boolean2dbi],
	BP::Model::ColumnType::TIMESTAMP_TYPE	=> ['DATETIME',DBI::SQL_DATETIME,\&__timestamp2dbi],
	BP::Model::ColumnType::DURATION_TYPE	=> ['VARCHAR(128)',DBI::SQL_VARCHAR,\&__bypass],
	BP::Model::ColumnType::COMPOUND_TYPE	=> ['TEXT',DBI::SQL_VARCHAR,undef],
);

my %ABSTYPE2SQLKEY = %ABSTYPE2SQL;
$ABSTYPE2SQLKEY{BP::Model::ColumnType::STRING_TYPE} = ['VARCHAR(128)',DBI::SQL_VARCHAR,\&__bypass];


my @FALSE_TRUE = ('FALSE','TRUE');
my @FALSE_TRUE_FAKE = (0,1);

sub _CV_SQL_UPDATE__mysql($$$);
sub _CV_SQL_UPDATE__postgresql($$$);
sub _CV_SQL_UPDATE__sqlite3($$$);

use constant {
	_SQLDIALECT_DRIVER	=>	0,
	_SQLDIALECT_DSNPARAMS	=>	1,
	_SQLDIALECT_TYPE2SQL	=>	2,
	_SQLDIALECT_TYPE2SQLKEY	=>	3,
	_SQLDIALECT_UPDATE_CV	=>	4,
	_SQLDIALECT_FALSE_TRUE	=>	5,
};


my %SQLDIALECTS = (
	'mysql'		=>	[
					'mysql',
					{
						BP::Loader::Mapper::Relational::CONF_DB	=> 'database',
						BP::Loader::Mapper::Relational::CONF_HOST	=> 'host',
						BP::Loader::Mapper::Relational::CONF_PORT	=> 'port'
					},
					\%ABSTYPE2SQL,
					\%ABSTYPE2SQLKEY,
					\&_CV_SQL_UPDATE__mysql,
					\@FALSE_TRUE,
				],
	'postgresql'	=>	[
					'Pg',
					{
						BP::Loader::Mapper::Relational::CONF_DB	=> 'dbname',
						BP::Loader::Mapper::Relational::CONF_HOST	=> 'host',
						BP::Loader::Mapper::Relational::CONF_PORT	=> 'port'
					},
					\%ABSTYPE2SQL,
					\%ABSTYPE2SQLKEY,
					\&_CV_SQL_UPDATE__postgresql,
					\@FALSE_TRUE,
				],
	'sqlite3'	=>	[
					'SQLite',
					{
						BP::Loader::Mapper::Relational::CONF_DB	=> 'dbname',
					},
					\%ABSTYPE2SQL,
					\%ABSTYPE2SQLKEY,
					\&_CV_SQL_UPDATE__sqlite3,
					\@FALSE_TRUE_FAKE,
				],
);

use constant {
	KEY_COLUMN_TEMPLATE	=>	'_keyColumnTemplate',
	INDEX_COLUMN_TEMPLATE	=>	'_indexColumnTemplate',
};

# Constructor parameters:
#	model: a BP::Model instance
#	config: a Config::IniFiles instance
sub new($$) {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	
	
	my $model = shift;
	my $config = shift;
	
	my $self  = $class->SUPER::new($model,$config);
	bless($self,$class);
	
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
	Carp::croak("ERROR: Unknown SQL dialect '$self->{BP::Loader::Mapper::Relational::CONF_DIALECT}'. Valid ones are: ".join(', ',keys(%SQLDIALECTS)))  unless(exists($SQLDIALECTS{$self->{BP::Loader::Mapper::Relational::CONF_DIALECT}}));
	$self->{__dialect} = $SQLDIALECTS{$self->{BP::Loader::Mapper::Relational::CONF_DIALECT}};
	
	#########
	# These definitions are needed to generate the proper SQL sentences for hashes and arrays
	#########
	my $kdoc = XML::LibXML::Document->new('1.0','UTF-8');
	my $root = $kdoc->createElementNS(BP::Model::dccNamespace,'column-set');
	$kdoc->setDocumentElement($root);
	
	my $keyColumnElem = $kdoc->createElementNS(BP::Model::dccNamespace,'column');
	$keyColumnElem->setAttribute('name','key');
	$root->appendChild($keyColumnElem);

	my $keyCT = $kdoc->createElementNS(BP::Model::dccNamespace,'column-type');
	$keyCT->setAttribute('column-kind','idref');
	$keyCT->setAttribute('item-type','string');
	$keyColumnElem->appendChild($keyCT);
	
	my $indexColumnElem = $kdoc->createElementNS(BP::Model::dccNamespace,'column');
	$indexColumnElem->setAttribute('name','idx');
	$root->appendChild($indexColumnElem);
	
	my $indexCT = $kdoc->createElementNS(BP::Model::dccNamespace,'column-type');
	$indexCT->setAttribute('column-kind','idref');
	$indexCT->setAttribute('item-type','integer');
	$indexColumnElem->appendChild($indexCT);
	
	# This one is for hashes
	my $keyColumn = BP::Model::Column->new('key',undef,undef,BP::Model::ColumnType->parseColumnType($keyColumnElem,$model,'dummy-key'));
	# This one is for arrays (at this moment, only unidimensional ones)
	my $indexColumn = BP::Model::Column->new('idx',undef,undef,BP::Model::ColumnType->parseColumnType($indexColumnElem,$model,'dummy-i'));
	
	$self->{BP::Loader::Mapper::Relational::KEY_COLUMN_TEMPLATE} = $keyColumn;
	$self->{BP::Loader::Mapper::Relational::INDEX_COLUMN_TEMPLATE} = $indexColumn;
	
	return $self;
}

sub nestedCorrelatedConcepts {
	return undef;
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

sub _CV_SQL_UPDATE__mysql($$$) {
	my($tableName,$columnName,$cvname)=@_;
	
	return <<TSQL;
UPDATE $tableName , ${cvname}_CVkeys , ${cvname}_CV
SET
	${tableName}.${columnName}_term = ${cvname}_CV.descr
WHERE
	${tableName}.${columnName}_term IS NULL AND
	${tableName}.${columnName} = ${cvname}_CVkeys.cvkey
	AND ${cvname}_CVkeys.idkey = ${cvname}_CV.idkey
;
TSQL
}

sub _CV_SQL_UPDATE__postgresql($$$) {
	my($tableName,$columnName,$cvname)=@_;
	
	return <<TSQL;
UPDATE $tableName
SET
	${columnName}_term = ${cvname}_CV.descr
FROM ${cvname}_CVkeys , ${cvname}_CV
WHERE
	${tableName}.${columnName}_term IS NULL AND
	${tableName}.${columnName} = ${cvname}_CVkeys.cvkey
	AND ${cvname}_CVkeys.idkey = ${cvname}_CV.idkey
;
TSQL
}

sub _CV_SQL_UPDATE__sqlite3($$$) {
	my($tableName,$columnName,$cvname)=@_;
	
	return <<TSQL;
UPDATE $tableName
SET
	${columnName}_term = (
		SELECT ${cvname}_CV.descr
		FROM ${cvname}_CVkeys , ${cvname}_CV
		WHERE ${tableName}.${columnName} = ${cvname}_CVkeys.cvkey
		AND ${cvname}_CVkeys.idkey = ${cvname}_CV.idkey
		LIMIT 1
	)
WHERE
	${columnName}_term IS NULL
;
TSQL
}

# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
# It returns a reference to an array of pairs
#	[absolute paths to the generated files (based on workingDir),is essential]
sub generateNativeModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $workingDir = shift;
	my $filePrefix = $self->{BP::Loader::Mapper::FILE_PREFIX_KEY};
	
	my $keyColumn = $self->{BP::Loader::Mapper::Relational::KEY_COLUMN_TEMPLATE};
	my $indexColumn = $self->{BP::Loader::Mapper::Relational::INDEX_COLUMN_TEMPLATE};
	
	my $fullFilePrefix = File::Spec->catfile($workingDir,$filePrefix);
	
#	model: a BP::Model instance, with the parsed model.
	my $model = $self->{model};
#	the path to the SQL output file
	my $outfileSQL = $fullFilePrefix.'-'.$self->{BP::Loader::Mapper::Relational::CONF_DIALECT}.'.sql';
#	the path to the SQL script which joins 
	my $outfileTranslateSQL = $fullFilePrefix.'_CVtrans'.'-'.$self->{BP::Loader::Mapper::Relational::CONF_DIALECT}.'.sql';
	
	my $dialectFuncs = $self->{__dialect};
	my $p_TYPE2SQL = $dialectFuncs->[_SQLDIALECT_TYPE2SQL];
	my $p_TYPE2SQLKEY = $dialectFuncs->[_SQLDIALECT_TYPE2SQLKEY];
	my($FALSE_VAL,$TRUE_VAL) = @{$dialectFuncs->[_SQLDIALECT_FALSE_TRUE]};
	
	# Needed later for CV dumping et al
	my @cvorder = ();
	my %cvdump = ();
	my $chunklines = $self->{'batch-size'};
	my $descType = $p_TYPE2SQL->{BP::Model::ColumnType::STRING_TYPE}[0];
	my $aliasType = $p_TYPE2SQL->{BP::Model::ColumnType::BOOLEAN_TYPE}[0];
	
	if(open(my $SQL,'>:utf8',$outfileSQL)) {
		print $SQL '-- File '.File::Basename::basename($outfileSQL)." (".$self->{BP::Loader::Mapper::Relational::CONF_DIALECT}." dialect)\n";
		print $SQL '-- Generated from '.$model->projectName.' '.$model->versionString."\n";
		print $SQL '-- '.localtime()."\n";
		
		my $p_TYPES = $model->types;
		
		# Needed later for foreign keys
		my @fks = ();
		
		my $__printTable = undef;
		
		$__printTable = sub($$$;\@$) {
			my($basename,$fullname,$columnSet,$p_colorder,$concept)=@_;
			
			$p_colorder = $columnSet->columnNames  unless(defined($p_colorder));
			
			my %fkselemrefs = ();
			my @fkselem = ($basename,$fullname,\%fkselemrefs);
			my $fksinit = undef;

			print $SQL "\n-- ",$fullname;
			print $SQL "\nCREATE TABLE $basename (";
			
			
			my $gottable=undef;
			
			my @subcolumns = ();
			
			my @columnsToPrint = @{$columnSet->columns}{@{$p_colorder}};
			
			my $idx = 0;
			foreach my $column (@columnsToPrint) {
				$idx++;
				if($column->columnType->containerType==BP::Model::ColumnType::SCALAR_CONTAINER && !(blessed($column->columnType->restriction) && $column->columnType->restriction->isa('BP::Model::CompoundType'))) {
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
					my $SQLtype = ($columnType->use == BP::Model::ColumnType::IDREF || defined($column->refColumn))?$p_TYPE2SQLKEY->{$columnType->type}[0]:$p_TYPE2SQL->{$columnType->type}[0];
					# Registering CVs
					if(blessed($columnType->restriction) && $columnType->restriction->isa('BP::Model::CV::Abstract')) {
						# At the end is a key outside here, so assuring it is using the right size
						# due restrictions on some SQL (cough, cough, MySQL, cough, cough) implementations
						$SQLtype = $p_TYPE2SQLKEY->{$columnType->type}[0];
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
				} elsif($column->columnType->containerType==BP::Model::ColumnType::SCALAR_CONTAINER) {
					# It only happens to compound types
					my $rColumnSet = $column->columnType->restriction->columnSet;
					
					splice(@columnsToPrint,$idx,0,map { $_->clone(undef,$column->name.'_') } @{$rColumnSet->columns}{@{$rColumnSet->columnNames}});
				} else {
					
					# These are defined in a separate table
					push(@subcolumns,$column);
				}
			}
			
			push(@fks,\@fkselem)  if(defined($fksinit));
			
			# Declaring a primary key (if any!)
			my @idColumnNames = @{$columnSet->idColumnNames};
			
			if(scalar(@idColumnNames)>0) {
				print $SQL ",\n\tPRIMARY KEY (".join(',',@idColumnNames).')';
			}
			
			print $SQL "\n);\n\n";
			
			# And now, let's process complicated columns
			foreach my $column (@subcolumns) {
				my $refColumnSet = $columnSet->idColumns($concept);
				my $numIdColumns = scalar(@{$refColumnSet->idColumnNames});
				
				my $newtable = $basename.'_'.$column->name;
				# Inject always a column "index" or "key"
				if($column->columnType->containerType==BP::Model::ColumnType::ARRAY_CONTAINER || $column->columnType->containerType==BP::Model::ColumnType::HASH_CONTAINER) {
					my $ikey = $column->columnType->containerType==BP::Model::ColumnType::ARRAY_CONTAINER?$indexColumn:$keyColumn;
					$refColumnSet->addColumn($ikey->clone(undef,$newtable.'_'),1);
				}
				
				if(blessed($column->columnType->restriction) && $column->columnType->restriction->isa('BP::Model::CompoundType')) {
					$refColumnSet->addColumns($column->columnType->restriction->columnSet,1);
				} else {
					my $newCol = $column->clone(undef,undef,1);
					$refColumnSet->addColumn($newCol,1);
				}
				
				# It is not really needed but .... HACK
				@{$refColumnSet->idColumnNames} = ()  if(scalar(@{$refColumnSet->idColumnNames})==$numIdColumns);
				
				$__printTable->($newtable,$fullname.' ('.$newtable.')',$refColumnSet);
			}
		};

		# Let's iterate over all the concept domains and their concepts
		foreach my $conceptDomain (@{$model->conceptDomains}) {
			# Skipping abstract concept domains
			next  if($self->{release} && $conceptDomain->isAbstract);
			
			my $conceptDomainName = $conceptDomain->name;
			
			foreach my $concept (@{$conceptDomain->concepts}) {
				#my $conceptName = $concept->name;
				my $basename = __entryName($concept,$conceptDomainName);
				
				my @colorder = BP::Loader::Mapper::_fancyColumnOrdering($concept);
				my $columnSet = $concept->columnSet;
				$__printTable->($basename,$concept->fullname,$columnSet,\@colorder,$concept);
			}
		}
		
		# Now, the CVs and the columns using them
		if(open(my $TSQL,'>:utf8',$outfileTranslateSQL)) {
			print $TSQL '-- File '.File::Basename::basename($outfileTranslateSQL)." (".$self->{BP::Loader::Mapper::Relational::CONF_DIALECT}." dialect)\n";
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

CREATE TABLE ${cvname}_CVkeys_u (
	cvkey $SQLtype NOT NULL,
	PRIMARY KEY (cvkey)
);

CREATE TABLE ${cvname}_CVkeys (
	cvkey $SQLtype NOT NULL,
	idkey $SQLtype NOT NULL,
	FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys_u(cvkey)
);

CREATE TABLE ${cvname}_CVparents (
	idkey $SQLtype NOT NULL,
	cvkey $SQLtype NOT NULL,
	FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys_u(cvkey)
);

CREATE TABLE ${cvname}_CVancestors (
	idkey $SQLtype NOT NULL,
	cvkey $SQLtype NOT NULL,
	FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys_u(cvkey)
);

CVEOF

				# Second, the data
				my $first = 0;
				my %cvseen = ();
				foreach my $enclosedCV (@{$CV->getEnclosedCVs}) {
					foreach my $key  (@{$enclosedCV->order},@{$enclosedCV->aliasOrder}) {
						next  if(exists($cvseen{$key}));
						$cvseen{$key}=undef;
						
						my $term = $enclosedCV->getTerm($key);
						if($first==0) {
							print $SQL <<CVEOF;
INSERT INTO ${cvname}_CV VALUES
CVEOF
						}
						print $SQL (($first>0)?",\n":''),'(',join(',',($doEscape)?__sql_escape($term->key):$term->key,__sql_escape($term->name),($term->isAlias)?$TRUE_VAL:$FALSE_VAL),')';
						
						$first++;
						if($first>=$chunklines) {
							print $SQL "\n;\n\n";
							$first=0;
						}
					}
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				$first = 0;
				%cvseen = ();
				foreach my $enclosedCV (@{$CV->getEnclosedCVs}) {
					foreach my $key  (@{$enclosedCV->order},@{$enclosedCV->aliasOrder}) {
						my $term = $enclosedCV->getTerm($key);
						my $ekey = ($doEscape)?__sql_escape($term->key):$term->key;
						
						foreach my $akey (@{$term->keys}) {
							next  if(exists($cvseen{$akey}));
							$cvseen{$akey}=undef;
							
							if($first==0) {
								print $SQL <<CVEOF;
INSERT INTO ${cvname}_CVkeys_u VALUES
CVEOF
							}
							print $SQL (($first>0)?",\n":''),'(',(($doEscape)?__sql_escape($akey):$akey),')';
							
							$first++;
							if($first>=$chunklines) {
								print $SQL "\n;\n\n";
								$first=0;
							}
						}
					}
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				$first = 0;
				%cvseen = ();
				foreach my $enclosedCV (@{$CV->getEnclosedCVs}) {
					foreach my $key  (@{$enclosedCV->order},@{$enclosedCV->aliasOrder}) {
						next  if(exists($cvseen{$key}));
						$cvseen{$key}=undef;
						
						my $term = $enclosedCV->getTerm($key);
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
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				$first = 0;
				%cvseen = ();
				foreach my $enclosedCV (@{$CV->getEnclosedCVs}) {
					foreach my $key  (@{$enclosedCV->order},@{$enclosedCV->aliasOrder}) {
						next  if(exists($cvseen{$key}));
						$cvseen{$key}=undef;
						
						my $term = $enclosedCV->getTerm($key);
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
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				$first = 0;
				%cvseen = ();
				foreach my $enclosedCV (@{$CV->getEnclosedCVs}) {
					foreach my $key  (@{$enclosedCV->order},@{$enclosedCV->aliasOrder}) {
						next  if(exists($cvseen{$key}));
						$cvseen{$key}=undef;
						
						my $term = $enclosedCV->getTerm($key);
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
REFERENCES ${cvname}_CVkeys_u(cvkey);

CVEOF
					print $TSQL <<TCVEOF;
ALTER TABLE $tableName ADD COLUMN ${columnName}_term $descType;

TCVEOF
					print $TSQL $dialectFuncs->[_SQLDIALECT_UPDATE_CV]->($tableName,$columnName,$cvname);
				}
			}
			close($TSQL);
		} else {
			Carp::croak("Unable to create output file $outfileTranslateSQL");
		}
		
		# Now, the FK restrictions from identification relations
		foreach my $p_fks (@fks) {
			my($basename,$fullname,$p_fkconcept) = @{$p_fks};
			
			print $SQL "\n-- ",$fullname, " foreign keys from inheritance";
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
			next  if($self->{release} && $conceptDomain->isAbstract);
			
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
						next  if($self->{release} && $relatedConcept->concept->conceptDomain->isAbstract);
						
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
	
	return [[$outfileSQL,1], [$outfileTranslateSQL,undef]];
}

# This method returns the dsn string, according to the dialect
sub _dsn() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my @dsnValues=();
	
	foreach my $key (CONF_DB,CONF_HOST,CONF_PORT) {
		if(exists($self->{$key}) && defined($self->{$key}) && $self->{$key} ne '' && exists($self->{__dialect}[_SQLDIALECT_DSNPARAMS]{$key})) {
			push(@dsnValues,$self->{__dialect}[_SQLDIALECT_DSNPARAMS]{$key} .'='.$self->{$key});
		}
	}
	
	return join(':','dbi',$self->{__dialect}[_SQLDIALECT_DRIVER],join(';',@dsnValues));
}

# This method returns a connection to the database
# In this case, a DBI database handler
sub _connect() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $dsn = $self->_dsn;
	
	my $user = exists($self->{BP::Loader::Mapper::Relational::CONF_DBUSER})?$self->{BP::Loader::Mapper::Relational::CONF_DBUSER}:'';
	my $pass = exists($self->{BP::Loader::Mapper::Relational::CONF_DBPASS})?$self->{BP::Loader::Mapper::Relational::CONF_DBPASS}:'';
	
	my $dbh = DBI->connect($dsn,$user,$pass,{RaiseError=>0,AutoCommit=>1});
	
	return $dbh;
}

sub storeNativeModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	if(defined($self->{model}->metadataCollection)) {
		my $workingDir = File::Temp::tempdir();
		
		# First, the native model in file
		my $p_nativeModelFiles = $self->generateNativeModel($workingDir);
		
		# Second, reading the file by ;
		my $dbh = $self->connect();
		$dbh->begin_work();
		eval {
			foreach my $p_nativeModelFile (@{$p_nativeModelFiles}) {
				next  unless($p_nativeModelFile->[1]);
				local $/ = ";\n";
				if(open(my $SQL,'<:utf8',$p_nativeModelFile->[0])) {
					while(my $sentence = <$SQL>) {
						chomp($sentence);
						$dbh->do($sentence) || Carp::croak("Failed sentence: $sentence");
					}
					
					close($SQL);
				}
			}
		};
		
		my $croakmsg = undef;
		if($@) {
			$dbh->rollback();
			$croakmsg = "ERROR: unable to load metadata model. Reason: $@\n";
		} else {
			$dbh->commit();
		}
		
		File::Path::remove_tree($workingDir);
		Carp::croak($croakmsg)  if(defined($croakmsg));
	}
}

# _genDestination parameters:
#	corrConcept: An instance of BP::Loader::CorrelatableConcept
# It returns the destination to be used in bulkInsert calls,
# i.e. a DBI prepared statement
sub _genDestination($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	
	my $dbh = $self->connect();
	my $keyColumn = $self->{BP::Loader::Mapper::Relational::KEY_COLUMN_TEMPLATE};
	my $indexColumn = $self->{BP::Loader::Mapper::Relational::INDEX_COLUMN_TEMPLATE};
	my $dialectFuncs = $self->{__dialect};
	my $p_TYPE2SQL = $dialectFuncs->[_SQLDIALECT_TYPE2SQL];
	my $p_TYPE2SQLKEY = $dialectFuncs->[_SQLDIALECT_TYPE2SQLKEY];
	
	# The order must be preserved, and so we are using a Tie::IxHash for $destination
	my %destHash = ();
	tie(%destHash,'Tie::IxHash');
	
	my $__genDest = undef;
	$__genDest = sub($$;\@$) {
		my($basename,$columnSet,$p_colorder,$concept) = @_;
		
		$p_colorder = $columnSet->columnNames  unless(defined($p_colorder));
		
		my @subcolumns = ();
		
		my @columnsToInsert = @{$columnSet->columns}{@{$p_colorder}};
		my $idx = 0;
		foreach my $column (@columnsToInsert) {
			$idx++;
			unless($column->columnType->containerType==BP::Model::ColumnType::SCALAR_CONTAINER && !(blessed($column->columnType->restriction) && $column->columnType->restriction->isa('BP::Model::CompoundType'))) {
				if($column->columnType->containerType==BP::Model::ColumnType::SCALAR_CONTAINER) {
					# It only happens to compound types
					my $rColumnSet = $column->columnType->restriction->columnSet;
					
					splice(@columnsToInsert,$idx,0,map { $_->clone(undef,$column->name.'_') } @{$rColumnSet->columns}{@{$rColumnSet->columnNames}});
				} else {
					
					# These are defined in a separate table
					push(@subcolumns,$column);
				}
			}
		}
		
		my @colnames = map { $_->name } @columnsToInsert;
		my @coltypes = map { (($_->columnType->use == BP::Model::ColumnType::IDREF || defined($_->refColumn))?$p_TYPE2SQLKEY:$p_TYPE2SQL)->{$_->columnType->type}[1] } @columnsToInsert;
		my @colmanglers = map { (($_->columnType->use == BP::Model::ColumnType::IDREF || defined($_->refColumn))?$p_TYPE2SQLKEY:$p_TYPE2SQL)->{$_->columnType->type}[2] } @columnsToInsert;
				
		my $insertSentence = 'INSERT INTO '.$basename.'('.join(',',@colnames).') VALUES ('.join(',', map { '?' } @columnsToInsert).')';
		
		$destHash{$basename} = [$dbh->prepare($insertSentence),\@colnames,\@coltypes,\@colmanglers];
		
		# And now, let's process complicated columns
		foreach my $column (@subcolumns) {
			my $refColumnSet = $columnSet->idColumns($concept);
			my $numIdColumns = scalar(@{$refColumnSet->idColumnNames});
			
			my $newtable = $basename.'_'.$column->name;
			# Inject always a column "index" or "key"
			if($column->columnType->containerType==BP::Model::ColumnType::ARRAY_CONTAINER || $column->columnType->containerType==BP::Model::ColumnType::HASH_CONTAINER) {
				my $ikey = $column->columnType->containerType==BP::Model::ColumnType::ARRAY_CONTAINER?$indexColumn:$keyColumn;
				$refColumnSet->addColumn($ikey->clone(undef,$newtable.'_'),1);
			}
			
			if(blessed($column->columnType->restriction) && $column->columnType->restriction->isa('BP::Model::CompoundType')) {
				$refColumnSet->addColumns($column->columnType->restriction->columnSet,1);
			} else {
				my $newCol = $column->clone(undef,undef,1);
				$refColumnSet->addColumn($newCol,1);
			}
			
			$__genDest->($newtable,$refColumnSet);
		}
	};
	
	my $concept = $correlatedConcept->concept;
	my $desttable = __entryName($concept);
	
	my @colorder = BP::Loader::Mapper::_fancyColumnOrdering($concept);
	my $columnSet = $concept->columnSet;
	
	$__genDest->($desttable,$columnSet,\@colorder,$concept);
	
	# Starting a transaction so it is all or nothing
	$dbh->begin_work();
	
	return \%destHash;
}

# _freeDestination parameters:
#	destination: the destination to be freed
#	errflag: The error flag
# It frees a destination, in this case a prepared statement
# It can also finish a transaction, based on the error flag
sub _freeDestination(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));

	my $destination = shift;
	my $errflag = shift;
	
	my $dbh = $self->connect();
	# Finishing the transaction
	if($errflag) {
		$dbh->rollback();
	} else {
		$dbh->commit();
	}
	
	# Freeing the sentence(s)
	foreach my $destInfo (values(%{$destination})) {
		$destInfo->[0]->finish();
		$destInfo->[0] = undef;
	}
}

# _bulkPrepare parameters:
#	correlatedConcept: A BP::Loader::CorrelatableConcept instance
#	entorp: The output of BP::Loader::CorrelatableConcept->readEntry
# It returns the bulkData to be used for the load
sub _bulkPrepare($$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $correlatedConcept = shift;
	my $entorp = shift;
	
	my $concept = $correlatedConcept->concept();
	my $dialectFuncs = $self->{__dialect};
	my $p_TYPE2SQL = $dialectFuncs->[_SQLDIALECT_TYPE2SQL];
	my $p_TYPE2SQLKEY = $dialectFuncs->[_SQLDIALECT_TYPE2SQLKEY];
	
	my @coldata = ();
	my @pushorder = ();
	my @colorder = BP::Loader::Mapper::_fancyColumnOrdering($concept);
	my $columnSet = $concept->columnSet;
	foreach my $colname (@colorder) {
		my $column = $columnSet->columns->{$colname};
		my $columnType = $column->columnType;
		my $mangler = undef;
		if($columnType->use == BP::Model::ColumnType::IDREF || defined($column->refColumn)) {
			$mangler = $p_TYPE2SQLKEY->{$columnType->type}[2];
		} else {
			$mangler = $p_TYPE2SQL->{$columnType->type}[2];
		}
		
		# FUTURE: improve these two cases, based on non-standard database features
		# The default case when we don't know to do, serialize in json
		$mangler = \&JSON::encode_json  unless(defined($mangler));
		
		# And the array case is the same
		$mangler = \&JSON::encode_json  if($columnType->arrayDimensions() > 0);
		
		my @preparedData = ();
		# Setting up optimized pusher, based on what we know about the mangler
		my $pusher = ($mangler == \&__bypass) ?
		sub {
			push(@preparedData,$_[0]);
		}
		:
		sub {
			push(@preparedData,$mangler->($_[0]));
		}
		;
		push(@coldata,\@preparedData);
		push(@pushorder,$pusher);
	}
	
	# Now, let's fill the data arrays for each entry and column
	foreach my $entry (@{$entorp->[0]}) {
		my $colnum = 0;
		foreach my $colname (@colorder) {
			$pushorder[$colnum]->($entry->{$colname});
			$colnum++;
		}
	}
	
	return \@coldata;
}

# _bulkInsert parameters:
#	destination: The destination of the bulk insertion, which is a DBI prepared statement.
#	bulkData: a reference to a hash of arrays which contain the values to store.
sub _bulkInsert($\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $destination = shift;
	my $bulkData = shift;
	
	Carp::croak("ERROR: _bulkInsert needs the destination as a hash of prepared statements")  unless(ref($destination) eq 'HASH');
	Carp::croak("ERROR: _bulkInsert needs the data as a hash")  unless(ref($bulkData) eq 'HASH');
	
	# The order is preserved because we are using a Tie::IxHash for $destination
	foreach my $bulkKey (keys(%{$destination})) {
		if(exists($bulkData->{$bulkKey})) {
			my $bulkArray = $bulkData->{$bulkKey};
			my $destInfo = $destination->{$bulkKey};
			my $destSentence = (ref($destInfo) eq 'ARRAY' && exists($destInfo->[0]))?$destInfo->[0]:undef;
			
			Carp::croak("ERROR: _bulkInsert needs a prepared statement")  unless(blessed($destSentence) && $destSentence->can('execute'));
			Carp::croak("ERROR: _bulkInsert needs an array of arrays for the prepared statement")  unless(ref($bulkArray) eq 'ARRAY');
			
			my $colnum = 1;
			foreach my $p_column (@{$bulkArray}) {
				my $mangler = $destInfo->[3][$colnum-1];
				
				# Preparing the data for SQL
				foreach my $coldata (@{$p_column}) {
					$coldata = $mangler->($coldata);
				}
				
				$destSentence->bind_param_array($colnum,$p_column,$destInfo->[2][$colnum-1]);
				$colnum++;
			}
			
			$destSentence->execute_array();
		}
	}
}

1;
