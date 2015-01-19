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
	_SQLDIALECT_DIRECTIVES	=>	6,
	_SQLDIALECT_DBI_DIRECTIVES	=>	7,
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
					[
						'SET storage_engine=InnoDB'
					],
					{
						mysql_enable_utf8	=>	1
					},
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
					[
					],
					{
					},
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
					[
						'PRAGMA journal_mode=WAL',
						'PRAGMA foreign_keys=ON'
					],
					{
						sqlite_unicode	=>	1
					},
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
	my $root = $kdoc->createElementNS(BP::Model::Common::dccNamespace,'column-set');
	$kdoc->setDocumentElement($root);
	
	my $keyColumnElem = $kdoc->createElementNS(BP::Model::Common::dccNamespace,'column');
	$keyColumnElem->setAttribute('name','key');
	$root->appendChild($keyColumnElem);

	my $keyCT = $kdoc->createElementNS(BP::Model::Common::dccNamespace,'column-type');
	$keyCT->setAttribute('column-kind','idref');
	$keyCT->setAttribute('item-type','string');
	$keyColumnElem->appendChild($keyCT);
	
	my $indexColumnElem = $kdoc->createElementNS(BP::Model::Common::dccNamespace,'column');
	$indexColumnElem->setAttribute('name','idx');
	$root->appendChild($indexColumnElem);
	
	my $indexCT = $kdoc->createElementNS(BP::Model::Common::dccNamespace,'column-type');
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

# _SQL_CREATE_INDEXES parameters:
#	table: a table name
#	indexes: an array of BP::Model::Index instances
#	columns: The array of columns
# It returns an array of SQL sentences
sub _SQL_CREATE_INDEXES($\@\@) {
	my($table,$p_indexes,$p_columns) = @_;
	
	my @retval = ();
	
	my %colcheck = map { $_->name => undef } @{$p_columns};
	
	my $icounter = 0;
	foreach my $index (@{$p_indexes}) {
		my $isUnique = $index->isUnique;
		
		my $sentence = undef;
		
		foreach my $p_attr (@{$index->indexAttributes}) {
			my($attrName,$isAscending) = @{$p_attr};
			
			if(exists($colcheck{$attrName})) {
				unless(defined($sentence)) {
					$sentence = 'CREATE';
					$sentence .= ' UNIQUE'  if($isUnique);
					$sentence .= ' INDEX '.$table.'_'.$icounter.' ON '.$table.'(';
					$icounter++;
				} else {
					$sentence .= ',';
				}
				
				$sentence .= $attrName.' '.(($isAscending==1)?'ASC':'DESC');
			}
		}
		
		if(defined($sentence)) {
			$sentence .= ')';
			push(@retval,$sentence);
		}
	}
	
	return @retval;
}

# generateNativeModel parameters:
#	workingDir: The directory where the model files are going to be saved.
# It returns a reference to an array of pairs
#	[absolute paths to the generated files (based on workingDir),is essential]
sub generateNativeModel($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
		
		# The directives
		foreach my $directive (@{$dialectFuncs->[_SQLDIALECT_DIRECTIVES]}) {
			print $SQL "\n",$directive,";\n";
		}
		
		
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
			my @indexesToPrint = @{$columnSet->indexes};
			
			my $idx = 0;
			foreach my $column (@columnsToPrint) {
				$idx++;
				if($column->columnType->containerType==BP::Model::ColumnType::SCALAR_CONTAINER) {
					if(blessed($column->columnType->restriction) && $column->columnType->restriction->isa('BP::Model::CompoundType')) {
						# It only happens to compound types
						my $rColumnSet = $column->columnType->restriction->columnSet;
						
						my %columnCorrespondence = ();
						my @cColumns = ();
						
						foreach my $rColumn (@{$rColumnSet->columns}{@{$rColumnSet->columnNames}}) {
							my $cColumn = $rColumn->clone(undef,$column->name.'_');
							$columnCorrespondence{$rColumn->name} = $cColumn->name;
							
							# Resetting the use when flattening complex types
							$cColumn->columnType->setUse($column->columnType->use)  if($column->columnType->use < $cColumn->columnType->use);
							
							push(@cColumns,$cColumn);
						}
						
						splice(@columnsToPrint,$idx,0,@cColumns);
						
						# And the index hints!
						push(@indexesToPrint,map { $_->relatedIndex(\%columnCorrespondence) } @{$rColumnSet->indexes});
					} else {
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
					}
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
			
			# Let's print the indexes
			foreach my $indexDecl (_SQL_CREATE_INDEXES($basename,@indexesToPrint,@columnsToPrint)) {
				print $SQL $indexDecl,";\n\n";
			}
			
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
			
			# The directives
			foreach my $directive (@{$dialectFuncs->[_SQLDIALECT_DIRECTIVES]}) {
				print $TSQL "\n",$directive,";\n";
			}
			
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
	CONSTRAINT c_${cvname}_CVkeys_idkey FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	CONSTRAINT c_${cvname}_CVkeys_cvkey FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys_u(cvkey)
);

CREATE TABLE ${cvname}_CVparents (
	idkey $SQLtype NOT NULL,
	cvkey $SQLtype NOT NULL,
	CONSTRAINT c_${cvname}_CVparents_idkey FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	CONSTRAINT c_${cvname}_CVparents_cvkey FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys_u(cvkey)
);

CREATE TABLE ${cvname}_CVancestors (
	idkey $SQLtype NOT NULL,
	cvkey $SQLtype NOT NULL,
	CONSTRAINT c_${cvname}_CVancestors_idkey FOREIGN KEY (idkey) REFERENCES ${cvname}_CV(idkey),
	CONSTRAINT c_${cvname}_CVancestors_cvkey FOREIGN KEY (cvkey) REFERENCES ${cvname}_CVkeys_u(cvkey)
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
				my %cvseen_u = ();
				foreach my $enclosedCV (@{$CV->getEnclosedCVs}) {
					foreach my $key  (@{$enclosedCV->order},@{$enclosedCV->aliasOrder}) {
						my $term = $enclosedCV->getTerm($key);
						my $ekey = ($doEscape)?__sql_escape($term->key):$term->key;
						
						foreach my $akey (@{$term->keys},@{$term->uriKeys}) {
							next  if(exists($cvseen_u{$akey}));
							$cvseen_u{$akey}=undef;
							
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
						
						foreach my $akey (@{$term->keys},@{$term->uriKeys}) {
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
							if(exists($cvseen_u{$pkey})) {
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
							if(exists($cvseen_u{$pkey})) {
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
				}
				print $SQL "\n;\n\n"  if($first>0);
				
				# Third, the references
				# And the translation script
				foreach my $p_columnRef (@{$p_columnRefs}) {
					my($columnName,$tableName)=@{$p_columnRef};
#CREATE INDEX ${tableName}_${columnName}_CVindex ON $tableName ($columnName);
#
					unless($CV->isLax()) {
						print $SQL <<CVEOF;
ALTER TABLE $tableName ADD FOREIGN KEY ($columnName)
REFERENCES ${cvname}_CVkeys_u(cvkey);

CVEOF
					}
					print $TSQL <<TCVEOF;
ALTER TABLE $tableName ADD COLUMN ${columnName}_term $descType;

TCVEOF
					print $TSQL $dialectFuncs->[_SQLDIALECT_UPDATE_CV]->($tableName,$columnName,$cvname);
				}
			}
			close($TSQL);
			
			$__printTable = undef;
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
						my $localbasename = $basename;
						
						# This only works with single columns
						$localbasename .= '_'.$refColumns[0]->name  if($relatedConcept->arity eq 'M');
						

						#print $SQL "\nCREATE INDEX ${basename}_FK${cycle}_${refBasename} ON $basename (",join(',',map { $_->name } @refColumns),");\n";
						print $SQL "\nALTER TABLE $localbasename ADD FOREIGN KEY (",join(',',map { $_->name } @refColumns),")";
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
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
# and runs the corresponding directives
sub _connect() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $dsn = $self->_dsn;
	
	my $user = exists($self->{BP::Loader::Mapper::Relational::CONF_DBUSER})?$self->{BP::Loader::Mapper::Relational::CONF_DBUSER}:'';
	my $pass = exists($self->{BP::Loader::Mapper::Relational::CONF_DBPASS})?$self->{BP::Loader::Mapper::Relational::CONF_DBPASS}:'';
	
	# Injecting dialect-specific directives
	my $p_dbiDirectives = {
		RaiseError=>0,
		AutoCommit=>1
	};
	@{$p_dbiDirectives}{keys(%{$self->{__dialect}[_SQLDIALECT_DBI_DIRECTIVES]})} = values(%{$self->{__dialect}[_SQLDIALECT_DBI_DIRECTIVES]});
	
	my $dbh = DBI->connect($dsn,$user,$pass,$p_dbiDirectives);
	
	# Execute the directives
	if($dbh) {
		foreach my $directive (@{$self->{__dialect}[_SQLDIALECT_DIRECTIVES]}) {
			$dbh->do($directive);
		}
	}
	
	return $dbh;
}

# It stores the native model, but only on empty databases
sub storeNativeModel() {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $dbh = $self->connect();
	
	my $schema = undef;
	my $name = undef;
	if(exists($self->{BP::Loader::Mapper::Relational::CONF_DB}) && defined($self->{BP::Loader::Mapper::Relational::CONF_DB})) {
		my $sth = $dbh->table_info( '', $self->{BP::Loader::Mapper::Relational::CONF_DB}, '', 'TABLE' );
		( undef, $schema, $name ) = $sth->fetchrow_array();
		$sth->finish();
	}
	unless(defined($name)) {
		my $sth = $dbh->table_info( '', 'public', '', 'TABLE' );
		( undef, $schema, $name ) = $sth->fetchrow_array();
		$sth->finish();
	}
	
	# Create the database only when the database is empty (i.e. with no table)
	unless(defined($name)) {
		my $workingDir = File::Temp::tempdir();
		
		# First, the native model in file
		my $p_nativeModelFiles = $self->generateNativeModel($workingDir);
		
		# Second, reading the file by ;
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

use constant {
	MAPPING_TABLE	=>	0,
	MAPPING_TYPE	=>	1,
	MAPPING_VALUE_MAPPING	=>	2,
	MAPPING_SUBMAPPINGS	=>	3,
	MAPPING_KEY_IDX	=>	4,
	MAPPING_KEYS_FOR_SUBMAPPINGS	=>	5,
	MAPPING_FOREIGN_KEYS	=>	6,
	MAPPING_VALUE_IDX	=>	7,
	MAPPING_REQUIRED_COLUMNS	=>	8,
	MAPPING_INCREMENTAL_UPDATE_SUBMAPPING_KEYS	=>	9,
};

# _genDestination parameters:
#	corrConcept: An instance of BP::Loader::CorrelatableConcept
# It returns the destination to be used in bulkInsert calls,
# i.e. a DBI prepared statement
sub _genDestination($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
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
	$__genDest = sub($$;\@$$) {
		my($basename,$columnSet,$p_colorder,$concept,$p_incrementalColumns) = @_;
		
		$p_colorder = $columnSet->columnNames  unless(defined($p_colorder));
		
		my @subcolumns = ();
		
		my @columnsToInsert = map { [$_->name,$_] } @{$columnSet->columns}{@{$p_colorder}};
		
		# A mapping has these features
		# 0. table container key name
		# 1. Container type (scalar, set, array, hash)
		# 2. (key names -> column indexes mapping)
		# 3. key names -> submapping
		# 4. key_idx (the column which holds either an index or a hash key)
		# 5. (key names -> submapping refkey indexes)
		# 6. (local refkey names -> local refkey indexes)
		# 7. value_idx (the column which holds the value when it is an array of scalar values)
		# 8. required columns
		# 9. incremental update submapping keys
		my $p_main_mappings = [ $basename, BP::Model::ColumnType::SET_CONTAINER, {}, {}, undef, undef, undef, undef, {}, $p_incrementalColumns];
		
		my $idx = 0;
		my $colidx = 0;
		my @mappingStack = ();
		my %idcolmap = ();
		my @survivors = ();
		foreach my $columnData (@columnsToInsert) {
			my($origColumnName,$column)  = @{$columnData};
			my $p_mappings = (scalar(@mappingStack) > 0) ? pop(@mappingStack) : $p_main_mappings;
			$idx++;
			unless($column->columnType->containerType==BP::Model::ColumnType::SCALAR_CONTAINER && !(blessed($column->columnType->restriction) && $column->columnType->restriction->isa('BP::Model::CompoundType'))) {
				if($column->columnType->containerType==BP::Model::ColumnType::SCALAR_CONTAINER) {
					# It only happens to compound types
					my $rColumnSet = $column->columnType->restriction->columnSet;
					
					my @cColumns = ();
					
					foreach my $rColumn (@{$rColumnSet->columns}{@{$rColumnSet->columnNames}}) {
						my $cColumn = $rColumn->clone(undef,$column->name.'_');
						
						# Resetting the use when flattening complex types
						$cColumn->columnType->setUse($column->columnType->use)  if($column->columnType->use < $cColumn->columnType->use);
						push(@cColumns,[$rColumn->name,$cColumn]);
					}
					
					splice(@columnsToInsert,$idx,0,@cColumns);
					
					# Creating the new mapping
					my $p_newmappings = [ $basename, BP::Model::ColumnType::SCALAR_CONTAINER, {}, {}, undef, undef, undef, undef, {} ];
					$p_mappings->[MAPPING_SUBMAPPINGS]{$origColumnName} = $p_newmappings;
					
					# Put as many copies as columns this compound type has
					map { push(@mappingStack,$p_newmappings) } @{$rColumnSet->columnNames};
				} else {
					
					# These are defined in a separate table
					push(@subcolumns,[$p_mappings,$origColumnName,$column]);
				}
			} else {
				$p_mappings->[MAPPING_VALUE_MAPPING]{$origColumnName} = $colidx;
				# Registering required column
				$p_mappings->[MAPPING_REQUIRED_COLUMNS]{$origColumnName} = undef  if($column->columnType->use>=BP::Model::ColumnType::IDREF);
				push(@survivors,$columnData);
				$idcolmap{$column+0} = $colidx;
				$colidx++;
			}
		}
		
		my @colnames = map { $_->[1]->name } @survivors;
		my @coltypes = map { my $col = $_->[1]; (($col->columnType->use == BP::Model::ColumnType::IDREF || defined($col->refColumn))?$p_TYPE2SQLKEY:$p_TYPE2SQL)->{$col->columnType->type}[1] } @survivors;
		my @colmanglers = map { my $col = $_->[1]; (($col->columnType->use == BP::Model::ColumnType::IDREF || defined($col->refColumn))?$p_TYPE2SQLKEY:$p_TYPE2SQL)->{$col->columnType->type}[2] } @survivors;
				
		my $insertSentence = 'INSERT INTO '.$basename.'('.join(',',@colnames).') VALUES ('.join(',', map { '?' } @colnames).')';
		#print STDERR "DEBUGPREPARE: $insertSentence\n";
		
		$destHash{$basename} = [$dbh->prepare_cached($insertSentence),\@colnames,\@coltypes,\@colmanglers];
		
		# And now, let's process complicated columns
		foreach my $columnData (@subcolumns) {
			my($p_mappings, $origColumnName, $column) = @{$columnData};
			my $refColumnSet = $columnSet->idColumns($concept);
			
			my %keytransfer = map { $_->name => $idcolmap{$_->refColumn + 0} }  @{$refColumnSet->columns}{@{$refColumnSet->columnNames}};
			
			my $newtable = $basename.'_'.$column->name;
			# Inject always a column "index" or "key"
			my $ikeycolumnname = undef;
			if($column->columnType->containerType==BP::Model::ColumnType::ARRAY_CONTAINER || $column->columnType->containerType==BP::Model::ColumnType::HASH_CONTAINER) {
				my $ikey = $column->columnType->containerType==BP::Model::ColumnType::ARRAY_CONTAINER?$indexColumn:$keyColumn;
				my $ikeycolumn = $ikey->clone(undef,$newtable.'_');
				$ikeycolumnname = $ikeycolumn->name;
				$refColumnSet->addColumn($ikeycolumn,1);
			}
			
			my $valuecolumnname = undef;
			if(blessed($column->columnType->restriction) && $column->columnType->restriction->isa('BP::Model::CompoundType')) {
				$refColumnSet->addColumns($column->columnType->restriction->columnSet,1);
			} else {
				my $newCol = $column->clone(undef,undef,1);
				$valuecolumnname = $newCol->name;
				$refColumnSet->addColumn($newCol,1);
			}
			
			my $new_submappings = $__genDest->($newtable,$refColumnSet);
			
			# Fixing up the submapping
			$new_submappings->[MAPPING_TYPE] = $column->columnType->containerType;
			if(defined($ikeycolumnname)) {
				$new_submappings->[MAPPING_KEY_IDX] = $new_submappings->[MAPPING_VALUE_MAPPING]{$ikeycolumnname};
				delete($new_submappings->[MAPPING_VALUE_MAPPING]{$ikeycolumnname});
				# This does not have to be checked
				delete($new_submappings->[MAPPING_REQUIRED_COLUMNS]{$ikeycolumnname});
			}
			$new_submappings->[MAPPING_KEYS_FOR_SUBMAPPINGS] = \%keytransfer;
			
			my %foreignKeys = ();
			foreach my $foreignKey (keys(%keytransfer))  {
				$foreignKeys{$foreignKey} = $new_submappings->[MAPPING_VALUE_MAPPING]{$foreignKey};
				delete($new_submappings->[MAPPING_VALUE_MAPPING]{$foreignKey});
			}
			
			$new_submappings->[MAPPING_FOREIGN_KEYS] = \%foreignKeys;
			
			if(defined($valuecolumnname)) {
				$new_submappings->[MAPPING_VALUE_IDX] = $new_submappings->[MAPPING_VALUE_MAPPING]{$valuecolumnname};
				delete($new_submappings->[MAPPING_VALUE_MAPPING]{$valuecolumnname});
				# This does not have to be checked
				delete($new_submappings->[MAPPING_REQUIRED_COLUMNS]{$valuecolumnname});
			}
			
			$p_mappings->[MAPPING_SUBMAPPINGS]{$origColumnName} = $new_submappings;
			# Registering required column
			$p_mappings->[MAPPING_REQUIRED_COLUMNS]{$origColumnName} = undef  if($column->columnType->use>=BP::Model::ColumnType::IDREF);
		}
		
		return $p_main_mappings;
	};
	
	my $concept = $correlatedConcept->concept;
	my $desttable = __entryName($concept);
	
	my @colorder = BP::Loader::Mapper::_fancyColumnOrdering($concept);
	my $columnSet = $concept->columnSet;
	
	my $p_main_mappings = $__genDest->($desttable,$columnSet,\@colorder,$concept,$correlatedConcept->incrementalColumns);
	
	$__genDest = undef;
	
	# Starting a transaction so it is all or nothing
	$dbh->begin_work();
	
	return [\%destHash,$p_main_mappings];
}

# _freeDestination parameters:
#	destination: the destination to be freed
#	errflag: The error flag
# It frees a destination, in this case a prepared statement
# It can also finish a transaction, based on the error flag
sub _freeDestination(;$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));

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
	foreach my $destInfo (values(%{$destination->[0]})) {
		$destInfo->[0]->finish();
		$destInfo->[0] = undef;
	}
}

# _bulkPrepare parameters:
#	entorp: The output of BP::Loader::CorrelatableConcept->readEntry (i.e. an array of hashes)
# It returns the bulkData to be used for the load
sub _bulkPrepare($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $entorp = shift;
	
	$entorp = [ $entorp ]  unless(ref($entorp) eq 'ARRAY');
	
	my $destination = $self->getInternalDestination();
	
	my($p_dest_hash,$p_main_mappings) = @{$destination};
	
	my %coldata = ();
	
	my $__processData = undef;
	
	$__processData = sub($$;%) {
		my($p_mappings,$data,$p_parentData) = @_;
		
		my $p_parentColumns = $p_mappings->[MAPPING_FOREIGN_KEYS];
		Carp::croak("Parent data unavailable for ".$p_mappings->[MAPPING_TABLE])  if(defined($p_parentColumns) && !defined($p_parentData));
		
		# Initializing the data structure
		$coldata{$p_mappings->[MAPPING_TABLE]} = []  unless(exists($coldata{$p_mappings->[MAPPING_TABLE]}));
		my $p_data_columns = $coldata{$p_mappings->[MAPPING_TABLE]};
		
		my $key_idx = $p_mappings->[MAPPING_KEY_IDX];
		my $value_idx = $p_mappings->[MAPPING_VALUE_IDX];
		my $p_value_mapping = $p_mappings->[MAPPING_VALUE_MAPPING];
		my $p_sub_mappings = $p_mappings->[MAPPING_SUBMAPPINGS];
		my $p_required = $p_mappings->[MAPPING_REQUIRED_COLUMNS];
		
		my $__doProcess = sub {
			my($p_key,$p_entry)=@_;
			
			my @columnData = ();
			
			my $skip = !defined($value_idx) && defined($p_mappings->[MAPPING_INCREMENTAL_UPDATE_SUBMAPPING_KEYS]) && exists($p_entry->{BP::Loader::Mapper::COL_INCREMENTAL_UPDATE_ID});
			
			# First, the values
			# The (partial) key
			if(defined($key_idx)) {
				push(@{$p_data_columns->[$key_idx]}, $p_key)  unless($skip);
				$columnData[$key_idx] = $p_key;
			}
			
			# The 'foreign keys'
			if(defined($p_parentColumns)) {
				while(my($colname,$colidx)=each(%{$p_parentColumns})) {
					Carp::croak("[".$p_mappings->[MAPPING_TABLE]."] Values for $colname cannot be null!")  if(exists($p_required->{$colname}) && !(exists($p_parentData->{$colname}) && defined($p_parentData->{$colname})));
					
					push(@{$p_data_columns->[$colidx]}, $p_parentData->{$colname})  unless($skip);
					$columnData[$colidx] = $p_parentData->{$colname};
				}
			}
			
			# And the values themselves
			if(defined($value_idx)) {
				push(@{$p_data_columns->[$value_idx]}, $p_entry);
				$columnData[$value_idx] = $p_entry;
			} else {
				while(my($colname,$colidx)=each(%{$p_value_mapping})) {
					Carp::croak("[".$p_mappings->[MAPPING_TABLE]."] Values for $colname cannot be null!")  if(exists($p_required->{$colname}) && !(exists($p_entry->{$colname}) && defined($p_entry->{$colname})));
					
					push(@{$p_data_columns->[$colidx]}, $p_entry->{$colname})  unless($skip);
					$columnData[$colidx] = $p_entry->{$colname};
				}
			}
			
			# Then, the submappings (or a part of them)
			my @sub_keys = $skip ? @{$p_mappings->[MAPPING_INCREMENTAL_UPDATE_SUBMAPPING_KEYS]} : keys(%{$p_sub_mappings});
			foreach my $sub_key (@sub_keys) {
				my $sub_mappings = $p_sub_mappings->{$sub_key};
				if(exists($p_entry->{$sub_key}) || $sub_mappings->[MAPPING_TYPE]!=BP::Model::ColumnType::SCALAR_CONTAINER) {
					# Then, building the parent data for the submappings which need it
					my $sub_parentData = undef;
					if($sub_mappings->[MAPPING_TYPE]!=BP::Model::ColumnType::SCALAR_CONTAINER) {
						my %parentData = ();
						
						@parentData{keys(%{$sub_mappings->[MAPPING_KEYS_FOR_SUBMAPPINGS]})} = @columnData[values(%{$sub_mappings->[MAPPING_KEYS_FOR_SUBMAPPINGS]})];
						
						$sub_parentData = \%parentData;
					}
					
					$__processData->($sub_mappings,$p_entry->{$sub_key},$sub_parentData);
				} elsif(!exists($p_entry->{$sub_key}) && exists($p_required->{$sub_key})) {
					Carp::croak("[".$p_mappings->[MAPPING_TABLE]."] Values for $sub_key cannot be null!");
				}
			}
		};
		
		if($p_mappings->[MAPPING_TYPE]==BP::Model::ColumnType::HASH_CONTAINER) {
			Carp::croak("Expected a hash, but got a ".ref($data)." on ".$p_mappings->[MAPPING_TABLE])  unless(ref($data) eq 'HASH');
			foreach my $p_key (keys(%{$data})) {
				my $p_entry = $data->{$p_key};
				
				$__doProcess->($p_key,$p_entry);
			}
		} else {
			# Be permissive about what to expect
			$data = [ $data ]  if($p_mappings->[MAPPING_TYPE]==BP::Model::ColumnType::SCALAR_CONTAINER || ref($data) ne 'ARRAY');
			my $p_key = 0;
			foreach my $p_entry (@{$data}) {
				$__doProcess->($p_key,$p_entry);
				
				$p_key++;
			}
		}
		
		$__doProcess = undef;
	};
	
	$__processData->($p_main_mappings,$entorp);
	$__processData = undef;
	
	return \%coldata;
}

# _bulkInsert parameters:
#	destination: The destination of the bulk insertion, which is a set of DBI prepared statements
#	bulkData: a reference to a hash of arrays which contain the values to store.
sub _bulkInsert($\@) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  if(BP::Model::DEBUG && !ref($self));
	
	my $destination = shift;
	
	Carp::croak("ERROR: ".(caller(0))[3]." needs the destination as an array whose first element is a hash of prepared statements")  unless(ref($destination) eq 'ARRAY' && ref($destination->[0]) eq 'HASH');
	
	my($p_dest_hash,$p_main_mappings) = @{$destination};
	
	my $bulkData = shift;
	Carp::croak("ERROR: ".(caller(0))[3]." needs the data as a hash")  unless(ref($bulkData) eq 'HASH');
	
	# The order is preserved because we are using a Tie::IxHash for $destination
	foreach my $bulkKey (keys(%{$p_dest_hash})) {
		if(exists($bulkData->{$bulkKey})) {
			my $bulkArray = $bulkData->{$bulkKey};
			Carp::croak("ERROR: ".(caller(0))[3]." needs an array of arrays for the prepared statement")  unless(ref($bulkArray) eq 'ARRAY');
			
			# Only when we have something to insert it is worth doing all this work
			if(scalar(@{$bulkArray}) > 0) {
				my $destInfo = $p_dest_hash->{$bulkKey};
				my $destSentence = (ref($destInfo) eq 'ARRAY' && exists($destInfo->[0]))?$destInfo->[0]:undef;
				
				Carp::croak("ERROR: ".(caller(0))[3]." needs a prepared statement")  unless(blessed($destSentence) && $destSentence->can('execute'));
				
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
				
				my @tuple_status = ();
				use Data::Dumper;
				unless($destSentence->execute_array({ArrayTupleStatus => \@tuple_status})) {
					foreach my $tuple_id (0..scalar(@tuple_status)) {
						my $status = $tuple_status[$tuple_id];
						next  if(!defined($status) || (!ref($status) && $status==1));
						
						print STDERR "DEBUG [$bulkKey]: Failed tuple (",join(',',map { defined($_->[$tuple_id])?$_->[$tuple_id]:'' } @{$bulkArray}),") Reasons: ",Dumper($status),"\n";
					}
				}
			}
		}
	}
}

1;
