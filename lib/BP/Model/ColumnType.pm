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
use Scalar::Util;

use BP::Model::Common;
use BP::Model::ColumnType::Common;

use BP::Model::CompoundType;
use BP::Model::CV::Meta;

package BP::Model::ColumnType;

use constant {
	TYPE	=>	0,
	CONTAINER_TYPE	=>	1,
	USE	=>	2,
	RESTRICTION	=>	3,
	DEFAULT	=>	4,
	SETSEPS	=>	5,
	ARRAYSEPS	=>	6,
	KEYHASHSEP	=>	7,
	VALHASHSEP	=>	8,
	ALLOWEDNULLS	=>	9,
	DATAMANGLER	=>	10,
	DATACHECKER	=>	11,
};

use constant STR2CONTAINER => {
	'scalar'	=>	SCALAR_CONTAINER,
	'set'		=>	SET_CONTAINER,
	'array'		=>	ARRAY_CONTAINER,
	'hash'		=>	HASH_CONTAINER
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
	# container type
	# column use (idref, required, optional)
	# content restrictions
	# default value
	# set separators
	# array separators
	# key separator
	# value separator
	# null values
	# data mangler
	# data checker
	my @nullValues = ();
	my @columnType = (undef,undef,undef,undef,undef,undef,undef,undef,undef,\@nullValues);
	
	# Let's parse the column type!
	foreach my $colType ($containerDecl->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'column-type')) {
		#First, the item type
		my $itemType = $colType->getAttribute('item-type');
		
		my $refItemType = $model->getItemType($itemType);
		Carp::croak("unknown type '$itemType' for column $columnName"."\nOffending XML fragment:\n".$colType->toString()."\n")  unless(defined($refItemType));
		
		$columnType[BP::Model::ColumnType::TYPE] = $itemType;
		
		my $containerType = BP::Model::ColumnType::SCALAR_CONTAINER;
		if($colType->hasAttribute('container-type')) {
			my $contType = $colType->getAttribute('container-type');
			$containerType = (BP::Model::ColumnType::STR2CONTAINER)->{$contType}  if(exists((BP::Model::ColumnType::STR2CONTAINER)->{$contType}));
		}
		
		$columnType[BP::Model::ColumnType::CONTAINER_TYPE] = $containerType;
		
		# Column use
		my $columnKind = $colType->getAttribute('column-kind');
		# Idref equals 0; required, 1; desirable, -1; optional, -2
		if(exists((BP::Model::ColumnType::STR2TYPE)->{$columnKind})) {
			$columnType[BP::Model::ColumnType::USE] = (BP::Model::ColumnType::STR2TYPE)->{$columnKind};
		} else {
			Carp::croak("Column $columnName has a unknown kind: $columnKind"."\nOffending XML fragment:\n".$colType->toString()."\n");
		}
		
		# Content restrictions (children have precedence over attributes)
		# Let's save allowed null values
		foreach my $null ($colType->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'null')) {
			my $val = $null->textContent();
			
			if($model->isValidNull($val)) {
				# Let's save the default value
				push(@nullValues,$val);
			} else {
				Carp::croak("Column $columnName uses an unknown default value: $val"."\nOffending XML fragment:\n".$colType->toString()."\n");
			}
		}
		
		# First, is it a compound type?
		my @compChildren = $colType->getChildrenByTagNameNS(BP::Model::Common::dccNamespace,'compound-type');
		if(defined($refItemType->[BP::Model::ColumnType::DATATYPEMANGLER]) && (scalar(@compChildren)>0 || $colType->hasAttribute('compound-type'))) {
			Carp::croak("Column $columnName does not use a compound type, but it was declared"."\nOffending XML fragment:\n".$colType->toString()."\n");
		} elsif(!defined($refItemType->[BP::Model::ColumnType::DATATYPEMANGLER]) && scalar(@compChildren)==0 && !$colType->hasAttribute('compound-type')) {
			Carp::croak("Column $columnName uses a compound type, but it was not declared"."\nOffending XML fragment:\n".$colType->toString()."\n");
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
				Carp::croak("Column $columnName tried to use undeclared compound type ".$colType->getAttribute('compound-type')."\nOffending XML fragment:\n".$colType->toString()."\n");
			}
		} else {
			$restriction = BP::Model::CV::Meta->parseMetaCV($colType,$model);
			
			# No children, so try using the attributes
			unless(defined($restriction)) {
				if($colType->hasAttribute('cv')) {
					my $namedCV = $model->getNamedCV($colType->getAttribute('cv'));
					if(defined($namedCV)) {
						$restriction = $namedCV;
					} else {
						Carp::croak("Column $columnName tried to use undeclared CV ".$colType->getAttribute('cv')."\nOffending XML fragment:\n".$colType->toString()."\n");
					}
				} elsif($colType->hasAttribute('pattern')) {
					my $PAT = $model->getNamedPattern($colType->getAttribute('pattern'));
					if(defined($PAT)) {
						$restriction = qr/^$PAT$/;
					} else {
						Carp::croak("Column $columnName tried to use undeclared pattern ".$colType->getAttribute('pattern')."\nOffending XML fragment:\n".$colType->toString()."\n");
					}
				} 
			}
		}
		$columnType[BP::Model::ColumnType::RESTRICTION] = $restriction;
		
		
		# Setting up the data checker
		my $dataChecker = \&__true;
		
		if(Scalar::Util::blessed($restriction)) {
			# We are covering here both compound type checks and CV checks
			if($restriction->can('dataChecker')) {
				$dataChecker = $restriction->dataChecker;
			} elsif($restriction->isa('Regexp')) {
				$dataChecker = sub { $_[0] =~ $restriction };
			}
		} else {
			# Simple type checks
			$dataChecker = $refItemType->[BP::Model::ColumnType::DATATYPECHECKER]  if(defined($refItemType->[BP::Model::ColumnType::DATATYPECHECKER]));
		}
		
		# Setting up the data mangler
		my $dataMangler = (Scalar::Util::blessed($restriction) && $restriction->can('dataMangler')) ? $restriction->dataMangler : $refItemType->[BP::Model::ColumnType::DATATYPEMANGLER];
		
		# Default value
		my $defval = $colType->hasAttribute('default')?$colType->getAttribute('default'):undef;
		# Default values must be rechecked once all the columns are available
		$columnType[BP::Model::ColumnType::DEFAULT] = (defined($defval) && substr($defval,0,2) eq '$$') ? \substr($defval,2): $defval;
		
		# Array and set separators
		$columnType[BP::Model::ColumnType::SETSEPS] = undef;
		$columnType[BP::Model::ColumnType::ARRAYSEPS] = undef;
		if($containerType==BP::Model::ColumnType::SET_CONTAINER || $containerType==BP::Model::ColumnType::ARRAY_CONTAINER || ($containerType==BP::Model::ColumnType::HASH_CONTAINER && ($colType->hasAttribute('set-seps') || $colType->hasAttribute('array-seps')))) {
			Carp::croak('set-seps attribute must be defined when the container type is "set"'."\nOffending XML fragment:\n".$colType->toString()."\n")  if($containerType==BP::Model::ColumnType::SET_CONTAINER && !$colType->hasAttribute('set-seps'));
			Carp::croak('array-seps attribute must be defined when the container type is "array"'."\nOffending XML fragment:\n".$colType->toString()."\n")  if($containerType==BP::Model::ColumnType::ARRAY_CONTAINER && !$colType->hasAttribute('array-seps'));
			
			my $seps = $colType->getAttribute($colType->hasAttribute('array-seps')?'array-seps':'set-seps');
			if(length($seps) > 0) {
				my %sepVal = ();
				my @sepsArr = split(//,$seps);
				foreach my $sep (@sepsArr) {
					if(exists($sepVal{$sep})) {
						Carp::croak("Column $columnName has repeated the separator $sep!"."\nOffending XML fragment:\n".$colType->toString()."\n")
					}
					
					$sepVal{$sep}=undef;
				}
				$columnType[$colType->hasAttribute('array-seps')?BP::Model::ColumnType::ARRAYSEPS : BP::Model::ColumnType::SETSEPS] = $seps;
				
				# Altering the data mangler in order to handle multidimensional matrices
				my $itemDataMangler = $dataMangler;
				$dataMangler = sub {
					my $result = [$_[0]];
					my @frags = ($result);
					my $countdown = $#sepsArr;
					foreach my $sep (@sepsArr) {
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
					my $result = [$_[0]];
					my @frags = ($result);
					my $countdown = $#sepsArr;
					foreach my $sep (@sepsArr) {
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
		} elsif($colType->hasAttribute('set-seps')) {
			Carp::croak('"container-type" must be either "set" or "hash" in order to use "set-seps" attribute!'."\nOffending XML fragment:\n".$colType->toString()."\n");
		} elsif($colType->hasAttribute('array-seps')) {
			Carp::croak('"container-type" must be either "array" or "hash" in order to use "array-seps" attribute!'."\nOffending XML fragment:\n".$colType->toString()."\n");
		}
		
		# We have to define the modifications to the data mangler and data checker
		if($containerType==BP::Model::ColumnType::HASH_CONTAINER) {
			# Default values
			my $keySep = ':';
			my $valSep = ';';
			
			$keySep = $colType->getAttribute('hash-key-sep')  if($colType->hasAttribute('hash-key-sep'));
			$valSep = $colType->getAttribute('hash-value-sep')  if($colType->hasAttribute('hash-value-sep'));
			
			$columnType[BP::Model::ColumnType::KEYHASHSEP] = $keySep;
			$columnType[BP::Model::ColumnType::VALHASHSEP] = $valSep;
			
			# Altering the data mangler in order to handle multidimensional matrices
			my $itemDataMangler = $dataMangler;
			$dataMangler = sub {
				my @keyvals = split($valSep,$_[0]);
				
				my %resHash = ();
				
				foreach my $keyval  (@keyvals) {
					my($key,$value) = split($keySep,$keyval,2);
					
					$resHash{$key} = $itemDataMangler->($value);
				}
				
				return \%resHash;
			};
			
			# Altering the data checker in order to handle multidimensional matrices
			my $itemDataChecker = $dataChecker;
			$dataChecker = sub {
				my @keyvals = split($valSep,$_[0]);
				
				foreach my $keyval  (@keyvals) {
					my($key,$value) = split($keySep,$keyval,2);
					
					return undef  unless($itemDataChecker->($value));
				}
				
				return 1;
			};
		} elsif($colType->hasAttribute('hash-key-sep') || $colType->hasAttribute('hash-value-sep')) {
			Carp::croak('"hash-key-sep" and "hash-value-sep" attributes must only be defined when the container type is "hash"'."\nOffending XML fragment:\n".$colType->toString()."\n");
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

# Container type
sub containerType {
	return $_[0]->[BP::Model::ColumnType::CONTAINER_TYPE];
}

# column use (idref, required, optional)
# Idref equals 0; required, 1; optional, -1
sub use {
	return $_[0]->[BP::Model::ColumnType::USE];
}

# content restrictions. Either
# BP::Model::CompoundType
# BP::Model::CV::Meta
# Pattern
sub restriction {
	return $_[0]->[BP::Model::ColumnType::RESTRICTION];
}

# default value
sub default {
	return $_[0]->[BP::Model::ColumnType::DEFAULT];
}

# set separators
sub setSeps {
	return $_[0]->[BP::Model::ColumnType::SETSEPS];
}

# set separators
sub setDimensions {
	defined($_[0]->[BP::Model::ColumnType::SETSEPS])?length($_[0]->[BP::Model::ColumnType::SETSEPS]):0;
}

# array separators
sub arraySeps {
	return $_[0]->[BP::Model::ColumnType::ARRAYSEPS];
}

# array separators
sub arrayDimensions {
	defined($_[0]->[BP::Model::ColumnType::ARRAYSEPS])?length($_[0]->[BP::Model::ColumnType::ARRAYSEPS]):0;
}

# array separators
sub hashKeySep {
	return $_[0]->[BP::Model::ColumnType::KEYHASHSEP];
}

# array separators
sub hashValuesSep {
	return $_[0]->[BP::Model::ColumnType::VALHASHSEP];
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

sub setUse($) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $val = shift;
	
	$self->[BP::Model::ColumnType::USE] = $val;
}

# clone parameters:
#	relatedConcept: optional BP::Model::RelatedConcept instance, it signals whether to change cloned columnType
#		according to relatedConcept hints
#	scalarize: If it is set, it resets the container type to SCALAR_CONTAINER
# it returns a BP::Model::ColumnType instance
sub clone(;$$) {
	my $self = shift;
	
	Carp::croak((caller(0))[3].' is an instance method!')  unless(ref($self));
	
	my $relatedConcept = shift;
	my $scalarize = shift;
	
	Carp::croak('Input parameter must be a BP::Model::RelatedConcept')  unless(!defined($relatedConcept) || (Scalar::Util::blessed($relatedConcept) && $relatedConcept->isa('BP::Model::RelatedConcept')));
	
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
			# We don't need explicit indexes
			$retval->[BP::Model::ColumnType::CONTAINER_TYPE] = BP::Model::ColumnType::SET_CONTAINER  if($retval->[BP::Model::ColumnType::CONTAINER_TYPE]!=BP::Model::ColumnType::ARRAY_CONTAINER);
		}
	} elsif($scalarize) {
		$retval->[BP::Model::ColumnType::CONTAINER_TYPE] = BP::Model::ColumnType::SCALAR_CONTAINER;
		$retval->[BP::Model::ColumnType::ARRAYSEPS] = undef;
	}
	
	return $retval;
}

1;
