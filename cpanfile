requires 'perl', '5.012';

# requires 'Some::Module', 'VERSION';
requires 'boolean', '0.32';
requires 'Carp';
requires 'Config::IniFiles';
requires 'Cwd';
requires 'DateTime::Format::ISO8601';
requires 'File::Basename';
requires 'File::Copy';
requires 'File::Path';
requires 'File::Spec';
requires 'File::Temp';
requires 'File::Which';
requires 'JSON';
requires 'Scalar::Util';
requires 'Sys::CPU';
requires 'TeX::Encode';
requires 'XML::LibXML';

# As there is no sane way to enable features , suggests or recommends
# from a dependency in cpanfile, disable them for now
#feature 'mongodb', 'MongoDB support' => sub {
	requires 'MongoDB', 'v0.704.0.0';
	requires 'Tie::IxHash';
#};

#feature 'relational', 'Relational databases support' => sub {
	requires 'Data::Dumper';
	requires 'DBI';
	requires 'Tie::IxHash';
#};

#feature 'elasticsearch', 'Elasticsearch support' => sub {
	requires 'Search::Elasticsearch', '1.12';
	requires 'Tie::IxHash';
#};

#feature 'docgen', 'Documentation Generator support' => sub {
	requires 'Encode';
	requires 'TeX::Encode';
#};

# This syntax is not working (Menlo / cpanm / carton do not acknowledge it)
#requires 'TabParser','0.01', git => 'git://github.com/inab/TabParser.git', ref => '0.01';
requires 'TabParser', '0.01', url => 'https://github.com/inab/TabParser/archive/0.01.tar.gz';
requires 'BP::Model', 'v1.1.1', url => 'https://github.com/inab/BP-Model/archive/v1.1.1.tar.gz';

on test => sub {
    requires 'Test::More', '0.96';
};

on develop => sub {
    requires 'Dist::Milla', '1.0.20';
    requires 'Dist::Zilla::Plugin::MakeMaker';
    requires 'Dist::Zilla::Plugin::Prereqs::FromCPANfile';
};
