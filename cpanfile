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
	requires 'File::ShareDir';
#};

# Next dependencies are in the DarkPAN
requires 'TabParser', '0.01';
requires 'BP::Model', 'v1.1.1';

on test => sub {
    requires 'Test::More', '0.96';
};

on develop => sub {
    requires 'Dist::Milla', '1.0.20';
    requires 'Dist::Zilla::Plugin::MakeMaker';
    requires 'Dist::Zilla::Plugin::Prereqs::FromCPANfile';
    requires 'Dist::Zilla::Plugin::Run', '0.048';
    requires 'OrePAN2';
};
