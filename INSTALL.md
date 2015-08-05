BP-Schema-tools
===============

Bioinformatic Pantry Schema tools depend on several libraries and command line tools:

Core
----

The bpmodel validator and the core libraries are written in Perl. Their requisites are:

* Archive::Zip
* boolean
* DateTime::Format::ISO8601
* Digest::SHA1
* Encode
* File::Basename
* File::Copy
* File::Spec
* File::Temp
* File::Which
* IO::File
* Log::Log4perl
* XML::LibXML
* URI

Many of these modules could already be included in your standard Perl installation

DocumentationGenerator (optional)
----------------------------------

The documentation generator classes heavy rely on LaTeX document preparation system. So, the needed requisites are:

* These additional Perl modules:
	* Config::IniFiles
	* TeX::Encode
	* Image::ExifTool (from ExifTool package)
* TeXLive 2013, with XeLaTeX and pdfLaTeX enabled.
* graphviz, version 2.30.1 or above (graph layout program)
* dot2tex, version 2.8.7 or later (https://code.google.com/p/dot2tex/)
* Make sure next LaTeX packages are installed (most of them are already in a full TeXLive installation):
  import, babel, ifxetex, ifluatex, fontspec, xunicode, pdftexcmds, fontenc, inputenc, longtable, tabularx,
  graphicx, hyperref, forarray, color, xcolor, colortbl, pbox, navigator, ocg-p, pdflscape, ifthen, hypcap,
  capt-of, tikz, adjustbox, fancyvrb, multirow, fullpage, chngcntr
* Make sure next TeX and OpenType fonts are installed: iwona, Consolas, beramono


Database Loader (model-mapper.pl , Elasticsearch , MongoDB, Relational)
-------------------------------------

If you want to populate a database with tabular files which follow a model validated by BP Schema tools, these are the additionanl requisites:

* Config::IniFiles
* JSON
* Sys::CPU

For Relational, depending on the target database instance, their installation procedures vary. The needed Perl modules are:

* DBI
* DBD::Pg (for PostgreSQL)
* DBD::mysql (for MySQL)
* DBD::SQLite (for SQLite 3.x)

For MongoDB:

* Use MongoDB 2.6.x or later (earlier versions had corruption and concurrency problems), 64 bit version, with V8 javascript engine. If you are lucky and you can install MongoDB module by package, then most of the configuration work is done. In any case, you should take into account where the database files are going to be stored. Depending on the Linux distro you are using, either you will only have to set up the right paths on /etc/mongod.conf file, or you have to change a variables file which is used by the startup init script (like in Gentoo). In any case, be sure that the configuration file contains a 'journal = true' declaration, or alternatively, MongoDB daemon (mongod) is being run with --journal parameter.
* These additional Perl modules from CPAN, along with their dependences.
	* MongoDB

For Elasticsearch:

* Use Elasticsearch 1.4.x or later. If you want to issue incremental updates, you also need to enable dynamic scripting.
* These additional Perl modules from CPAN, along with their dependencies.
	* Search::Elasticsearch
	* Tie::IxHash
