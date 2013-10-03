BP-Schema-tools
===============

Bioinformatic Pantry Schema tools depend on several libraries and command line tools:

Core
----

The bpmodel validator and the core libraries are written in Perl. Their requisites are:

* Archive::Zip
* DateTime::Format::ISO8601
* Digest::SHA1
* Encode
* File::Basename
* File::Copy
* File::Spec
* File::Temp
* IO::File
* JSON
* Sys::CPU
* XML::LibXML
* URI

Many of these modules could already be included in your standard Perl installation

Documentation Generator (optional)
----------------------------------

The documentation generator relies on LaTeX document preparation system. So, the needed requisites are:

* TeXLive 2012, with XeLaTeX and pdfLaTeX enabled.
* graphviz, version 2.30.1 or above (graph layout program)
* dot2tex, version 2.8.7 (https://code.google.com/p/dot2tex/)
* Make sure next LaTeX packages are installed (most of them are already in a full TeXLive installation):
  import, babel, ifxetex, ifluatex, fontspec, xunicode, pdftexcmds, fontenc, inputenc, longtable, tabularx,
  graphicx, hyperref, forarray, color, xcolor, colortbl, pbox, navigator, ocg-p, pdflscape, ifthen, hypcap,
  capt-of, tikz, adjustbox, fancyvrb, multirow, fullpage,chngcntr
* Make sure next TeX and OpenType fonts are installed: iwona, Consolas, beramono


Database Loader
---------------

If you want to populate a MongoDB database with tabular files which follow a model validated by BP Schema tools, these are the additionanl requisites:

* MongoDB 2.4.6 or later (earlier versions had corruption problems), 64 bit version, with V8 javascript engine
*	MongoDB Perl module from CPAN, along with its dependences.

If you are lucky and you can install MongoDB by package, then most of the configuration work is done. In any case, you should take into account where the database files are going to be stored. Depending on the Linux distro you are using, either you will only have to set up the right paths on /etc/mongod.conf file, or you have to change a variables file which is used by the startup init script (like in Gentoo).

In any case, be sure that the configuration file contains a 'journal = true' declaration, or alternatively, MongoDB daemon (mongod) is being run with --journal parameter.

Generic MongoDB REST frontend
-----------------------------

If you want to give a REST frontend to a MongoDB database, then install

* nodejs (at least 0.10.17), along with npm, its package manager.
* mongodb-rest nodejs package (and all its dependencies)
