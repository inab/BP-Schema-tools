# NAME

BP::Loader - Bioinformatic Pantry data Model processing classes

# SYNOPSIS

```perl

    use BP::Model;
    use BP::Loader::CorrelatableConcept;
    use BP::Loader::Mapper;
    
    my $model = BP::Model->new($modelFile);

```

# DESCRIPTION

BP::Model is the keystone of a data modelling methodology created under
the umbrella of [BLUEPRINT project](https://blueprint-epigenome.eu).

# RATIONALE

We have created this methodology and tools because we needed a foundation
for the data models of several research projects, like BLUEPRINT and
RD-Connect, which are nearer to semi-structured, hierarchical models
(like ASN.1 or XML) than to relational ones, and whose data are going to
be stored in hierarchical storages like JSON-based NoSQL databases, where
the database schema definition and constraints are almost non-existent.
The data modeling methodology around which BP::Model and BP-Schema-tools
have been designed is inspired on extended entity relationship (EER) model
and methodology, and object-oriented programming. The methodology has
concept domains (in the case of BLUEPRINT model, regulatory regions,
protein-DNA interactions, exon junction, DNA methylation, sample data,
etc...) where the different concepts, which would be the entities in a
EER model, are defined. Following with the inspiration on EER methodology,
we can define directional relations (without attributes) in our model
between two concepts from the same or different concept domain. Also, we
can define identification relations between a boss and a subordinate (aka
weak) concepts from the same concept domain, which corresponds to weak
entities and identifying relationships from EER methodology.

As our models on NGS-related projects contain lots of repetitions (for
instance, almost each data contains chromosome coordinates), we have
created "concept types" in the methodology, which are concept templates
(something like interfaces in Java programming language) without any
relationship information. When the concepts are defined, they must be
based on one or more concept types on its definition, or in a base
concept (like subentities in EER model or subclasses in Java programming
language). Our methodology allows more complex data structures
(multidimensional matrices, key-value dictionaries, ...) and content
restrictions (regular expressions, different types of NULL values) for
each column than simple types. Our methodology allows defining both inline
and external controlled vocabularies and ontologies, which can be used as
a restriction on the values for each column.

This methodology is skewed to receive and load the data in the database
in tabular formats, inspired on ICGC DCC. These formats are easy to
generate, easy to parse by program and easy to load from analysis
workbenches, like R, Cytoscape or Galaxy. Almost any database management
system (like MySQL, PostgreSQL, SQLite, MongoDB, Cassandra, Riak,
etc....) allows bulk loading them. So, our model also needs to track the
correlation and representation of the input tabular file formats with the
model used for BLUEPRINT DCC. For that reason, we have included in the
model additional details, like the separators for multidimensional array
values, the way to format or extract keys and values from dictionaries,
etc... For each project we also needed to generate documentation about
its corresponding data model, so the documentation is synchronized to the
data model itself. So, the model can embed both basic documentation and
annotations about the concepts, the columns and the used controlled
vocabularies.

In the early stages of the development of BP-Schema-tools, all these
requirements pushed as to define an XML Schema based on our data modeling
methodology to describe the model itself. The next step was to build a
programming library which allowed us validating and working with the
model. We wrote the library in Perl because, although it is one of the
worst languages to do OOP, it is one of the programming languages with
the better and faster pattern matching support. The next step was to
write the module of the documentation generator. As we wanted as
distributable documentation PDF documents with enough quality, the
documentation generator creates a set of diagrams and LaTeX documents
from the input model and the controlled vocabularies it uses, describing
it in high detail and embedding the needed internal and external links.
The documentation generator embeds on each run the definition of the data
model into the PDF, as an attachment, so the model is not lost. The
documentation generator also integrates in the process custom
documentation snippets written in LaTeX for each one of the defined
concepts.

As we realized in the early stages of BLUEPRINT (the project which
inspired us to create BP-Schema-tools) that we could need to replace the
underlying database technology used to store all the DCC data, we had the
need to support first a fallback plan (i.e. BioMart). So, we also created
a module which writes to a couple of files the SQL database definition
sentences needed for a BioMart database. Currently, the SQL tables have
an almost one to one correspondence with the concepts in the model, but
it could diverge in the future, which requires a database loader which
understands the data model and the transformations.

At last, we have created (we are still testing it) a modular database
data loader, which validates the input tabular files against the
definitions and restrictions of the used model, generates the needed
schema definitions for the destination database paradigm (currently
relational, MongoDB and ElasticSearch databases are supported), and it
bulk loads the data in the database.

# METHODS

_(to be documented)_

# INSTALLATION

Latest release of this package is available in the [BSC INB DarkPAN](https://gitlab.bsc.es/inb/darkpan/). You
can install it just using `cpanm`:

```bash

    cpanm --mirror-only --mirror https://gitlab.bsc.es/inb/darkpan/raw/master/ --mirror https://cpan.metacpan.org/ BP::Loader

```

# AUTHOR

José M. Fernández [https://github.com/jmfernandez](https://github.com/jmfernandez)

# COPYRIGHT

The library was initially created several years ago for the data
management tasks in the
[BLUEPRINT project](http://www.blueprint-epigenome.eu/).

Copyright 2019- José M. Fernández & Barcelona Supercomputing Center (BSC)

# LICENSE

These libraries are free software; you can redistribute them and/or modify
them under the Apache 2 terms.

# SEE ALSO
