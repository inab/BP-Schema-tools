
CREATE TABLE exp_m (
	analysis_id VARCHAR(64) NOT NULL,
	sample_id VARCHAR(64) NOT NULL,
	assembly_version VARCHAR(64) NOT NULL,
	gene_build_version INTEGER NOT NULL,
	platform VARCHAR(512) NOT NULL,
	experimental_protocol VARCHAR(512),
	base_calling_algorithm VARCHAR(512) NOT NULL,
	alignment_algorithm VARCHAR(512) NOT NULL,
	normalization_algorithm VARCHAR(512) NOT NULL,
	other_analysis_algorithm VARCHAR(512),
	seq_coverage FLOAT(5,2),
	raw_data_repository VARCHAR(128),
	raw_data_accession VARCHAR(128),
	note TEXT
);


CREATE TABLE exp_g (
	analysis_id VARCHAR(64) NOT NULL,
	sample_id VARCHAR(64) NOT NULL,
	gene_stable_id VARCHAR(64) NOT NULL,
	gene_chromosome VARCHAR(64) NOT NULL,
	gene_strand INTEGER NOT NULL,
	gene_start INTEGER NOT NULL,
	gene_end INTEGER NOT NULL,
	normalized_read_count FLOAT(5,2) NOT NULL,
	raw_read_count INTEGER NOT NULL,
	normalized_expression_level FLOAT(5,2),
	fold_change FLOAT(5,2),
	reference_sample VARCHAR(64),
	quality_score INTEGER,
	probability FLOAT(3,2),
	is_annotated VARCHAR(64),
	validation_status VARCHAR(64) NOT NULL,
	validation_platform VARCHAR(512),
	probeset_id VARCHAR(128),
	note TEXT
);


CREATE TABLE cngv_m (
	analysis_id VARCHAR(64) NOT NULL,
	control_sample_id VARCHAR(64) NOT NULL,
	matched_sample_id VARCHAR(64) NOT NULL,
	assembly_version VARCHAR(64) NOT NULL,
	platform VARCHAR(512) NOT NULL,
	experimental_protocol VARCHAR(512),
	base_calling_algorithm VARCHAR(512) NOT NULL,
	alignment_algorithm VARCHAR(512) NOT NULL,
	variation_calling_algorithm VARCHAR(512) NOT NULL,
	other_analysis_algorithm VARCHAR(512),
	seq_coverage FLOAT(5,2),
	raw_data_repository VARCHAR(512),
	raw_data_accession VARCHAR(512),
	note TEXT
);


CREATE TABLE cngv_p (
	analysis_id TEXT NOT NULL,
	control_sample_id TEXT NOT NULL,
	variation_id VARCHAR(128) NOT NULL,
	variation_type VARCHAR(64) NOT NULL,
	chromosome VARCHAR(64) NOT NULL,
	chromosome_start INTEGER NOT NULL,
	chromosome_end INTEGER NOT NULL,
	chromosome_strand INTEGER NOT NULL,
	refsnp_allele VARCHAR(512) NOT NULL,
	refsnp_strand INTEGER,
	reference_genome_allele VARCHAR(512) NOT NULL,
	control_genotype VARCHAR(512) NOT NULL,
	tumour_genotype VARCHAR(512) NOT NULL,
	expressed_allele VARCHAR(512),
	quality_score INTEGER,
	probability FLOAT(3,2),
	read_count FLOAT(5,2),
	is_annotated VARCHAR(64),
	validation_status VARCHAR(64) NOT NULL,
	validation_platform VARCHAR(512),
	xref_ensembl_var_id VARCHAR(128),
	note TEXT
);


CREATE TABLE jcn_m (
	analysis_id VARCHAR(64) NOT NULL,
	sample_id TEXT NOT NULL,
	assembly_version VARCHAR(64) NOT NULL,
	gene_build_version INTEGER NOT NULL,
	platform VARCHAR(512) NOT NULL,
	experimental_protocol VARCHAR(512),
	base_calling_algorithm VARCHAR(512) NOT NULL,
	alignment_algorithm VARCHAR(512) NOT NULL,
	normalization_algorithm VARCHAR(512) NOT NULL,
	other_analysis_algorithm VARCHAR(512),
	seq_coverage FLOAT(5,2),
	raw_data_repository VARCHAR(128) NOT NULL,
	raw_data_accession VARCHAR(128) NOT NULL,
	note TEXT
);


CREATE TABLE jcn_p (
	analysis_id VARCHAR(64) NOT NULL,
	sample_id VARCHAR(64) NOT NULL,
	junction_id VARCHAR(256) NOT NULL,
	gene_stable_id VARCHAR(64) NOT NULL,
	gene_chromosome VARCHAR(64) NOT NULL,
	gene_strand INTEGER NOT NULL,
	gene_start INTEGER NOT NULL,
	gene_end INTEGER NOT NULL,
	second_gene_stable_id VARCHAR(64),
	exon1_chromosome VARCHAR(64) NOT NULL,
	exon1_number_bases INTEGER NOT NULL,
	exon1_end INTEGER NOT NULL,
	exon1_strand INTEGER,
	exon2_chromosome VARCHAR(64) NOT NULL,
	exon2_number_bases INTEGER NOT NULL,
	exon2_start INTEGER NOT NULL,
	exon2_strand INTEGER,
	is_fusion_gene VARCHAR(16),
	is_novel_splice_form VARCHAR(16),
	junction_seq TEXT,
	junction_type VARCHAR(64),
	junction_read_count FLOAT(5,2) NOT NULL,
	quality_score INTEGER,
	probability FLOAT(3,2),
	validation_status VARCHAR(64) NOT NULL,
	validation_platform VARCHAR(512),
	note TEXT
);


CREATE TABLE meth_s (
	analysis_id TEXT NOT NULL,
	analyzed_sample_id TEXT NOT NULL,
	methylated_fragment_id TEXT NOT NULL,
	gene_affected VARCHAR(128) NOT NULL,
	gene_build_version INTEGER NOT NULL,
	note TEXT
);


CREATE TABLE meth_m (
	analysis_id VARCHAR(64) NOT NULL,
	analyzed_sample_id VARCHAR(64) NOT NULL,
	matched_sample_id VARCHAR(64) NOT NULL,
	assembly_version VARCHAR(64) NOT NULL,
	platform VARCHAR(512) NOT NULL,
	experimental_protocol VARCHAR(512),
	base_calling_algorithm VARCHAR(512) NOT NULL,
	alignment_algorithm VARCHAR(512) NOT NULL,
	variation_calling_algorithm VARCHAR(512) NOT NULL,
	other_analysis_algorithm VARCHAR(512),
	raw_data_repository VARCHAR(128),
	raw_data_accession VARCHAR(128),
	note TEXT
);


CREATE TABLE meth_p (
	analysis_id TEXT NOT NULL,
	analyzed_sample_id TEXT NOT NULL,
	methylated_fragment_id VARCHAR(128) NOT NULL,
	chromosome VARCHAR(64) NOT NULL,
	chromosome_start INTEGER NOT NULL,
	chromosome_end INTEGER NOT NULL,
	chromosome_strand INTEGER,
	beta_value_methylation FLOAT(5,2),
	beta_value_hydroxymethylation FLOAT(5,2),
	quality_score_methylation INTEGER,
	quality_score_hydroxymethylation INTEGER,
	probability_methylation FLOAT(3,2),
	probability_hydroxymethylation FLOAT(3,2),
	validation_status VARCHAR(64) NOT NULL,
	validation_platform VARCHAR(512),
	note TEXT
);


CREATE TABLE pdna_m (
	analysis_id VARCHAR(64) NOT NULL,
	analyzed_sample_id VARCHAR(64) NOT NULL,
	matched_sample_id VARCHAR(64) NOT NULL,
	assembly_version VARCHAR(64) NOT NULL,
	platform VARCHAR(512) NOT NULL,
	experimental_protocol VARCHAR(512),
	base_calling_algorithm VARCHAR(512) NOT NULL,
	alignment_algorithm VARCHAR(512) NOT NULL,
	variation_calling_algorithm VARCHAR(512) NOT NULL,
	other_analysis_algorithm VARCHAR(512),
	raw_data_repository VARCHAR(128),
	raw_data_accession VARCHAR(128),
	NSC FLOAT(5,2),
	RSC FLOAT(5,2),
	note TEXT
);


CREATE TABLE pdna_p (
	analysis_id TEXT NOT NULL,
	analyzed_sample_id TEXT NOT NULL,
	protein_dna_interaction_id VARCHAR(128) NOT NULL,
	protein_stable_id VARCHAR(128) NOT NULL,
	chromosome VARCHAR(64) NOT NULL,
	chromosome_start INTEGER NOT NULL,
	chromosome_end INTEGER NOT NULL,
	chromosome_strand INTEGER,
	idr FLOAT(5,2) NOT NULL,
	fdr FLOAT(5,2),
	rank_type VARCHAR(64),
	rank_value FLOAT(5,2),
	validation_status VARCHAR(64) NOT NULL,
	validation_platform VARCHAR(512),
	note TEXT
);


CREATE TABLE pdna_s (
	analysis_id TEXT NOT NULL,
	analyzed_sample_id TEXT NOT NULL,
	protein_dna_interaction_id TEXT NOT NULL,
	gene_affected VARCHAR(128) NOT NULL,
	transcript_affected VARCHAR(128) NOT NULL,
	gene_build_version INTEGER NOT NULL,
	note TEXT
);


CREATE TABLE sample (
	analyzed_sample_id VARCHAR(64) NOT NULL,
	specimen_id VARCHAR(64) NOT NULL,
	analyzed_sample_type VARCHAR(128) NOT NULL,
	analyzed_sample_type_other VARCHAR(64),
	analyzed_sample_interval INTEGER,
	analyzed_sample_notes TEXT
);


CREATE TABLE donor (
	donor_id VARCHAR(64) NOT NULL,
	donor_sex VARCHAR(128) NOT NULL,
	donor_region_of_residence VARCHAR(64) NOT NULL,
	donor_vital_status VARCHAR(128) NOT NULL,
	donor_age_at_diagnosis INTEGER NOT NULL,
	donor_diagnosis_do VARCHAR(64) NOT NULL,
	donor_notes TEXT
);


CREATE TABLE specimen (
	donor_id VARCHAR(64) NOT NULL,
	specimen_id VARCHAR(64) NOT NULL,
	specimen_type VARCHAR(128) NOT NULL,
	specimen_type_other VARCHAR(64) NOT NULL,
	specimen_processing VARCHAR(128) NOT NULL,
	specimen_processing_other VARCHAR(64) NOT NULL,
	specimen_storage VARCHAR(128) NOT NULL,
	specimen_storage_other VARCHAR(64) NOT NULL,
	specimen_biobank VARCHAR(64) NOT NULL,
	specimen_biobank_id VARCHAR(64) NOT NULL,
	specimen_available VARCHAR(128) NOT NULL,
	specimen_notes TEXT
);


CREATE TABLE family (
	donor_id TEXT NOT NULL,
	relationship_type VARCHAR(128) NOT NULL,
	relationship_type_other TEXT NOT NULL,
	relationship_sex VARCHAR(128) NOT NULL,
	relationship_age INTEGER NOT NULL,
	relationship_diagnosis_do TEXT NOT NULL,
	relationship_diagnosis TEXT NOT NULL
);


CREATE TABLE rreg_p (
	analysis_id TEXT NOT NULL,
	analyzed_sample_id TEXT NOT NULL,
	regulatory_region_id VARCHAR(128) NOT NULL,
	chromosome VARCHAR(64) NOT NULL,
	chromosome_start INTEGER NOT NULL,
	chromosome_end INTEGER NOT NULL,
	chromosome_strand INTEGER,
	normalized_read_count FLOAT(5,2) NOT NULL,
	raw_read_count INTEGER NOT NULL,
	validation_status VARCHAR(64) NOT NULL,
	validation_platform VARCHAR(512),
	note TEXT
);


CREATE TABLE rreg_s (
	analysis_id TEXT NOT NULL,
	analyzed_sample_id TEXT NOT NULL,
	regulatory_region_id TEXT NOT NULL,
	gene_affected VARCHAR(128) NOT NULL,
	gene_build_version INTEGER NOT NULL,
	note TEXT
);


CREATE TABLE rreg_m (
	analysis_id VARCHAR(64) NOT NULL,
	analyzed_sample_id VARCHAR(64) NOT NULL,
	matched_sample_id VARCHAR(64) NOT NULL,
	assembly_version VARCHAR(64) NOT NULL,
	platform VARCHAR(512) NOT NULL,
	experimental_protocol VARCHAR(512),
	base_calling_algorithm VARCHAR(512) NOT NULL,
	alignment_algorithm VARCHAR(512) NOT NULL,
	variation_calling_algorithm VARCHAR(512) NOT NULL,
	other_analysis_algorithm VARCHAR(512),
	raw_data_repository VARCHAR(128),
	raw_data_accession VARCHAR(128),
	note TEXT
);

