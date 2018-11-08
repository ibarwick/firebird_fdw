#!/usr/bin/env perl

# 06-import-foreign-schema.pl
#
# Test "IMPORT FOREIGN SCHEMA" functionality

use strict;
use warnings;

use Cwd;
use Config;
use TestLib;
use Test::More tests => 2;

use FirebirdFDWNode;
use FirebirdFDWDB;


# Initialize nodes
# ----------------

my $pg_node = get_new_fdw_node('pg_node');

$pg_node->init();
$pg_node->start();

my $pg_db = FirebirdFDWDB->new($pg_node);

my $data_table_name = $pg_db->init_data_table(firebird_only => 1);


# 1) Test "IMPORT FOREIGN SCHEMA"
# -------------------------------

my $import_foreign_schema_sql = sprintf(
    q|IMPORT FOREIGN SCHEMA foo LIMIT TO (%s) FROM SERVER fb_test INTO public|,
    $data_table_name,
);


$pg_db->safe_psql($import_foreign_schema_sql);

my $q1_sql = sprintf(
    q|\d %s|,
    $data_table_name,
);

my $q1_res = $pg_db->safe_psql($q1_sql);

my $q1_expected_output = {
	2 => <<EO_TXT,
id|integer||not null||
blob_type|text||||
EO_TXT
	3 => <<EO_TXT,
id|integer||not null||
blob_type|text||||
bool_type|boolean||||
EO_TXT
};

my $q1_expected = $q1_expected_output->{$pg_node->{firebird_major_version}};

chomp $q1_expected;

is (
    $q1_res,
    $q1_expected,
    q|Check IMPORT FOREIGN SCHEMA|,
);

# 2) Test "import_not_null" option
# --------------------------------


my $q2_table_name = $pg_db->init_table(firebird_only => 1);

my $q2_import_foreign_schema_sql = sprintf(
    q|IMPORT FOREIGN SCHEMA foo LIMIT TO (%s) FROM SERVER fb_test INTO public OPTIONS (import_not_null 'false')|,
    $q2_table_name,
);


$pg_db->safe_psql($q2_import_foreign_schema_sql);

my $q2_sql = sprintf(
    q|\d %s|,
    $q2_table_name,
);

my $q2_res = $pg_db->safe_psql($q2_sql);


my $q2_expected = <<EO_TXT;
lang_id|character(2)||||
name_english|character varying(64)||||
name_native|character varying(64)||||
EO_TXT

chomp($q2_expected);

is (
    $q2_res,
    $q2_expected,
    q|Check "import_not_null" option|,
);
