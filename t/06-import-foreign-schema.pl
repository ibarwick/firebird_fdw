#!/usr/bin/env perl

# 05-import-foreign-schema.pl
#
# Test "IMPORT FOREIGN SCHEMA" functionality

use strict;
use warnings;

use Cwd;
use Config;
use TestLib;
use Test::More tests => 1;

use FirebirdFDWNode;
use FirebirdFDWDB;


# Initialize nodes
# ----------------

my $pg_node = get_new_fdw_node('pg_node');

$pg_node->init();
$pg_node->start();

my $pg_db = FirebirdFDWDB->new($pg_node);

my $table_name = $pg_db->init_data_table(firebird_only => 1);


# 1) Test "IMPORT FOREIGN SCHEMA"
# -------------------------------

my $import_foreign_schema_sql = sprintf(
    q|IMPORT FOREIGN SCHEMA foo LIMIT TO (%s) FROM SERVER fb_test INTO public|,
    $table_name,
);


$pg_db->safe_psql($import_foreign_schema_sql);

my $q1_sql = sprintf(
    q|\d %s|,
    $table_name,
);

my $q1_res = $pg_db->safe_psql($q1_sql);

my $q1_expected = <<EO_TXT;
id|integer||not null||
blob_type|text||||
bool_type|boolean||||
EO_TXT

chomp $q1_expected;

is (
    $q1_res,
    $q1_expected,
    q|Check IMPORT FOREIGN SCHEMA|,
);
