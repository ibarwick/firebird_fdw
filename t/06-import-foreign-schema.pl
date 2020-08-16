#!/usr/bin/env perl

# 06-import-foreign-schema.pl
#
# Test "IMPORT FOREIGN SCHEMA" functionality

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;


# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# Get PostgreSQL version
# ----------------------

my $version = $node->pg_version();
if ($version < 90500) {
    plan skip_all => sprintf(
        q|version is %i, tests for 9.5 and later|,
        $version,
    );
}
else {
    plan tests => 5;
}


my $data_table_name = $node->init_data_type_table(firebird_only => 1);


# 1) Test "IMPORT FOREIGN SCHEMA"
# -------------------------------

my $import_foreign_schema_sql = sprintf(
    q|IMPORT FOREIGN SCHEMA foo LIMIT TO (%s) FROM SERVER %s INTO public|,
    $data_table_name,
    $node->server_name(),
);


$node->safe_psql($import_foreign_schema_sql);

my $q1_sql = sprintf(
    q|\d %s|,
    $data_table_name,
);



my $q1_res = $node->safe_psql($q1_sql);

my $q1_expected_output = {};

if ($version < 100000) {
	$q1_expected_output->{2} = <<EO_TXT;
id|integer|not null|
blob_type|text||
EO_TXT

    $q1_expected_output->{3} = <<EO_TXT;
id|integer|not null|
blob_type|text||
bool_type|boolean||
EO_TXT
}
else {
    $q1_expected_output->{2} = <<EO_TXT;
id|integer||not null||
blob_type|text||||
EO_TXT

    $q1_expected_output->{3} = <<EO_TXT;
id|integer||not null||
blob_type|text||||
bool_type|boolean||||
EO_TXT
}

my $q1_expected = $q1_expected_output->{$node->{firebird_major_version}};

chomp $q1_expected;

is (
    $q1_res,
    $q1_expected,
    q|Check IMPORT FOREIGN SCHEMA|,
);



# 2) Test "import_not_null" option
# --------------------------------

my $table_name = $node->init_table(firebird_only => 1);

my $q2_import_foreign_schema_sql = sprintf(
    <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
               LIMIT TO (%s)
            FROM SERVER %s
                   INTO public
                OPTIONS (import_not_null 'false')
EO_SQL
    $table_name,
    $node->server_name(),
);


$node->safe_psql($q2_import_foreign_schema_sql);

my $q2_sql = sprintf(
    q|\d %s|,
    $table_name,
);

my $q2_res = $node->safe_psql($q2_sql);

my $q2_expected = undef;

if ($version < 100000) {
    $q2_expected = <<EO_TXT;
lang_id|character(2)||
name_english|character varying(64)||
name_native|character varying(64)||
EO_TXT
}
else {
    $q2_expected = <<EO_TXT;
lang_id|character(2)||||
name_english|character varying(64)||||
name_native|character varying(64)||||
EO_TXT
}

chomp($q2_expected);

is (
    $q2_res,
    $q2_expected,
    q|Check "import_not_null" option|,
);

$node->drop_foreign_table($table_name);


# 3) Test "updatable" option
# --------------------------

my $q3_import_foreign_schema_sql = sprintf(
    <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
               LIMIT TO (%s)
            FROM SERVER %s
                   INTO public
                OPTIONS (updatable 'false')
EO_SQL
    $table_name,
    $node->server_name(),
);


$node->safe_psql($q3_import_foreign_schema_sql);

my $q3_sql = sprintf(
    <<'EO_SQL',
  SELECT pg_catalog.unnest(ftoptions)
    FROM pg_catalog.pg_foreign_table ft
    JOIN pg_catalog.pg_class c
      ON c.oid=ft.ftrelid
   WHERE c.relname='%s'
EO_SQL
    $table_name,
);

my $q3_res = $node->safe_psql($q3_sql);

my $q3_expected = q|updatable=false|;

is (
    $q3_res,
    $q3_expected,
    q|Check "updatable" option|,
);

# 4) Test default quoted identifier handling (1)
# ----------------------------------------------

# Here we'll just check the "IMPORT FOREIGN SCHEMA" operation
# succeeds

my $q4_tbl_name = $node->make_table_name(uc_prefix => 1);

my $q4_fb_sql = sprintf(
    <<'EO_SQL',
CREATE TABLE "%s" (
   "col1" INT,
   "UClc" INT,
   unquoted INT,
   "lclc" INT
)
EO_SQL
    $q4_tbl_name,
);

$node->firebird_execute_sql($q4_fb_sql);

my $q4_import_sql = sprintf(
    <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
               LIMIT TO ("%s")
            FROM SERVER fb_test
                   INTO public
EO_SQL
    $q4_tbl_name,
);

my ($q4_res, $q4_stdout, $q4_stderr) = $node->psql(
    $q4_import_sql,
);

is (
    $q4_res,
    q|0|,
    q|Check "IMPORT FOREIGN SCHEMA" operation succeeds|,
);

# 5) Test default quoted identifier handling (2)
# ----------------------------------------------

# Here we'll check the previous "IMPORT FOREIGN SCHEMA" operation
# created table name and columns correctly

# Query won't return any results if table name was created incorrectly
my $q5_sql = sprintf(
    <<'EO_SQL',
    SELECT a.attname
      FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_attribute a
        ON a.attrelid = c.oid
INNER JOIN pg_catalog.pg_namespace n
        ON n.oid = c.relnamespace
     WHERE c.relname = '%s'
       AND n.nspname = 'public'
       AND a.attnum > 0
  ORDER BY a.attnum
EO_SQL
    $q4_tbl_name,
);

my $q5_res = $node->safe_psql($q5_sql);

my $q5_expected = <<EO_TXT;
col1
UClc
unquoted
lclc
EO_TXT

chomp $q5_expected;

is (
    $q5_res,
    $q5_expected,
    q|Check table and column names created correctly|,
);

# Clean up
# --------

$node->drop_foreign_server();

$node->firebird_drop_table($table_name);
$node->firebird_drop_table($data_table_name);
$node->firebird_drop_table($q4_tbl_name, 1);

done_testing();
