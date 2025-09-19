#!/usr/bin/env perl

# 06-import-foreign-schema.pl
#
# Test "IMPORT FOREIGN SCHEMA" functionality

use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;


# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();
my $version = $node->pg_version();
my $min_compat_version = $node->get_min_compat_version();

# 1) Test "IMPORT FOREIGN SCHEMA"
# -------------------------------

my $q1_table_name = $node->init_data_type_table(firebird_only => 1);

note "import table name is '$q1_table_name'";

my $q1_import_sql = sprintf(
    q|IMPORT FOREIGN SCHEMA foo LIMIT TO (%s) FROM SERVER %s INTO public|,
    $q1_table_name,
    $node->server_name(),
);

my ($q1_res, $q1_stdout, $q1_stderr) = $node->psql(
    $q1_import_sql,
);

is (
    $q1_res,
    q|0|,
    q|Check "IMPORT FOREIGN SCHEMA" operation succeeds|,
);


# 1a) verify table imported
# =========================

my $q1a_sql = sprintf(
    q|\d %s|,
    $q1_table_name,
);

my ($q1a_res, $q1a_stdout, $q1a_stderr) = $node->psql($q1a_sql);

my $q1a_expected_output = {};

$q1a_expected_output->{2} = <<EO_TXT;
id|integer||not null||
blob_type|text||||
implicit_bool_type|smallint||||
EO_TXT

$q1a_expected_output->{3} = <<EO_TXT;
id|integer||not null||
blob_type|text||||
bool_type|boolean||||
implicit_bool_type|smallint||||
uuid_type|character(16)||||
time_type|time without time zone||||
timestamp_type|timestamp without time zone||||
EO_TXT

$q1a_expected_output->{4} = <<EO_TXT;
id|integer||not null||
blob_type|text||||
bool_type|boolean||||
implicit_bool_type|smallint||||
uuid_type|character(16)||||
int128_type|numeric(39,0)||||
time_type|time without time zone||||
timestamp_type|timestamp without time zone||||
ttz_type|time with time zone||||
tstz_type|timestamp with time zone||||
EO_TXT

my $q1a_expected = $q1a_expected_output->{$min_compat_version};

chomp $q1a_expected;

is (
    $q1a_stdout,
    $q1a_expected,
    q|Check IMPORT FOREIGN SCHEMA|,
);


# 2) Test "import_not_null" option
# --------------------------------

my $q2_table_name = $node->init_table(firebird_only => 1);

my $q2_import_foreign_schema_sql = sprintf(
    <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
               LIMIT TO (%s)
            FROM SERVER %s
                   INTO public
                OPTIONS (import_not_null 'false')
EO_SQL
    $q2_table_name,
    $node->server_name(),
);


$node->safe_psql($q2_import_foreign_schema_sql);

my $q2_sql = sprintf(
    q|\d %s|,
    $q2_table_name,
);

my $q2_res = $node->safe_psql($q2_sql);

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

$node->drop_foreign_table($q2_table_name);


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
    $q2_table_name,
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
    $q2_table_name,
);

my ($q3_res, $q3_stdout, $q3_stderr) = $node->psql($q3_sql);

my $q3_expected = q|updatable=false|;

is (
    $q3_stdout,
    $q3_expected,
    q|Check "updatable" option|,
);

# 4) Test default quoted identifier handling (1)
# ----------------------------------------------

# Here we'll just check the "IMPORT FOREIGN SCHEMA" operation
# succeeds

my $q4_table_name = $node->make_table_name(uc_prefix => 1);

my $q4_fb_sql = sprintf(
    <<'EO_SQL',
CREATE TABLE "%s" (
   "col1" INT,
   "UClc" INT,
   unquoted INT,
   "lclc" INT
)
EO_SQL
    $q4_table_name,
);

$node->firebird_execute_sql($q4_fb_sql);

my $q4_import_sql = sprintf(
    <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
               LIMIT TO ("%s")
            FROM SERVER fb_test
                   INTO public
EO_SQL
    $q4_table_name,
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
    $q4_table_name,
);

my ($q5_res, $q5_stdout, $q5_stderr) = $node->psql($q5_sql);

my $q5_expected = <<EO_TXT;
col1
UClc
unquoted
lclc
EO_TXT

chomp $q5_expected;

is (
    $q5_stdout,
    $q5_expected,
    q|Check table and column names created correctly|,
);

# 6) Test quoted identifier handling with all-upper case table name
# -----------------------------------------------------------------

my $q6_table_name = uc($node->make_table_name());

$node->init_table(
    firebird_only => 1,
    table_name => $q6_table_name,
);


my $q6_import_sql = sprintf(
    <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
               LIMIT TO ("%s")
            FROM SERVER fb_test
                   INTO public
EO_SQL
    $q6_table_name,
);

my ($q6_res, $q6_stdout, $q6_stderr) = $node->psql(
    $q6_import_sql,
);

is (
    $q6_res,
    q|0|,
    q|Check "IMPORT FOREIGN SCHEMA" operation succeeds|,
);


# 7) Verify table is actually imported
# ------------------------------------

# Check the table specified in the previous test was actually
# imported. If not handled correctly, the PostgreSQL FDW API
# will silently discard table definitions generated by the FDW.
#
# We don't need to check the schema here.

my $q7_sql = sprintf(
    <<'EO_SQL',
    SELECT COUNT(*)
      FROM pg_catalog.pg_class
     WHERE oid = '"%s"'::regclass
EO_SQL
    $q6_table_name,
);

my ($q7_res, $q7_stdout, $q7_stderr) = $node->psql($q7_sql);

is (
    $q7_stdout,
    q|1|,
    q|Check table was imported|,
);


# 8) Test "IMPORT SCHEMA ... EXCEPT"
# ----------------------------------

$node->drop_foreign_table($q1_table_name);
$node->drop_foreign_table($q2_table_name);
$node->drop_foreign_table($q4_table_name);
$node->drop_foreign_table($q6_table_name);

my $q8_import_sql = sprintf(
    <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
                 EXCEPT (%s, "%s", "%s")
            FROM SERVER fb_test
                   INTO public
EO_SQL
    # Default case
    $q1_table_name,
    # UC prefix
    $q4_table_name,
    # All upper-case
    $q6_table_name,
);

my ($q8_res, $q8_stdout, $q8_stderr) = $node->psql(
    $q8_import_sql,
);

is (
    $q8_res,
    q|0|,
    q|Check "IMPORT FOREIGN SCHEMA ... EXCEPT" operation succeeds|,
);

# 8a) Check expected table imported
# --------------------------------

my $q8a_sql = sprintf(
    <<'EO_SQL',
    SELECT COUNT(*)
      FROM pg_catalog.pg_class
     WHERE oid = '%s'::regclass
EO_SQL
    $q2_table_name,
);

my ($q8a_res, $q8a_stdout, $q8a_stderr) = $node->psql($q8a_sql);

is (
    $q8a_stdout,
    q|1|,
    q|Check expected table was imported after "IMPORT FOREIGN SCHEMA ... EXCEPT" operation|,
);

# 8b) Check other tables not imported
# -----------------------------------

my $q8b_sql = sprintf(
    <<'EO_SQL',
    SELECT COUNT(*)
      FROM pg_catalog.pg_class
     WHERE relname IN (
       '%s',
       '%s',
       '%s'
     )
EO_SQL
    # Default case
    $q1_table_name,
    # UC prefix
    $q4_table_name,
    # All upper-case
    $q6_table_name,
);

my ($q8b_res, $q8b_stdout, $q8b_stderr) = $node->psql($q8b_sql);

is (
    $q8b_stdout,
    q|0|,
    q|Check other tables not imported|,
);


# 9) Check relations with names > 32 characters can be imported
# -------------------------------------------------------------

if ($node->{firebird_major_version} >= 4) {

    my $q9_table_name = '';

    # Maximum identifier size in Firebird 4+ is 63 characters
    foreach $a (0..62) {
        $q9_table_name .= chr(int(26*rand) + 65);
    }

    $node->firebird_execute_sql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (id INT NOT NULL)
EO_SQL
            $q9_table_name,
        ),
    );

    my $q9_import_sql = sprintf(
        <<'EO_SQL',
  IMPORT FOREIGN SCHEMA foo
               LIMIT TO (%s)
            FROM SERVER fb_test
                   INTO public
EO_SQL
        $q9_table_name,
    );

    my ($q9_res, $q9_stdout, $q9_stderr) = $node->psql(
        $q9_import_sql,
    );

    is (
        $q9_res,
        q|0|,
        q|Check "IMPORT FOREIGN SCHEMA" succeeds with long table name|,
    );

    $node->firebird_drop_table($q9_table_name);
}

# Clean up
# --------

$node->drop_foreign_server();

$node->firebird_drop_table($q1_table_name);
$node->firebird_drop_table($q2_table_name);
$node->firebird_drop_table($q4_table_name, 1);
$node->firebird_drop_table($q6_table_name, 1);

done_testing();
