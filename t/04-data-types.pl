#!/usr/bin/env perl

# 04-data-types.pl
#
# Check various data types (work-in-progress)

use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# Check Firebird version
# ----------------------

if ($node->{firebird_major_version} >= 3) {
	plan tests => 10;
}
else {
	plan tests => 3;
}

# Prepare table
# --------------

my $table_name = $node->init_data_type_table();

note qq|data type table name: "$table_name"|;

# 1) Test BLOB type
# -----------------

my $blob_insert_sql = sprintf(
    <<EO_SQL,
INSERT INTO %s (id, blob_type)
         VALUES(1, 'foo\nbar\nテスト');
EO_SQL
    $table_name,
);

$node->safe_psql($blob_insert_sql);

my $q1_sql = sprintf(
    q|SELECT blob_type FROM %s WHERE id = 1|,
    $table_name,
);

my $q1 = $node->firebird_conn()->prepare($q1_sql);

$q1->execute();

my $q1_res = $q1->fetchrow_array();

$q1->finish();

is (
    $q1_res,
    qq/foo\nbar\nテスト/,
    q|Check BLOB (subtype TEXT)|,
);


if ($node->{firebird_major_version} >= 3) {

	# 2) Test BOOLEAN type (basic)
	# ----------------------------
    #
    # Check that inserted values will round-trip

	my $bool_insert_sql = sprintf(
		<<EO_SQL,
INSERT INTO %s (id, bool_type)
         VALUES(2, TRUE), (3, FALSE), (4, NULL);
EO_SQL
		$table_name,
	);

	$node->safe_psql($bool_insert_sql);

	my $q2_sql = sprintf(
		q|SELECT id, bool_type FROM %s WHERE id IN (2,3,4) ORDER BY id|,
		$table_name,
	);

	my ($q2_res, $q2_stdout, $q2_stderr) = $node->psql($q2_sql);

	is (
		$q2_stdout,
		qq/2|t\n3|f\n4|/,
		q|Check BOOLEAN|,
	);

	# 3) Test BOOLEAN "IS TRUE"
	# -------------------------

    my $q3_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS TRUE|,
        $table_name,
    );

    my ($q3_res, $q3_stdout, $q3_stderr) = $node->psql($q3_sql);

	is (
		$q3_stdout,
		qq/2|t/,
		q|Check BOOLEAN query with "IS TRUE"|,
	);

	# 4) Test BOOLEAN "IS NOT TRUE"
	# -----------------------------

    my $q4_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NOT TRUE|,
        $table_name,
    );

    my ($q4_res, $q4_stdout, $q4_stderr) = $node->psql($q4_sql);

	is (
		$q4_stdout,
		qq/3|f\n4|/,
		q|Check BOOLEAN query with "IS NOT TRUE"|,
	);

	# 5) Test BOOLEAN "IS FALSE"
	# --------------------------

    my $q5_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS FALSE|,
        $table_name,
    );

    my ($q5_res, $q5_stdout, $q5_stderr) = $node->psql($q5_sql);

	is (
		$q5_stdout,
		qq/3|f/,
		q|Check BOOLEAN query with "IS FALSE"|,
	);

	# 6) Test BOOLEAN "IS NOT FALSE"
	# ------------------------------

    my $q6_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NOT FALSE|,
        $table_name,
    );

    my ($q6_res, $q6_stdout, $q6_stderr) = $node->psql($q6_sql);

	is (
		$q6_stdout,
		qq/2|t\n4|/,
		q|Check BOOLEAN query with "IS NOT FALSE"|,
	);

	# 7) Test BOOLEAN "IS NULL"
	# -------------------------

    my $q7_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NULL|,
        $table_name,
    );

    my ($q7_res, $q7_stdout, $q7_stderr) = $node->psql($q7_sql);

	is (
		$q7_stdout,
		qq/4|/,
		q|Check BOOLEAN query with "IS NULL"|,
	);

	# 8) Test BOOLEAN "IS NOT NULL"
	# -----------------------------

    my $q8_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NOT NULL|,
        $table_name,
    );

    my ($q8_res, $q8_stdout, $q8_stderr) = $node->psql($q8_sql);

	is (
		$q8_stdout,
		qq/2|t\n3|f/,
		q|Check BOOLEAN query with "IS NOT NULL"|,
	);
}


# 9) Test UUID type
# -----------------
#
# Note: currently insertion of UUID values into OCTETS columns is not supported,
# so we need to generate the UUID in Firebird.

$node->firebird_execute_sql(
    sprintf(
        <<'EO_SQL',
INSERT INTO %s
  (id, uuid_type)
VALUES
  (5, (SELECT gen_uuid() FROM rdb$database))
EO_SQL
        $table_name,
    ),
);

# retrieve UUID from Firebird as formatted text, normalized to lower case
# (so it matches the value PostgreSQL will retrieve via the FDW)

my $uuid_std = $node->firebird_single_value_query(
    sprintf(
        <<'EO_SQL',
SELECT lower(uuid_to_char(uuid_type))
  FROM %s
 WHERE id = 5
EO_SQL
        $table_name,
    ),
);

note "uuid $uuid_std";

my $q9_sql = sprintf(
    sprintf(
        <<'EO_SQL',
SELECT uuid_type
  FROM %s
 WHERE id = 5
EO_SQL
        $table_name,
    ),
);

my ($q9_res, $q9_stdout, $q9_stderr) = $node->psql($q9_sql);

is (
    $q9_stdout,
    qq/$uuid_std/,
    q|Check UUID value retrieved correctly|,
);

# 10) Test UUID type comparison
# -----------------------------

# The Perl DBD driver returns OCTET values as raw bytes;
# convert the raw bytes into the hexadecimal equivalent

my $uuid_raw = $node->firebird_single_value_query(
    sprintf(
        <<'EO_SQL',
SELECT uuid_type
  FROM %s
 WHERE id = 5
EO_SQL
        $table_name,
    ),
);

my $uuid_nonstd = '';

for (my $i = 0; $i < length($uuid_raw); $i++) {
    $uuid_nonstd .= sprintf(q|%02X|, ord(substr($uuid_raw, $i, 1)));
}

note "uuid $uuid_nonstd";

my $q10_sql = sprintf(
    sprintf(
        <<'EO_SQL',
SELECT id
  FROM %s
 WHERE uuid_type = '%s'::uuid
EO_SQL
        $table_name,
        $uuid_nonstd,
    ),
);

my ($q10_res, $q10_stdout, $q10_stderr) = $node->psql($q10_sql);

is (
    $q10_stdout,
    qq/5/,
    q|Check UUID value retrieved correctly|,
);

# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
