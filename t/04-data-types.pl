#!/usr/bin/env perl

# 04-data-types.pl
#
# Check various data types (work-in-progress)

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# Check Firebird version
# ----------------------

if ($node->{firebird_major_version} >= 3) {
	plan tests => 8;
}
else {
	plan tests => 1;
}

# Prepare table
# --------------

my $table_name = $node->init_data_type_table();

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

	my $q2_res = $node->safe_psql($q2_sql);

	is (
		$q2_res,
		qq/2|t\n3|f\n4|/,
		q|Check BOOLEAN|,
	);

	# 3) Test BOOLEAN "IS TRUE"
	# -------------------------

    my $q3_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS TRUE|,
        $table_name,
    );

    my $q3_res = $node->safe_psql($q3_sql);

	is (
		$q3_res,
		qq/2|t/,
		q|Check BOOLEAN query with "IS TRUE"|,
	);

	# 4) Test BOOLEAN "IS NOT TRUE"
	# -----------------------------

    my $q4_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NOT TRUE|,
        $table_name,
    );

    my $q4_res = $node->safe_psql($q4_sql);

	is (
		$q4_res,
		qq/3|f\n4|/,
		q|Check BOOLEAN query with "IS NOT TRUE"|,
	);

	# 5) Test BOOLEAN "IS FALSE"
	# --------------------------

    my $q5_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS FALSE|,
        $table_name,
    );

    my $q5_res = $node->safe_psql($q5_sql);

	is (
		$q5_res,
		qq/3|f/,
		q|Check BOOLEAN query with "IS FALSE"|,
	);

	# 6) Test BOOLEAN "IS NOT FALSE"
	# ------------------------------

    my $q6_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NOT FALSE|,
        $table_name,
    );

    my $q6_res = $node->safe_psql($q6_sql);

	is (
		$q6_res,
		qq/2|t\n4|/,
		q|Check BOOLEAN query with "IS NOT FALSE"|,
	);

	# 7) Test BOOLEAN "IS NULL"
	# -------------------------

    my $q7_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NULL|,
        $table_name,
    );

    my $q7_res = $node->safe_psql($q7_sql);

	is (
		$q7_res,
		qq/4|/,
		q|Check BOOLEAN query with "IS NULL"|,
	);

	# 8) Test BOOLEAN "IS NOT NULL"
	# -----------------------------

    my $q8_sql = sprintf(
        q|SELECT id, bool_type FROM %s WHERE id >= 2 AND bool_type IS NOT NULL|,
        $table_name,
    );

    my $q8_res = $node->safe_psql($q8_sql);

	is (
		$q8_res,
		qq/2|t\n3|f/,
		q|Check BOOLEAN query with "IS NOT NULL"|,
	);
}


# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
