#!/usr/bin/env perl

# 11-pushdowns.pl
#
# Check support for generated columns

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();


if ($node->{firebird_major_version} >= 3) {
	plan tests => 3;
}
else {
    plan skip_all => sprintf(
        q|Firebird version is %i, no tests for this yet|,
        $node->{firebird_major_version},
    );
}

# Prepare table
# --------------

my $table_name = $node->init_data_type_table();


if ($node->{firebird_major_version} >= 3) {

    # Prepare table data
    # ------------------

	my $bool_insert_sql = sprintf(
		<<EO_SQL,
INSERT INTO %s (id, bool_type)
         VALUES(1, TRUE), (2, FALSE), (3, NULL);
EO_SQL
		$table_name,
	);

	$node->safe_psql($bool_insert_sql);

    # 1. Check basic pushdown
    # -----------------------

    my $explain_1_q = sprintf(
        q|EXPLAIN SELECT * FROM %s WHERE bool_type IS TRUE|,
        $table_name,
	);

    my ($explain_1_res, $explain_1_stdout, $explain_1_stderr) = $node->psql(
        $explain_1_q,
    );

    my $explain_1_expected = sprintf(
        q|Firebird query: SELECT.+?WHERE\s+\(\(bool_type IS TRUE\)\)|,
    );

    like (
        $explain_1_stdout,
        qr/$explain_1_expected/,
        q|Check basic pushdown|,
    );


    # 2. Check IS NOT TRUE
    # --------------------


    my $explain_2_q = sprintf(
        q|EXPLAIN SELECT * FROM %s WHERE bool_type IS NOT TRUE|,
        $table_name,
	);

    my ($explain_2_res, $explain_2_stdout, $explain_2_stderr) = $node->psql(
        $explain_2_q,
    );

    my $explain_2_expected = sprintf(
        q|Firebird query: SELECT.+?WHERE\s+\(\(bool_type IS FALSE\) OR \(bool_type IS NULL\)\)|,
    );

    like (
        $explain_2_stdout,
        qr/$explain_2_expected/,
        q|Check pushdown with IS NOT TRUE|,
    );

    # 3. Check IS NOT FALSE
    # ---------------------

    my $explain_3_q = sprintf(
        q|EXPLAIN SELECT * FROM %s WHERE bool_type IS NOT FALSE|,
        $table_name,
	);

    my ($explain_3_res, $explain_3_stdout, $explain_3_stderr) = $node->psql(
        $explain_3_q,
    );

    my $explain_3_expected = sprintf(
        q|Firebird query: SELECT.+?WHERE\s+\(\(bool_type IS TRUE\) OR \(bool_type IS NULL\)\)|,
    );

    like (
        $explain_3_stdout,
        qr/$explain_3_expected/,
        q|Check pushdown with IS NOT FALSE|,
    );


}

# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
