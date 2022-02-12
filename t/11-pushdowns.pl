#!/usr/bin/env perl

# 11-pushdowns.pl
#
# Check support for generated columns

use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

my @boolean_pushdowns = (
    ['IS TRUE' ,     q|\(\(bool_type IS TRUE\)\)|],
    ['IS FALSE',     q|\(\(bool_type IS FALSE\)\)|],
    ['IS NOT TRUE',  q|\(\(bool_type IS FALSE\) OR \(bool_type IS NULL\)\)|],
    ['IS NOT FALSE', q|\(\(bool_type IS TRUE\) OR \(bool_type IS NULL\)\)|],
    ['IS NULL',      q|\(\(bool_type IS NULL\)\)|],
    ['IS NOT NULL',  q|\(\(bool_type IS NOT NULL\)\)|],
);


my $test_cnt = 0;

if ($node->{firebird_major_version} >= 3) {
    $test_cnt += scalar @boolean_pushdowns;
}

if ($test_cnt) {
    plan tests => $test_cnt;
}
else {
    plan skip_all => sprintf(
        q|No tests for Firebird version %s|,
        $node->{firebird_major_version},
    );
}

# 1. Boolean pushdowns (Firebird 3+)
# ----------------------------------

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

    foreach my $boolean_pushdown (@boolean_pushdowns) {
        my $explain_q = sprintf(
            q|EXPLAIN SELECT * FROM %s WHERE bool_type %s|,
            $table_name,
            $boolean_pushdown->[0],
        );

        my ($explain_res, $explain_stdout, $explain_stderr) = $node->psql(
            $explain_q,
        );

        my $explain_expected = sprintf(
            q|Firebird query: SELECT.+?WHERE\s+%s|,
            $boolean_pushdown->[1],
        );

        like (
            $explain_stdout,
            qr/$explain_expected/,
            sprintf(
                q|Check pushdown for "%s"|,
                $boolean_pushdown->[0],
            ),
        );
    }

}



# Clean up
# --------

$node->drop_foreign_server();

done_testing();
