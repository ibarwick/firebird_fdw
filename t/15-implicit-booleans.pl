#!/usr/bin/env perl

# 15-implicit-booleans.pl
#
# Check support for implicit booleans

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

my @implicit_boolean_pushdowns = (
    ['IS TRUE' ,     q|\(\(implicit_bool_type <> 0\)\)|],
    ['IS FALSE',     q|\(\(implicit_bool_type = 0\)\)|],
    ['IS NOT TRUE',  q|\(\(implicit_bool_type = 0\) OR \(implicit_bool_type IS NULL\)\)|],
    ['IS NOT FALSE', q|\(\(implicit_bool_type <> 0\) OR \(implicit_bool_type IS NULL\)\)|],
    ['IS NULL',      q|\(\(implicit_bool_type IS NULL\)\)|],
    ['IS NOT NULL',  q|\(\(implicit_bool_type IS NOT NULL\)\)|],
);

my $test_cnt = scalar @implicit_boolean_pushdowns;
$test_cnt += 3;

plan tests => $test_cnt;

$node->alter_server_option('implicit_bool_type', 'true');

my $table_name = $node->init_data_type_table();

# 1. Implicit Boolean pushdowns
# -----------------------------

# Prepare table data
# ------------------


# my $bool_insert_sql = sprintf(
#     <<EO_SQL,
# INSERT INTO %s (id, implicit_bool_type)
#          VALUES(1, 1), (2, 0), (3, NULL);
# EO_SQL
#     $table_name,
# );

# $node->safe_psql($bool_insert_sql);

foreach my $implicit_boolean_pushdown (@implicit_boolean_pushdowns) {
    my $explain_q = sprintf(
        q|EXPLAIN SELECT * FROM %s WHERE implicit_bool_type %s|,
        $table_name,
        $implicit_boolean_pushdown->[0],
    );

    my ($explain_res, $explain_stdout, $explain_stderr) = $node->psql(
        $explain_q,
    );

    my $explain_expected = sprintf(
        q|Firebird query: SELECT.+?WHERE\s+%s|,
        $implicit_boolean_pushdown->[1],
    );

    like (
        $explain_stdout,
        qr/$explain_expected/,
        sprintf(
            q|Check implicit pushdown for "%s"|,
            $implicit_boolean_pushdown->[0],
        ),
    );
}


# 2. Check column retrieval
# -------------------------

# Value to insert directly into Firebird

my @values_2 = (
    [1, 1],
    [2, 0],
    [3, undef],
    [4, 2],
    [5, -1],
);


my $insert_2 = sprintf(
    <<EO_SQL,
 INSERT INTO %s
             (id, implicit_bool_type)
      VALUES (?, ?)
EO_SQL
    $table_name,
);

my $fb_query_2 = $node->firebird_conn()->prepare($insert_2);

foreach my $value_set (@values_2) {

    $fb_query_2->execute(
        $value_set->[0],
        $value_set->[1],
    );
}

$fb_query_2->finish();

my $expected_2 = <<EO_TXT;
1|t
2|f
3|
4|t
5|t
EO_TXT

chomp($expected_2);

my $query_2 = sprintf(
    q|SELECT id, implicit_bool_type FROM %s ORDER BY id|,
    $table_name,
);

my $q2_res = $node->safe_psql($query_2);

is (
    $q2_res,
    $expected_2,
    q|Check retrieval of implicit bool column values|,
);

# 3. Check INSERT
# ---------------

my $insert_3 = sprintf(
    <<EO_SQL,
 INSERT INTO %s
             (id, implicit_bool_type)
      VALUES (6, false),
             (7, true),
             (8, NULL)
EO_SQL
    $table_name,
);

$node->safe_psql($insert_3);

# Check the expected values arrived in Firebird
my $select_q3 = sprintf(
    q|SELECT id, implicit_bool_type FROM %s WHERE id >= 6 ORDER BY id|,
    $table_name,
);

my $fb_query_3 = $node->firebird_conn()->prepare($select_q3);

$fb_query_3->execute();

my $res_3 = $node->firebird_format_results($fb_query_3);

$fb_query_3->finish();

my $expected_3 = <<EO_TXT;
6|0
7|1
8|
EO_TXT

chomp $expected_3;

is (
    $res_3,
    $expected_3,
    q|Check implicit boolean inserts|,
);

# 4. Check INSERT ... RETURNING
# -----------------------------

my $insert_4 = sprintf(
    <<EO_SQL,
 INSERT INTO %s
             (id, implicit_bool_type)
      VALUES (9, false),
             (10, true),
             (11, NULL)
   RETURNING id, implicit_bool_type
EO_SQL
    $table_name,
);


my $res_4 = $node->safe_psql($insert_4);

my $expected_4 =  <<EO_TXT;
9|f
10|t
11|
EO_TXT

chomp $expected_4;

is (
    $res_4,
    $expected_4,
    q|Check implicit boolean inserts with RETURNING|,
);


# Clean up
# --------

$node->firebird_drop_table($table_name);


$node->drop_foreign_server();

done_testing();
