#!/usr/bin/env perl

# 10-generated-columns.pl
#
# Check support for generated columns

use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();


# Get PostgreSQL version
# ----------------------

my $version = $node->pg_version();
if ($version < 120000) {
    plan skip_all => sprintf(
        q|version is %i, tests for 12 and later|,
        $version,
    );
}


# Create tables
# -------------

my $table_name = $node->make_table_name();

# Note that Firebird accepts "GENERATED ALWAYS AS" but renders
# it as "COMPUTED BY". It does not accept the STORED suffix.
#
# Reference:
#   https://firebirdsql.org/file/documentation/html/en/refdocs/fblangref50/firebird-50-language-reference.html#fblangref50-ddl-tbl-computedby

my $tbl_sql = sprintf(
    <<EO_SQL,
CREATE TABLE %s (
   a INT,
   b INT GENERATED ALWAYS AS (a * 2)
)
EO_SQL
    $table_name,
);

my $tbl_query = $node->firebird_conn()->prepare($tbl_sql);
$tbl_query->execute();
$tbl_query->finish();

my $ftbl_sql = sprintf(
    <<EO_SQL,
CREATE FOREIGN TABLE %s (
   a INT,
   b INT GENERATED ALWAYS AS (a * 2) STORED
)
SERVER %s
OPTIONS (table_name '%s')
EO_SQL
    $table_name,
    $node->{server_name},
    $table_name,
);

$node->safe_psql($ftbl_sql);

# 1. Check INSERT
# ---------------

my $insert_q1 = sprintf(
    <<EO_SQL,
  INSERT INTO %s VALUES(1), (2)
EO_SQL
    $table_name,
);

$node->safe_psql($insert_q1);

my $select_q1 = sprintf(
    q|SELECT * FROM %s ORDER BY a|,
    $table_name,
);

my $q1 = $node->firebird_conn()->prepare($select_q1);

$q1->execute();

my $res1 = $node->firebird_format_results($q1);

$q1->finish();

my $expected1 = <<EO_TXT;
1|2
2|4
EO_TXT

chomp($expected1);

is(
	$res1,
	$expected1,
	'insert OK',
);


# 2. Check UPDATE
# ---------------


my $update_q2 = sprintf(
    <<EO_SQL,
  UPDATE %s SET a = 22 WHERE a = 2
EO_SQL
    $table_name,
);

$node->safe_psql($update_q2);

my $select_q2 = sprintf(
    q|SELECT * FROM %s ORDER BY a|,
    $table_name,
);

my $q2 = $node->firebird_conn()->prepare($select_q2);

$q2->execute();

my $res2 = $node->firebird_format_results($q2);

$q2->finish();

my $expected2 = <<EO_TXT;
1|2
22|44
EO_TXT

chomp($expected2);

is(
	$res2,
	$expected2,
	'insert OK',
);

# Cleanup
# -------

$node->firebird_drop_table($table_name);

done_testing();
