#!/usr/bin/env perl

# 14-tuple-routing.pl
#
# Check support for tuple routing

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

my $version = $node->pg_version();

if ($version < 110000) {
    plan skip_all => sprintf(
        q|version is %i, tests for 11 and later|,
        $version,
    );
}
else {
    plan tests => 3;
}

# Create a partitioned table for use by multiple tests
# ----------------------------------------------------
my $parent_table_name = $node->make_table_name();
my $child_table_1 = sprintf(q|%s_child1|, $parent_table_name);
my $child_table_2 = sprintf(q|%s_child2|, $parent_table_name);

# Create Firebird tables

$node->firebird_execute_sql(
    sprintf(
        <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL,
  val VARCHAR(32)
)
EO_SQL
        $child_table_1,
    ),
);

$node->firebird_execute_sql(
    sprintf(
        <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL,
  val VARCHAR(32)
)
EO_SQL
        $child_table_2,
    ),
);

# Create partitioned PostgreSQL tables

$node->safe_psql(
    sprintf(
        <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL,
  val VARCHAR(32)
) PARTITION BY RANGE (id)
EO_SQL
        $parent_table_name,
    ),
);

$node->safe_psql(
    sprintf(
        <<EO_SQL,
CREATE FOREIGN TABLE %s (
  id INT NOT NULL,
  val VARCHAR(32)
)
SERVER %s
EO_SQL
        $child_table_1,
        $node->server_name(),
    ),
);


$node->safe_psql(
    sprintf(
        <<EO_SQL,
CREATE FOREIGN TABLE %s (
  id INT NOT NULL,
  val VARCHAR(32)
)
SERVER %s
EO_SQL
        $child_table_2,
        $node->server_name(),
    ),
);

$node->safe_psql(
    sprintf(
        <<EO_SQL,
       ALTER TABLE %s
  ATTACH PARTITION %s FOR VALUES FROM (100) TO (199);
EO_SQL
        $parent_table_name,
        $child_table_1,
    ),
);

$node->safe_psql(
    sprintf(
        <<EO_SQL,
       ALTER TABLE %s
  ATTACH PARTITION %s FOR VALUES FROM (200) TO (299);
EO_SQL
        $parent_table_name,
        $child_table_2,
    ),
);


# 1. Check COPY into partitioned table (1)
# ----------------------------------------


$node->safe_psql(
    sprintf(
        <<'EO_SQL',
COPY %s FROM STDIN (format 'csv');
101,"foo"
102,"bar"
\.
EO_SQL
        $parent_table_name,
    ),
);

my $select_q1_t1 = sprintf(
    q|SELECT * FROM %s ORDER BY 1|,
    $child_table_1,
);


my $q1_t1 = $node->firebird_conn()->prepare($select_q1_t1);

$q1_t1->execute();

my $res1_t1 = $node->firebird_format_results($q1_t1);

$q1_t1->finish();

my $expected1_t1 = <<EO_TXT;
101|foo
102|bar
EO_TXT

chomp($expected1_t1);

is(
	$res1_t1,
	$expected1_t1,
	'COPY into partitioned table OK (1)',
);



# 2. Check copy into partitioned table (2)
# ----------------------------------------


$node->safe_psql(
    sprintf(
        <<'EO_SQL',
COPY %s FROM STDIN (format 'csv');
201,"baz"
202,"qux"
\.
EO_SQL
        $parent_table_name,
    ),
);

my $select_q2_t2 = sprintf(
    q|SELECT * FROM %s ORDER BY 1|,
    $child_table_2,
);

my $q2_t2 = $node->firebird_conn()->prepare($select_q2_t2);

$q2_t2->execute();

my $res2_t2 = $node->firebird_format_results($q2_t2);

$q2_t2->finish();

my $expected2_t2 = <<EO_TXT;
201|baz
202|qux
EO_TXT

chomp($expected2_t2);

is(
	$res2_t2,
	$expected2_t2,
	'COPY into partitioned table OK (2)',
);



# 3. Check INSERT INTO ... RETURNING with partitioned table
# ---------------------------------------------------------

my $res3 = $node->safe_psql(
    sprintf(
        <<'EO_SQL',
INSERT INTO %s VALUES(203,'mooh') RETURNING *;
EO_SQL
        $parent_table_name,
    ),
);

my $expected3 = <<EO_TXT;
203|mooh
EO_TXT

chomp $expected3;

is(
	$res3,
	$expected3,
	'Check INSERT INTO ... RETURNING with partitioned table',
);


# Clean up
# --------

$node->firebird_drop_table($child_table_1);
$node->firebird_drop_table($child_table_2);

$node->drop_foreign_server();

done_testing();
