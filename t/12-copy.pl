#!/usr/bin/env perl

# 12-copy.pl
#
# Check support for COPY

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


# 1. Check simple COPY
# --------------------

my $table_1 = $node->init_table();

my $copy_sql_1 = sprintf(
    <<'EO_SQL',
COPY %s FROM STDIN WITH (format 'csv');
xx,Xxxish,Xxxisch
zz,Zzzish,Zzzisch
\.
EO_SQL
    $table_1,
);

$node->safe_psql($copy_sql_1);

my $select_q1 = sprintf(
    q|SELECT * FROM %s ORDER BY 1|,
    $table_1,
);

my $q1 = $node->firebird_conn()->prepare($select_q1);

$q1->execute();

my $res1 = $node->firebird_format_results($q1);

$q1->finish();

my $expected1 = <<EO_TXT;
xx|Xxxish|Xxxisch
zz|Zzzish|Zzzisch
EO_TXT

chomp($expected1);

is(
	$res1,
	$expected1,
	'Basic COPY OK',
);

$node->firebird_drop_table($table_1);


# 2. Check copy into partitioned table
# =====================================

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

$node->safe_psql(
    sprintf(
        <<'EO_SQL',
COPY %s FROM STDIN (format 'csv');
101,"foo"
102,"bar"
201,"baz"
202,"qux"
\.
EO_SQL
        $parent_table_name,
    ),
);

my $select_q2_t1 = sprintf(
    q|SELECT * FROM %s ORDER BY 1|,
    $child_table_1,
);


my $q2_t1 = $node->firebird_conn()->prepare($select_q2_t1);

$q2_t1->execute();

my $res2_t1 = $node->firebird_format_results($q2_t1);

$q2_t1->finish();

my $expected2_t1 = <<EO_TXT;
101|foo
102|bar
EO_TXT

chomp($expected2_t1);

is(
	$res2_t1,
	$expected2_t1,
	'COPY into partitioned table OK (1)',
);



# 3. Check copy into partitioned table (2)
# ========================================

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

$node->firebird_drop_table($child_table_1);
$node->firebird_drop_table($child_table_2);

# Clean up
# --------

$node->drop_foreign_server();

done_testing();
