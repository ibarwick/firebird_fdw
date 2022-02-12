#!/usr/bin/env perl

# 17-scans.pl
#
# Check that foreign table scans work as expected


use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

our $node = FirebirdFDWNode->new();

our $version = $node->pg_version();

plan tests => 1;

# Ensure rescans work properly
# -----------------------------
#
# This test reproduces the situation described in this issue:
#
#    https://github.com/ibarwick/firebird_fdw/issues/21
#
# where the provided query - if executed without analyzing the
# foreign table - resulted in a merge join, which triggered an
# error in the table rescan implementation, leading to incorrect
# results.

# Create a table with some random hierachical-esque data; this
# can be used to craft a query which should cause a rescan to
# occur:

my $q1_table_name = $node->make_table_name();

my $q1_fb_sql = sprintf(
    <<'EO_SQL',
CREATE TABLE %s (
  c0 INT NOT NULL,
  c1 INT NOT NULL
)
EO_SQL
    $q1_table_name,
);

$node->firebird_execute_sql($q1_fb_sql);

my $q1_pg_sql  = sprintf(
    <<'EO_SQL',
CREATE FOREIGN TABLE %s (
  c0 INT NOT NULL,
  c1 INT NOT NULL
)
SERVER %s
OPTIONS ( table_name '%s' );
EO_SQL
    $q1_table_name,
    $node->server_name(),
    $q1_table_name,
);

$node->safe_psql( $q1_pg_sql );

my $q1_data_sql = sprintf(
    <<'EO_SQL',
WITH y AS (
  WITH x AS (
    SELECT x.id
      FROM (SELECT pg_catalog.generate_series(1,20000, (pg_catalog.random() * 10)::int + 1) id) x
     LIMIT 1500
  )
  SELECT x.id AS c0,
        (SELECT y.id
           FROM x y
          WHERE y.id < x.id
       ORDER BY pg_catalog.random()
          LIMIT 1
        ) AS c1
    FROM x
)
INSERT INTO %s (c0, c1)
     SELECT y.c0, y.c1
       FROM y
      WHERE y.c1 IS NOT NULL
EO_SQL
    $q1_table_name,
);
$node->safe_psql( $q1_data_sql );

# Fetch a "seed" ID for the query

my $q1_seed_id_sql = sprintf(
    q|SELECT c1 FROM %s WHERE c1 > 1 ORDER BY 1 OFFSET 5 LIMIT 1|,
    $q1_table_name,
);

my ($q1_res, $q1_stdout, $q1_stderr) = $node->psql(
    $q1_seed_id_sql,
);

my $seed_id = $q1_stdout;

# Determine how many rows should be returned

my $fb_query_sql = sprintf(
    <<'EO_SQL',
SELECT COUNT(*) FROM
(
  WITH RECURSIVE r AS (
  SELECT p.c0
    FROM %s p
   WHERE p.c1 = %i
     UNION ALL
  SELECT p.c0
    FROM %s p
    JOIN r ON p.c1 = r.c0
)
SELECT * FROM r)
EO_SQL
    $q1_table_name,
    $seed_id,
    $q1_table_name,
);

my $fb_query = $node->firebird_conn()->prepare($fb_query_sql);

$fb_query->execute();

my $expected_count = $fb_query->fetchrow_array();

# We want the following query to use a merge join

$node->safe_psql( q|SET enable_hashjoin = 'off'| );

my $pg_query_sql = sprintf(
    <<'EO_SQL',
WITH RECURSIVE r AS (
  SELECT p.c0
    FROM %s p
   WHERE p.c1 = %i
     UNION ALL
  SELECT p.c0
    FROM %s p
    JOIN r ON p.c1 = r.c0
)
SELECT count(*) FROM r
EO_SQL
    $q1_table_name,
    $seed_id,
    $q1_table_name,
);

my ($count_res, $count_stdout, $count_stderr) = $node->psql(
    $pg_query_sql,
);

is (
    $count_stdout,
    $expected_count,
    q|Check query results match|,
);

# Clean up
# --------

$node->drop_foreign_server();

$node->firebird_drop_table($q1_table_name);

done_testing();
