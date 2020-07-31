#!/usr/bin/env perl

# 10-generated-columns.pl
#
# Check support for generated columns

use strict;
use warnings;

use Cwd;
use Config;
use TestLib;
use Test::More tests => 2;

use FirebirdFDWNode;
use FirebirdFDWDB;



# Initialize nodes
# ----------------

my $pg_node = get_new_fdw_node('pg_node');

$pg_node->init();
$pg_node->start();

my $pg_db = FirebirdFDWDB->new($pg_node);

# Create tables
# -------------

my $table_name = $pg_db->_make_table_name();

my $tbl_sql = sprintf(
    <<EO_SQL,
CREATE TABLE %s (
   a INT,
   b INT
)
EO_SQL
    $table_name,
);

my $tbl_query = $pg_node->{dbh}->prepare($tbl_sql);
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
    $pg_db->{server_name},
    $table_name,
);

$pg_db->safe_psql($ftbl_sql);

# 1. Check INSERT
# ---------------

my $insert_q1 = sprintf(
    <<EO_SQL,
  INSERT INTO %s VALUES(1), (2)
EO_SQL
    $table_name,
);

$pg_db->safe_psql($insert_q1);

my $select_q1 = sprintf(
    q|SELECT * FROM %s ORDER BY a|,
    $table_name,
);

my $q1 = $pg_node->{dbh}->prepare($select_q1);

$q1->execute();

my $res1 = $pg_node->firebird_format_results($q1);

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

$pg_db->safe_psql($update_q2);

my $select_q2 = sprintf(
    q|SELECT * FROM %s ORDER BY a|,
    $table_name,
);

my $q2 = $pg_node->{dbh}->prepare($select_q2);

$q2->execute();

my $res2 = $pg_node->firebird_format_results($q2);

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

$pg_node->drop_table($table_name);
