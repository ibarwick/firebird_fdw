#!/usr/bin/env perl

# 09-identifier-quoting.pl
#
# Check identifier quoting options

use strict;
use warnings;

use Cwd;
use Config;
use TestLib;
use Test::More tests => 1;

use FirebirdFDWNode;
use FirebirdFDWDB;


# Initialize nodes
# ----------------

my $pg_node = get_new_fdw_node('pg_node');

$pg_node->init();
$pg_node->start();

my $pg_db = FirebirdFDWDB->new($pg_node);

# 1. Check server option "quote_identifiers"
# ------------------------------------------

# Create Firebird table with quoted identifiers

$pg_db->add_server_option('quote_identifiers', 'true');

my $table_name = $pg_db->_make_table_name();

my $tbl_sql = sprintf(
    <<EO_SQL,
CREATE TABLE "%s" (
   "col1" INT,
   "UClc" INT,
   unquoted INT,
   "lclc" INT
)
EO_SQL
    $table_name,
);

my $tbl_query = $pg_node->{dbh}->prepare($tbl_sql);
$tbl_query->execute();
$tbl_query->finish();

my $ftbl_sql = sprintf(
    <<EO_SQL,
CREATE FOREIGN TABLE "%s" (
   "col1" INT,
   "UClc" INT,
   unquoted INT OPTIONS(quote_identifier 'false'),
   lclc INT
)
SERVER %s
OPTIONS (table_name '%s')
EO_SQL
    $table_name,
    $pg_db->{server_name},
    $table_name,
);

$pg_db->safe_psql($ftbl_sql);

my $insert_q1 = sprintf(
    <<EO_SQL,
  INSERT INTO "%s" VALUES(1, 2, 3, 4) RETURNING "col1", "UClc", unquoted, lclc
EO_SQL
    $table_name,
);


my $res = $pg_db->safe_psql($insert_q1);


is(
	$res,
	'1|2|3|4',
	'Default options OK',
);


$pg_node->drop_table($table_name, 1);
