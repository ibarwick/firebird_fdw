#!/usr/bin/env perl

# 09-identifier-quoting.pl
#
# Check identifier quoting options

use strict;
use warnings;

use TestLib;
use Test::More tests => 1;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# 1. Check server option "quote_identifiers"
# ------------------------------------------

# Create Firebird table with quoted identifiers

$node->add_server_option('quote_identifiers', 'true');

my $table_name = $node->make_table_name();

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

my $tbl_query = $node->firebird_conn()->prepare($tbl_sql);
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
    $node->{server_name},
    $table_name,
);

$node->safe_psql($ftbl_sql);

my $insert_q1 = sprintf(
    <<EO_SQL,
  INSERT INTO "%s" VALUES(1, 2, 3, 4) RETURNING "col1", "UClc", unquoted, lclc
EO_SQL
    $table_name,
);


my $res = $node->safe_psql($insert_q1);


is(
	$res,
	'1|2|3|4',
	'Default options OK',
);


$node->firebird_drop_table($table_name, 1);
done_testing();
