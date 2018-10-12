#!/usr/bin/env perl

# 04-data-types.pl
#
# Check various data types (work-in-progress)

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

# Prepare table
# --------------

my $table_name = $pg_db->init_data_table();

# 1) Test BLOB type
# -----------------

my $insert_sql = sprintf(
    <<EO_SQL,
INSERT INTO %s (id, blob_type)
         VALUES(1, 'foo\nbar\nテスト');
EO_SQL
    $table_name,
);

$pg_db->safe_psql($insert_sql);

my $q1_sql = sprintf(
    q|SELECT blob_type FROM %s WHERE id = 1|,
    $table_name,
);

my $q1 = $pg_node->{dbh}->prepare($q1_sql);

$q1->execute();

my $q1_res = $q1->fetchrow_array();

$q1->finish();

is (
    $q1_res,
    qq/foo\nbar\nテスト/,
    q|Check BLOB (subtype TEXT)|,
);



# Clean up
# --------

$pg_db->drop_foreign_server();
$pg_node->drop_table($table_name);

done_testing();
