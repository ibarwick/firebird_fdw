#!/usr/bin/env perl

# 05-fdw-options.pl
#
# Check various server/table options (work-in-progress)

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

# Prepare table
# --------------

my $estimated_row_count = 100;

my $table_name = $pg_db->init_table(
    'updatable' => 'FALSE',
    'estimated_row_count' => $estimated_row_count,
);


# 1. Check inserts not allowed
# ----------------------------

my $insert_q = sprintf(
    q|INSERT INTO %s (lang_id, name_english, name_native) VALUES('en', 'English', 'English')|,
    $table_name,
);

my ($insert_res, $insert_stdout, $insert_stderr) = $pg_db->psql(
    $insert_q,
);

my $insert_expected = sprintf(
    q|foreign table "%s" does not allow inserts|,
    $table_name,
);

like (
    $insert_stderr,
    qr/$insert_expected/,
    q|Check INSERT|,
);


# 2. Check "estimated_row_count" is used
# --------------------------------------

# Foreign Scan on qtest  (cost=10.00..110.00 rows=100 width=294)

my $explain_q = sprintf(
    q|EXPLAIN SELECT * FROM %s|,
    $table_name,
);

my ($explain_res, $explain_stdout, $explain_stderr) = $pg_db->psql(
    $explain_q,
);

my $explain_expected = sprintf(
    q|rows=%i\b|,
    $estimated_row_count,
);

like (
    $explain_stdout,
    qr/$explain_expected/,
    q|Check "estimated_row_count" is used|,
);


# Clean up
# --------

$pg_db->drop_foreign_server();
$pg_node->drop_table($table_name);

done_testing();
