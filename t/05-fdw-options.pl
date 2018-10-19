#!/usr/bin/env perl

# 05-fdw-options.pl
#
# Check various server/table options (work-in-progress)

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

my $table_name = $pg_db->init_table(
    'updatable' => 'FALSE',
);

my $insert_q = sprintf(
    q|INSERT INTO %s (lang_id, name_english, name_native) VALUES('en', 'English', 'English')|,
    $table_name,
);

my ($insert_res, $insert_stdout, $insert_stderr) = $pg_db->psql(
    $insert_q,
);

my $expected = sprintf(
    q|foreign table "%s" does not allow inserts|,
    $table_name,
);

like (
    $insert_stderr,
    qr/$expected/,
    q|Check INSERT|,
);


# Clean up
# --------

$pg_db->drop_foreign_server();
$pg_node->drop_table($table_name);

done_testing();
