#!/usr/bin/env perl

# 01-extension.pl
#
# Basic sanity check to ensure the extension is installed
#
# Creates a single table on the Firebird server, a corresponding
# foreign table in PostgreSQL and performs some simple DML

use strict;
use warnings;

use TestLib;
use Test::More tests => 4;

use FirebirdFDWNode;


# Initialize PostgreSQL node
# --------------------------

my $node = FirebirdFDWNode->new();


# 1. Check version
# ----------------
#
# TODO: parse the value from "firebird_fdw.control" and check for a match

my $version = '10201';

my $res = $node->safe_psql(q|SELECT firebird_fdw_version()|);

is(
	$res,
	$version,
	'version OK',
);



# Prepare table
# --------------

my $table_name = $node->init_table();

# 2. Insert a row
# ---------------

my $insert_q = sprintf(
    q|INSERT INTO %s (lang_id, name_english, name_native) VALUES('en', 'English', 'English')|,
    $table_name,
);

$node->safe_psql( $insert_q );

# Check it arrives

my $queryText = sprintf(
    q|SELECT name_english FROM %s WHERE lang_id = 'en'|,
    $table_name,
);

my $query = $node->firebird_conn()->prepare($queryText);

$query->execute();

$res = $query->fetchrow_array();

$query->finish();

is(
	$res,
	'English',
	'insert OK',
);


# 3. Update the row
# -----------------

my $update_q = sprintf(
    q|UPDATE %s SET name_native = 'Wibblish' WHERE lang_id = 'en'|,
    $table_name,
);

$node->safe_psql( $update_q );

$queryText = sprintf(
    q|SELECT name_native FROM %s WHERE lang_id = 'en'|,
    $table_name,
);

$query = $node->firebird_conn()->prepare($queryText);

$query->execute();

$res = $query->fetchrow_array();

$query->finish();

is(
	$res,
	'Wibblish',
	'update OK',
);


# 4. Delete the row
# -----------------

my $delete_q = sprintf(
    q|DELETE FROM %s WHERE lang_id = 'en'|,
    $table_name,
);

$node->safe_psql( $delete_q );

$queryText = sprintf(
    q|SELECT COUNT(*) FROM %s WHERE lang_id = 'en'|,
    $table_name,
);

$query = $node->firebird_conn()->prepare($queryText);

$query->execute();

$res = $query->fetchrow_array();

$query->finish();

is(
	$res,
	'0',
	'delete OK',
);


# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
