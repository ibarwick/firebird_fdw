#!/usr/bin/env perl

# 07-query-tables.pl
#
# Test foreign tables created as queries

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

my $table_name = $pg_db->init_table();

my $query_table_name = sprintf(q|%s_query|, $table_name);

# 1) Test basic functionality
# ---------------------------

my $create_q = sprintf(
    <<'EO_SQL',
CREATE FOREIGN TABLE %s (
  lang_id CHAR(2),
  name_english VARCHAR(64),
  name_native VARCHAR(64)
)
SERVER fb_test
OPTIONS(
   query $$SELECT lang_id, name_english, name_native FROM %s$$
)
EO_SQL
    $query_table_name,
    $table_name,
);

$pg_db->safe_psql( $create_q );

my $insert_q = sprintf(
    q|INSERT INTO %s (lang_id, name_english, name_native) VALUES('en', 'English', 'English')|,
    $table_name,
);

$pg_db->safe_psql( $insert_q );

my $queryText = sprintf(
    q|SELECT lang_id, name_english, name_native FROM %s WHERE lang_id = 'en'|,
    $query_table_name,
);

my $res = $pg_db->safe_psql( $queryText );


is(
	$res,
	'en|English|English',
	'query OK',
);
