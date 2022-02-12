#!/usr/bin/env perl

# 05-table-options.pl
#
# Check table-level options (work-in-progress)

use strict;
use warnings;

use Test::More tests => 3;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# Prepare table
# --------------

my $estimated_row_count = 100;

my $table_name = $node->init_table(
    'updatable' => 'FALSE',
    'estimated_row_count' => $estimated_row_count,
);


# 1. Check inserts not allowed
# ----------------------------

my $insert_q = sprintf(
    q|INSERT INTO %s (lang_id, name_english, name_native) VALUES('en', 'English', 'English')|,
    $table_name,
);

my ($insert_res, $insert_stdout, $insert_stderr) = $node->psql(
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

my ($explain_res, $explain_stdout, $explain_stderr) = $node->psql(
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


# 3. Check "table_name" and "column_name" options
# -----------------------------------------------

my $column_name_table = sprintf(
    q|%s_colcheck|,
    $table_name,
);

my $column_name_table_q = sprintf(
    <<'EO_SQL',
CREATE FOREIGN TABLE %s (
  pg_lang_id CHAR(2) OPTIONS (column_name 'lang_id'),
  pg_name_english VARCHAR(64) OPTIONS (column_name 'name_english'),
  pg_name_native VARCHAR(64) OPTIONS (column_name 'name_native')
)
SERVER %s
OPTIONS(
   table_name '%s'
)
EO_SQL
    $column_name_table,
    $node->server_name(),
    $table_name,
);

$node->safe_psql( $column_name_table_q );

my $q3_insert_q = sprintf(
    q|INSERT INTO %s (pg_lang_id, pg_name_english, pg_name_native) VALUES('de', 'German', 'Deutsch')|,
    $column_name_table,
);

$node->safe_psql( $q3_insert_q );

# Check it arrives

my $q3_check_query = sprintf(
    q|SELECT lang_id, name_english, name_native FROM %s WHERE lang_id = 'de'|,
    $table_name,
);

my $query = $node->firebird_conn()->prepare($q3_check_query);

$query->execute();

my @q3_res = $query->fetchrow_array();

my $q3_res = join('|', @q3_res);

$query->finish();

is(
	$q3_res,
	'de|German|Deutsch',
	'table_name/column_name options OK',
);




# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
