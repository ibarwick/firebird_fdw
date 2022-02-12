#!/usr/bin/env perl

# 07-query-tables.pl
#
# Test foreign tables created as queries

use strict;
use warnings;

use Test::More tests => 2;

use FirebirdFDWNode;


# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# Prepare table
# --------------

my $table_name = $node->init_table(firebird_only => 1);

my $query_table_name = sprintf(q|%s_query|, $table_name);

# 1) Test basic functionality
# ---------------------------

my $create_q1 = sprintf(
    <<'EO_SQL',
CREATE FOREIGN TABLE %s (
  lang_id CHAR(2),
  name_english VARCHAR(64),
  name_native VARCHAR(64)
)
SERVER %s
OPTIONS(
   query $$SELECT lang_id, name_english, name_native FROM %s$$
)
EO_SQL
    $query_table_name,
    $node->server_name(),
    $table_name,
);

$node->safe_psql( $create_q1 );

my $insert_q1 = sprintf(
    q|INSERT INTO %s (lang_id, name_english, name_native) VALUES('en', 'English', 'English')|,
    $table_name,
);

my $fb_q1= $node->firebird_conn()->prepare($insert_q1);

$fb_q1->execute();

$fb_q1->finish();

my $select_q1 = sprintf(
    q|SELECT lang_id, name_english, name_native FROM %s WHERE lang_id = 'en'|,
    $query_table_name,
);

my ($res, $res_stdout, $res_stderr) = $node->psql( $select_q1 );


is(
	$res_stdout,
	'en|English|English',
	'query OK',
);

# 2) Verify that insert operations fail
# -------------------------------------

my $insert_q2 = sprintf(
    q|INSERT INTO %s (lang_id, name_english, name_native) VALUES('de', 'German', 'Deutsch')|,
    $query_table_name,
);

my ($insert_q2_res, $insert_q2_stdout, $insert_q2_stderr) = $node->psql(
    $insert_q2,
);

my $expected_q2_stderr = q|unable to modify a foreign table defined as a query|;
like (
    $insert_q2_stderr,
    qr/$expected_q2_stderr/,
    q|Check INSERT on foreign table defined as query fails|,
);

# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
