#!/usr/bin/env perl

# 04-data-types.pl
#
# Check various data types (work-in-progress)

use strict;
use warnings;

use Cwd;
use Config;
use TestLib;
use Test::More;

use FirebirdFDWNode;
use FirebirdFDWDB;


# Initialize nodes
# ----------------

my $pg_node = get_new_fdw_node('pg_node');

$pg_node->init();
$pg_node->start();

my $pg_db = FirebirdFDWDB->new($pg_node);

# Check Firebird version
# ----------------------

if ($pg_node->{firebird_major_version} >= 3) {
	plan tests => 2;
}
else {
	plan tests => 1;
}

# Prepare table
# --------------

my $table_name = $pg_db->init_data_table();

# 1) Test BLOB type
# -----------------

my $blob_insert_sql = sprintf(
    <<EO_SQL,
INSERT INTO %s (id, blob_type)
         VALUES(1, 'foo\nbar\nテスト');
EO_SQL
    $table_name,
);

$pg_db->safe_psql($blob_insert_sql);

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


if ($pg_node->{firebird_major_version} >= 3) {

	# 2) Test BOOLEAN type
	# ---------------------

	my $bool_insert_sql = sprintf(
		<<EO_SQL,
INSERT INTO %s (id, bool_type)
         VALUES(2, TRUE), (3, FALSE);
EO_SQL
		$table_name,
	);

	$pg_db->safe_psql($bool_insert_sql);

	my $q2_sql = sprintf(
		q|SELECT id, bool_type FROM %s WHERE id IN (2,3) ORDER BY id|,
		$table_name,
	);

	my $q2_res = $pg_db->safe_psql($q2_sql);

	is (
		$q2_res,
		qq/2|t\n3|f/,
		q|Check BOOLEAN|,
	);
}


# Clean up
# --------

$pg_db->drop_foreign_server();
$pg_node->drop_table($table_name);

done_testing();
