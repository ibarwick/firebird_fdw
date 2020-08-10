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

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# Check Firebird version
# ----------------------

if ($node->{firebird_major_version} >= 3) {
	plan tests => 2;
}
else {
	plan tests => 1;
}

# Prepare table
# --------------

my $table_name = $node->init_data_type_table();

# 1) Test BLOB type
# -----------------

my $blob_insert_sql = sprintf(
    <<EO_SQL,
INSERT INTO %s (id, blob_type)
         VALUES(1, 'foo\nbar\nテスト');
EO_SQL
    $table_name,
);

$node->safe_psql($blob_insert_sql);

my $q1_sql = sprintf(
    q|SELECT blob_type FROM %s WHERE id = 1|,
    $table_name,
);

my $q1 = $node->firebird_conn()->prepare($q1_sql);

$q1->execute();

my $q1_res = $q1->fetchrow_array();

$q1->finish();

is (
    $q1_res,
    qq/foo\nbar\nテスト/,
    q|Check BLOB (subtype TEXT)|,
);


if ($node->{firebird_major_version} >= 3) {

	# 2) Test BOOLEAN type
	# ---------------------

	my $bool_insert_sql = sprintf(
		<<EO_SQL,
INSERT INTO %s (id, bool_type)
         VALUES(2, TRUE), (3, FALSE), (4, NULL);
EO_SQL
		$table_name,
	);

	$node->safe_psql($bool_insert_sql);

	my $q2_sql = sprintf(
		q|SELECT id, bool_type FROM %s WHERE id IN (2,3,4) ORDER BY id|,
		$table_name,
	);

	my $q2_res = $node->safe_psql($q2_sql);

	is (
		$q2_res,
		qq/2|t\n3|f\n4|/,
		q|Check BOOLEAN|,
	);
}


# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
