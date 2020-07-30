#!/usr/bin/env perl

# 08-server-options.pl
#
# Check server-level options (work-in-progress)
#
# Option "quote_identifiers" covered by 09-identifier-quoting.pl

use strict;
use warnings;

use Cwd;
use Config;
use TestLib;
use Test::More tests => 5;

use FirebirdFDWNode;
use FirebirdFDWDB;


# Initialize nodes
# ----------------

my $pg_node = get_new_fdw_node('pg_node');

$pg_node->init();
$pg_node->start();

my $pg_db = FirebirdFDWDB->new($pg_node);

# 1. Check options as reported by "firebird_fdw_server_options()"
# ---------------------------------------------------------------

# Record order is stable so no need for ORDER BY
my $options_q1 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s')|,
	$pg_db->{server_name},
);

my $options_e1 = sprintf(
	<<EO_TXT,
address|localhost|t
port|3050|t
database|%s|t
disable_pushdowns|false|t
updatable|true|t
EO_TXT
	$pg_db->{pg_fdw_node}->{firebird_dbname},
);

chomp($options_e1);

my $res = $pg_db->safe_psql($options_q1);

is(
	$res,
	$options_e1,
	'Default options OK',
);

# 2. Set "updatable" to "false"
# -----------------------------

$pg_db->alter_server_option('updatable', 'false');

my $options_q2 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s') WHERE name = 'updatable'|,
	$pg_db->{server_name},
);

my $options_e2 = q/updatable|false|t/;

$res = $pg_db->safe_psql($options_q2);

is(
	$res,
	$options_e2,
	q|Disable "updatable" option|,
);

# 3. Verify table cannot be updated
# ---------------------------------

my $table_name_3 = $pg_db->init_table();
my $options_q3 = sprintf(
	q|INSERT INTO %s VALUES('xx','yy','zz')|,
	$table_name_3,
);

my ($insert_res, $insert_stdout, $insert_stderr) = $pg_db->psql(
    $options_q3,
);

my $options_e3 = sprintf(
	q|foreign table "%s" does not allow inserts|,
	$table_name_3,
);

like(
	$insert_stderr,
	qr/$options_e3/,
	q|Check table cannot be inserted into|,
);

$pg_node->drop_table($table_name_3);

# 4. Create table with updatable=true, verify can be updated
# ----------------------------------------------------------

my $table_name_4 = $pg_db->init_table(
	'updatable' => 'true',
);

my $options_q4 = sprintf(
	q|INSERT INTO %s VALUES('xx','yy','zz')|,
	$table_name_4,
);

$pg_db->safe_psql( $options_q4 );


my $check_query_q4 = sprintf(
    q|SELECT lang_id, name_english, name_native FROM %s WHERE lang_id = 'xx'|,
    $table_name_4,
);

my $query_q4 = $pg_node->{dbh}->prepare($check_query_q4);

$query_q4->execute();

my @res_q4 = $query_q4->fetchrow_array();

my $res_q4 = join('|', @res_q4);

$query_q4->finish();

is(
	$res_q4,
	'xx|yy|zz',
	q|table option "updatable" overrides server-level option|,
);


$pg_node->drop_table($table_name_4);

# 5. drop updateable option
# -------------------------

$pg_db->drop_server_option('updatable');

my $options_q5 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s') WHERE name = 'updatable'|,
	$pg_db->{server_name},
);

my $options_e5 = q/updatable|true|f/;

$res = $pg_db->safe_psql($options_q5);

is(
	$res,
	$options_e5,
	q|Drop "updatable" option|,
);


# Clean up
# --------

$pg_db->drop_foreign_server();

done_testing();
