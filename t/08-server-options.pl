#!/usr/bin/env perl

# 08-server-options.pl
#
# Check server-level options (work-in-progress)
#
# Option "quote_identifiers" covered by 09-identifier-quoting.pl

use strict;
use warnings;

use Test::More tests => 5;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();


# 1. Check options as reported by "firebird_fdw_server_options()"
# ---------------------------------------------------------------

# Record order is stable so no need for ORDER BY
my $options_q1 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s')|,
	$node->{server_name},
);

my $options_e1 = sprintf(
	<<EO_TXT,
address|localhost|t
port|3050|t
database|%s|t
updatable|true|t
quote_identifiers|false|t
implicit_bool_type|true|t
disable_pushdowns|false|t
EO_TXT
	$node->{firebird_dbname},
);

chomp($options_e1);

my ($res, $res_stdout, $res_stderr) = $node->psql($options_q1);

is(
	$res_stdout,
	$options_e1,
	'Default options OK',
);

# 2. Set "updatable" to "false"
# -----------------------------

$node->alter_server_option('updatable', 'false');

my $options_q2 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s') WHERE name = 'updatable'|,
	$node->{server_name},
);

my $options_e2 = q/updatable|false|t/;

($res, $res_stdout, $res_stderr) = $node->psql($options_q2);

is(
	$res_stdout,
	$options_e2,
	q|Disable "updatable" option|,
);

# 3. Verify table cannot be updated
# ---------------------------------

my $table_name_3 = $node->init_table();
my $options_q3 = sprintf(
	q|INSERT INTO %s VALUES('xx','yy','zz')|,
	$table_name_3,
);

my ($insert_res, $insert_stdout, $insert_stderr) = $node->psql(
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

$node->firebird_drop_table($table_name_3);

# 4. Create table with updatable=true, verify can be updated
# ----------------------------------------------------------

my $table_name_4 = $node->init_table(
	'updatable' => 'true',
);

my $options_q4 = sprintf(
	q|INSERT INTO %s VALUES('xx','yy','zz')|,
	$table_name_4,
);

$node->safe_psql( $options_q4 );


my $check_query_q4 = sprintf(
    q|SELECT lang_id, name_english, name_native FROM %s WHERE lang_id = 'xx'|,
    $table_name_4,
);

my $query_q4 = $node->firebird_conn()->prepare($check_query_q4);

$query_q4->execute();

my @res_q4 = $query_q4->fetchrow_array();

my $res_q4 = join('|', @res_q4);

$query_q4->finish();

is(
	$res_q4,
	'xx|yy|zz',
	q|table option "updatable" overrides server-level option|,
);


$node->firebird_drop_table($table_name_4);

# 5. drop updateable option
# -------------------------

$node->drop_server_option('updatable');

my $options_q5 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s') WHERE name = 'updatable'|,
	$node->{server_name},
);

my $options_e5 = q/updatable|true|f/;

($res, $res_stdout, $res_stderr) = $node->psql($options_q5);

is(
	$res_stdout,
	$options_e5,
	q|Drop "updatable" option|,
);


# Clean up
# --------

$node->drop_foreign_server();

done_testing();
