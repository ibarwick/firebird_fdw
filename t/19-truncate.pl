#!/usr/bin/env perl

# 19-truncate.pl
#
# Check that truncate functionality work as expected (PostgreSQL 14 and later)

use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

our $node = FirebirdFDWNode->new();

our $version = $node->pg_version();

if ($version < 140000) {
    plan skip_all => sprintf(
        q|version is %i, tests for 14 and later|,
        $version,
    );
}


# Prepare table
# -------------

my $table_name = $node->init_table(
    definition_pg => [
        ['id',  'INT NOT NULL'],
        ['val', 'VARCHAR(32)'],
    ],
    definition_fb => [
        ['id',  'INT NOT NULL PRIMARY KEY'],
        ['val', 'VARCHAR(32)'],
    ],
);

# 1) check server-level truncatable = false
# -----------------------------------------

$node->alter_server_option('truncatable', 'false');

my $truncatable_q1 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s') WHERE name = 'truncatable'|,
	$node->{server_name},
);

my $truncatable_e1 = qq/truncatable|false|t/;

my ($res, $res_stdout, $res_stderr) = $node->psql($truncatable_q1);

is(
	$res_stdout,
	$truncatable_e1,
	qq|Set "truncatable" option to "false"|,
);


# 2) check behaviour when truncatable = false (server)
# ----------------------------------------------------

my $truncate_q2 = sprintf(
    q|TRUNCATE %s|,
    $table_name,
);

($res, $res_stdout, $res_stderr) = $node->psql($truncate_q2);

my $truncate_e2 = sprintf(
    q|foreign table "%s" does not allow truncates|,
    $table_name,
);

like(
	$res_stderr,
	qr|$truncate_e2|,
	qq|Check truncate fails when server "truncatable" option is "false"|,
);


# 3) check behaviour when truncatable = false (server) and true (table)
# ---------------------------------------------------------------------


$node->add_foreign_table_option(
    $table_name,
    'truncatable',
    'true',
);

my $truncate_q3 = sprintf(
    q|TRUNCATE %s|,
    $table_name,
);

($res, $res_stdout, $res_stderr) = $node->psql($truncate_q2);

is(
	$res,
	0,
	qq|Check truncate succeeds when "truncatable" option is "false" for server and "true" for table|,
);

# 4) check behaviour when truncatable = true (server) and false (table)
# ---------------------------------------------------------------------

my $truncate_q4 = sprintf(
    q|TRUNCATE %s|,
    $table_name,
);

$node->alter_server_option('truncatable', 'true');

$node->alter_foreign_table_option(
    $table_name,
    'truncatable',
    'false',
);

($res, $res_stdout, $res_stderr) = $node->psql($truncate_q4);

my $truncate_e4 = sprintf(
    q|foreign table "%s" does not allow truncates|,
    $table_name,
);

like(
	$res_stderr,
	qr|$truncate_e4|,
	qq|Check truncate fails when "truncatable" option is "true" (server) and "false" (table)|,
);

# Restore table's truncatable status

$node->alter_foreign_table_option(
    $table_name,
    'truncatable',
    'true',
);


# 5. Verify data removed
# ----------------------

# Insert some data; we'll assume the standard FDW functionality is working
# at this point, so no need to verify the Firebird side.

my $insert_sql_q5 = sprintf(
    <<'EO_SQL',
INSERT INTO %s
       (id, val)
VALUES (1, 'foo'),
       (2, 'bar'),
       (3, 'baz')
EO_SQL
    $table_name,
);

$node->safe_psql($insert_sql_q5);

my $truncate_q5 = sprintf(
    q|TRUNCATE %s|,
    $table_name,
);

$node->safe_psql($truncate_q5);

my $rows_q5 = sprintf(
    q|SELECT COUNT(*) FROM %s|,
    $table_name,
);

($res, $res_stdout, $res_stderr) = $node->psql($rows_q5);

is(
	$res_stdout,
	q|0|,
	qq|Check table contains 0 rows after INSERT and TRUNCATE operation|,
);

# 6. Verify TRUNCATE ... CASCADE is rejected
# ------------------------------------------

my $truncate_q6 = sprintf(
    q|TRUNCATE %s CASCADE|,
    $table_name,
);

($res, $res_stdout, $res_stderr) = $node->psql($truncate_q6);

like(
	$res_stderr,
	qr|TRUNCATE with CASCADE option not supported by firebird_fdw|,
	qq|Check truncate fails when TRUNCATE ... CASCADE is provided|,
);


# 7. Verify TRUNCATE ... RESTART IDENTITY is rejected
# ---------------------------------------------------

my $truncate_q7 = sprintf(
    q|TRUNCATE %s RESTART IDENTITY|,
    $table_name,
);

($res, $res_stdout, $res_stderr) = $node->psql($truncate_q7);

like(
	$res_stderr,
	qr|TRUNCATE with RESTART IDENTITY option not supported by firebird_fdw|,
	qq|Check truncate fails when TRUNCATE ... CASCADE is provided|,
);

# 8. Verify TRUNCATE rejected for Firebird tables with foreign key references
# ---------------------------------------------------------------------------

my $fkey_table = $node->init_table(
    firebird_only => 1,
    definition_fb => [
        ['id',  sprintf('INT NOT NULL REFERENCES %s (id)', $table_name)],
    ],
);

my $truncate_q8 = sprintf(
    q|TRUNCATE %s|,
    $table_name,
);

($res, $res_stdout, $res_stderr) = $node->psql($truncate_q8);

my $truncate_e8 = sprintf(q|foreign table "%s" has foreign key references|, $table_name);

like(
	$res_stderr,
	qr|$truncate_e8|,
	qq|Check truncate fails when TRUNCATE ... CASCADE is provided|,
);

# Clean up
# --------

$node->drop_foreign_server();

$node->firebird_drop_table($fkey_table);
$node->firebird_drop_table($table_name);

done_testing();
