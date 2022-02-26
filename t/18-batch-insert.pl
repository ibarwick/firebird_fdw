#!/usr/bin/env perl

# 18-batch-insert.pl
#
# Check that batch inserts work as expected (PostgreSQL 14 and later)

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
else {
    plan tests => 5;
}


our $batch_size = 5;

# 1) Verify that "batch_size" setting works
# -----------------------------------------

$node->alter_server_option('batch_size', $batch_size);

my $batch_q1 = sprintf(
	q|SELECT * FROM firebird_fdw_server_options('%s') WHERE name = 'batch_size'|,
	$node->{server_name},
);

my $batch_e1 = qq/batch_size|${batch_size}|t/;

my ($res, $res_stdout, $res_stderr) = $node->psql($batch_q1);

is(
	$res_stdout,
	$batch_e1,
	qq|Set "batch_size" option to "${batch_size}"|,
);


# Prepare table
# --------------

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


my @tbl_data = ();
my @tbl_expected = ();


my $cur_val = 1;

for (my $i = 1; $i < 99; $i++) {
    push @tbl_data, sprintf(
        q|(%i, 'test_%i')|,
        $cur_val,
        $i,
    );

    push @tbl_expected,  sprintf(
        q/%i|test_%i/,
        $cur_val,
        $i,
    );

    $cur_val = $cur_val + int(rand(10)+1);
}


# 2. Verify that INSERT operations work
# -------------------------------------
#
# Here we assume that if the foreign table reports the values we just inserted,
# everything is running correctly. We could double-check on the Firebird side,
# but there seems little added benefit.

my $insert_q2 = sprintf(
    <<EO_SQL,
INSERT INTO %s
  (id, val)
VALUES
%s
EO_SQL
    $table_name,
    join(",\n", @tbl_data),
);

$node->safe_psql( $insert_q2 );

my $select_q2 = sprintf(
    q|SELECT * FROM %s ORDER BY id|,
    $table_name,
);

my $q2_expected = join("\n", @tbl_expected);

($res, $res_stdout, $res_stderr) = $node->psql($select_q2);

is(
    $res_stdout,
    $q2_expected,
    q|Check inserted values are retrieved|,
);

# 3. Verify reported batch size in EXPLAIN output
# -----------------------------------------------

$node->truncate_table($table_name);

my $explain_q3 = sprintf(
    <<EO_SQL,
EXPLAIN (VERBOSE, ANALYZE)
INSERT INTO %s
  (id, val)
VALUES
%s
EO_SQL
    $table_name,
    join(",\n", @tbl_data),
);


($res, $res_stdout, $res_stderr) = $node->psql($explain_q3);


like(
    $res_stdout,
    qr/Batch Size: ${batch_size}/,
    q|Check batch size value reported in EXPLAIN|,
);

# 4. Verify behaviour with INSERT ... RETURNING
# ---------------------------------------------

$node->truncate_table($table_name);

my $explain_q4 = sprintf(
    <<EO_SQL,
EXPLAIN (VERBOSE, ANALYZE)
INSERT INTO %s
  (id, val)
VALUES
%s
RETURNING id, val
EO_SQL
    $table_name,
    join(",\n", @tbl_data),
);


($res, $res_stdout, $res_stderr) = $node->psql($explain_q4);


like(
    $res_stdout,
    qr/Batch Size: 1/,
    q|Check batch size value reported in EXPLAIN is 1 with INSERT ... RETURNING ...|,
);

# 5. Verify table-level batch_size setting
# ----------------------------------------

$node->truncate_table($table_name);

our $table_batch_size = 15;

$node->add_foreign_table_option(
    $table_name,
    'batch_size',
    $table_batch_size,
);

my $explain_q5 = sprintf(
    <<EO_SQL,
EXPLAIN (VERBOSE, ANALYZE)
INSERT INTO %s
  (id, val)
VALUES
%s
EO_SQL
    $table_name,
    join(",\n", @tbl_data),
);

($res, $res_stdout, $res_stderr) = $node->psql($explain_q5);

like(
    $res_stdout,
    qr/Batch Size: $table_batch_size/,
    qq|Check batch size value reported in EXPLAIN is matches table batch_size ${table_batch_size}|,
);
