#!/usr/bin/env perl

# 12-misc.pl
#
# Checks for miscellaneous items which don't fit elsewhere

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

my $version = $node->pg_version();

my $tests = 0;

if ($version > 90500) {
    # INSERT ... ON CONFLICT
    $tests ++;
}

if (!$tests) {
    plan skip_all => q|all test(s) skipped|;
}
else {
    plan tests => $tests;
}

# 1. Check INSERT ... ON CONFLICT
# -------------------------------
if ($version > 90500) {
    my $table_q1 = $node->init_table();

    my $insert_q1 = sprintf(
        <<'EO_SQL',
    INSERT INTO %s
                (lang_id, name_english, name_native)
         VALUES ('en', 'English', 'English')
    ON CONFLICT DO NOTHING
EO_SQL
        $table_q1,
    );

    my ($insert_q1_res, $insert_q1_stdout, $insert_q1_stderr) = $node->psql(
        $insert_q1,
    );

    my $expected_q1_stderr = q|INSERT with ON CONFLICT clause is not supported|;

    like (
        $insert_q1_stderr,
        qr/$expected_q1_stderr/,
        q|Check INSERT with ON CONFLICT clause fails|,
    );
}
