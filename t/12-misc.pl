#!/usr/bin/env perl

# 12-misc.pl
#
# Checks for miscellaneous items which don't fit elsewhere

use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

my $version = $node->pg_version();

my $tests = 0;

my @on_conflict_tests = (
    [
        <<'EO_SQL',
    INSERT INTO %s
                (lang_id, name_english, name_native)
         VALUES ('en', 'English', 'English')
    ON CONFLICT DO NOTHING
EO_SQL
        q|INSERT with ON CONFLICT clause is not supported|,
        q|Check INSERT with ON CONFLICT DO NOTHING clause fails|,
    ],
    [
        <<'EO_SQL',
    INSERT INTO %s
                (lang_id, name_english, name_native)
         VALUES ('en', 'English', 'English')
    ON CONFLICT (lang_id) DO NOTHING
EO_SQL
        q|there is no unique or exclusion constraint matching the ON CONFLICT specification|,
        q|Check INSERT with ON CONFLICT (col) DO NOTHING clause fails|,
    ],
    [
        <<'EO_SQL',
    INSERT INTO %s
                (lang_id, name_english, name_native)
         VALUES ('en', 'English', 'English')
    ON CONFLICT (lang_id) DO UPDATE SET name_english = EXCLUDED.name_english
EO_SQL
        q|there is no unique or exclusion constraint matching the ON CONFLICT specification|,
        q|Check INSERT with ON CONFLICT (col) DO NOTHING clause fails|,
    ],
);


if ($version > 90500) {
    # INSERT ... ON CONFLICT
    $tests += scalar @on_conflict_tests;
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
    my $table_name = $node->init_table();

    foreach my $on_conflict_test (@on_conflict_tests) {
        my $insert = sprintf(
            $on_conflict_test->[0],
            $table_name,
        );

        my ($insert_res, $insert_stdout, $insert_stderr) = $node->psql(
            $insert,
        );

        like (
            $insert_stderr,
            qr/$on_conflict_test->[1]/,
            $on_conflict_test->[2],
        );
    }
}
