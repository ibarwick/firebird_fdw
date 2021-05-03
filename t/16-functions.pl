#!/usr/bin/env perl

# 16-functions.pl
#
# Checks for functions provided by firebird_fdw

use strict;
use warnings;

use TestLib;
use Test::More tests => 2;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

my ($version, $version_int) = $node->get_firebird_version();

# 1. Check firebird_version() output
# ----------------------------------

my $version_expected = sprintf(
    q/%s|%i|%s/,
    $node->server_name(),
    $version_int,
    $version,
);


my $q1_sql = q|SELECT * FROM firebird_version()|;
my ($q1_res, $q1_stdout, $q1_stderr) = $node->psql($q1_sql);

is (
    $q1_stdout,
    $version_expected,
    q|Check firebird_version() output|,
);



# 2. Check firebird_version() output as non-superuser
# ---------------------------------------------------

$node->psql(q|CREATE USER foo|);

my $q2_sql = q|SET session AUTHORIZATION foo; SELECT * FROM firebird_version()|;
my ($q2_res, $q2_stdout, $q2_stderr) = $node->psql($q2_sql);

is (
    $q2_stdout,
    $version_expected,
    q|Check firebird_version() output with non-superuser|,
);


