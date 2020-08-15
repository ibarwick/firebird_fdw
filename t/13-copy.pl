#!/usr/bin/env perl

# 13-copy.pl
#
# Check support for COPY
#
# See also 14-tuple-routing.pl for COPY support with tuple routing

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

my $version = $node->pg_version();

if ($version < 110000) {
    plan skip_all => sprintf(
        q|version is %i, tests for 11 and later|,
        $version,
    );
}
else {
    plan tests => 1;
}


# 1. Check simple COPY
# --------------------

my $table_1 = $node->init_table();

my $copy_sql_1 = sprintf(
    <<'EO_SQL',
COPY %s FROM STDIN WITH (format 'csv');
xx,Xxxish,Xxxisch
zz,Zzzish,Zzzisch
\.
EO_SQL
    $table_1,
);

$node->safe_psql($copy_sql_1);

my $select_q1 = sprintf(
    q|SELECT * FROM %s ORDER BY 1|,
    $table_1,
);

my $q1 = $node->firebird_conn()->prepare($select_q1);

$q1->execute();

my $res1 = $node->firebird_format_results($q1);

$q1->finish();

my $expected1 = <<EO_TXT;
xx|Xxxish|Xxxisch
zz|Zzzish|Zzzisch
EO_TXT

chomp($expected1);

is(
	$res1,
	$expected1,
	'Basic COPY OK',
);

$node->firebird_drop_table($table_1);


# Clean up
# --------

$node->drop_foreign_server();

done_testing();
