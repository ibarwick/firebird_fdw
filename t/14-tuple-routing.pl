#!/usr/bin/env perl

# 14-tuple-routing.pl
#
# Check support for tuple routing

use strict;
use warnings;

use TestLib;
use Test::More;

use FirebirdFDWNode;

# Initialize nodes
# ----------------

our $node = FirebirdFDWNode->new();

our $version = $node->pg_version();

if ($version < 110000) {
    plan skip_all => sprintf(
        q|version is %i, tests for 11 and later|,
        $version,
    );
}
else {
    plan tests => 12;
}


# Create a table partitioned by RANGE for use by multiple tests
# -------------------------------------------------------------

our $parent_range_table_name = $node->make_table_name();
our $child_range_table_0 = sprintf(q|%s_child0|, $parent_range_table_name);
our $child_range_table_1 = sprintf(q|%s_child1|, $parent_range_table_name);
our $child_range_table_2 = sprintf(q|%s_child2|, $parent_range_table_name);

create_range_partition_table(
    $parent_range_table_name,
    $child_range_table_0,
    $child_range_table_1,
    $child_range_table_2,
);


# Create a table partitioned by LIST for use by multiple tests
# ------------------------------------------------------------

our $parent_list_table_name = $node->make_table_name();
our $child_list_table_0 = sprintf(q|%s_child0|, $parent_list_table_name);
our $child_list_table_1 = sprintf(q|%s_child1|, $parent_list_table_name);
our $child_list_table_2 = sprintf(q|%s_child2|, $parent_list_table_name);

create_list_partition_table(
    $parent_list_table_name,
    $child_list_table_0,
    $child_list_table_1,
    $child_list_table_2,
);



# 1. Check COPY into partitioned table via parent
# -----------------------------------------------

check_copy_via_parent();

# Current contents:
# 101|foo
# 102|bar

# 2. Check COPY directly into partition
# -------------------------------------

check_copy_into_partition();

# Current contents:
# 101|foo
# 102|bar
# 111|baz
# 112|qux


# 3. Check delete from parent partition
# -------------------------------------

check_delete_all_from_parent();

# Current contents:
# (none)

# 4. Check COPY into multiple partitions
# --------------------------------------

check_copy_into_multiple_partitions();

# Current contents:
# tbl_XXX_child1|3|baa
# tbl_XXX_child2|103|bee
# tbl_XXX_child3|203|boo

# 5. Check INSERT INTO ... RETURNING with table partitioned by RANGE
# ------------------------------------------------------------------

check_insert_into_returning_via_parent(
    $parent_range_table_name,
    204,
    'mooh',
);

# Current contents:
# 3|baa
# 103|bee
# 203|boo
# 204|mooh

# 6. Check UPDATE which attempts to move tuple from local to foreign
#    partition with table partitioned by RANGE
# ------------------------------------------------------------------

check_update_local_to_foreign_partition_fail(
    'range',
    $parent_range_table_name,
    'baa',
    105,
);


# 7. Check UPDATE which attempts to move tuple from foreign to local
#    partition with table partitioned by RANGE
# ------------------------------------------------------------------

check_update_foreign_to_local_partition_fail(
    'range',
    $parent_range_table_name,
    'bee', # currently in remote partition
    205,
);


# 8. Check INSERT INTO ... RETURNING with table partitioned by LIST
# ------------------------------------------------------------------

check_insert_into_returning_via_parent(
    $parent_list_table_name,
    1,
    'mooh',
);

# Current contents:
# 1|mooh

# 9. Check multiple INSERT INTO ... RETURNING with table partitioned by LIST
# ---------------------------------------------------------------------------

check_multiple_insert_into_returning_via_parent(
    $parent_list_table_name,
    [0, 'listqoo'],
    [1, 'listqox'],
    [2, 'listqoz'],
);

# Current contents:
# 1|mooh
# 0|listqoo
# 1|listqox
# 2|listqoz

# 10. Check UPDATE which attempts to move tuple from local to foreign
#     partition with table partitioned by LIST (invalid scenario)
# -------------------------------------------------------------------

check_update_local_to_foreign_partition_fail(
    'list',
    $parent_list_table_name,
    'listqoo', # currently in first local partition
    1,
);

# 11. Check UPDATE which attempts to move tuple from foreign to local
#     partition with table partitioned by LIST (invalid scenario)
# -------------------------------------------------------------------

check_update_foreign_to_local_partition_fail(
    'list',
    $parent_list_table_name,
    'listqox', # currently in remote partition
    2,
);

# 12. Test triggers
# -----------------

check_before_trigger();

# cleanup
# -------
cleanup();


# Check COPY into partitioned table via parent
# --------------------------------------------
#
# Check verifies Firebird table contents

sub check_copy_via_parent {
    $node->safe_psql(
        sprintf(
            <<'EO_SQL',
COPY %s FROM STDIN (format 'csv');
101,"foo"
102,"bar"
\.
EO_SQL
            $parent_range_table_name,
        ),
    );

    my $select_t1 = sprintf(
        q|SELECT * FROM %s ORDER BY 1|,
        $child_range_table_1,
    );

    my $q_t1 = $node->firebird_conn()->prepare($select_t1);

    $q_t1->execute();

    my $res1_t1 = $node->firebird_format_results($q_t1);

    $q_t1->finish();

    my $expected1_t1 = <<EO_TXT;
101|foo
102|bar
EO_TXT

    chomp($expected1_t1);

    is(
        $res1_t1,
        $expected1_t1,
        'COPY into partitioned table via parent OK',
    );
}

# Check COPY directly into partition
# ---------------------------------
#
# Check verifies Firebird table contents

sub check_copy_into_partition {
    $node->safe_psql(
        sprintf(
            <<'EO_SQL',
COPY %s FROM STDIN (format 'csv');
111,"baz"
112,"qux"
\.
EO_SQL
            $child_range_table_1,
        ),
    );

    my $select_t1 = sprintf(
        q|SELECT * FROM %s ORDER BY 1|,
        $child_range_table_1,
    );

    my $q_t1 = $node->firebird_conn()->prepare($select_t1);

    $q_t1->execute();

    my $res_t1 = $node->firebird_format_results($q_t1);

    $q_t1->finish();

    my $expected_t1 = <<EO_TXT;
101|foo
102|bar
111|baz
112|qux
EO_TXT

    chomp($expected_t1);

    is(
        $res_t1,
        $expected_t1,
        'direct COPY into partition OK',
    );
}


# Check delete from parent partition
# ----------------------------------

sub check_delete_all_from_parent {
    $node->safe_psql(
        sprintf(
            <<'EO_SQL',
DELETE FROM %s
EO_SQL
            $parent_range_table_name,
        ),
    );

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            q|SELECT tableoid::regclass, * FROM %s|,
            $parent_range_table_name,
        ),
    );

    is(
        $res_stdout,
        '',
        'Check delete from parent partition',
    );
}

# Check COPY into multiple partitions
# -----------------------------------

sub check_copy_into_multiple_partitions {

    $node->safe_psql(
        sprintf(
            <<'EO_SQL',
COPY %s FROM STDIN (format 'csv');
3,"baa"
103,"bee"
203,"boo"
\.
EO_SQL
            $parent_range_table_name,
        ),
    );

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            q|SELECT tableoid::regclass, * FROM %s|,
            $parent_range_table_name,
        ),
    );

    my $expected = sprintf(
        <<'EO_TXT',
%s|3|baa
%s|103|bee
%s|203|boo
EO_TXT
        $child_range_table_0,
        $child_range_table_1,
        $child_range_table_2,
    );

    chomp $expected;

    is(
        $res_stdout,
        $expected,
        'Check COPY into multiple partitions',
    );

}

# Check INSERT INTO ... RETURNING via parent table
# ------------------------------------------------

sub check_insert_into_returning_via_parent {
    my $parent_table_name = shift;
    my $intval = shift;
    my $strval = shift;

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            q|INSERT INTO %s VALUES (%i, '%s') RETURNING *|,
            $parent_table_name,
            $intval,
            $strval,
        ),
    );

    my $expected = sprintf(
        q/%i|%s/,
        $intval,
        $strval,
    );

    is(
        $res_stdout,
        $expected,
        'Check INSERT INTO ... RETURNING with table partitioned by range',
    );
}



# Check INSERT INTO ... RETURNING via parent table with multiple rows
# -------------------------------------------------------------------

sub check_multiple_insert_into_returning_via_parent {
    my $parent_table_name = shift;
    my @items = @_;

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            <<'EO_SQL',
INSERT INTO %s VALUES
       %s
       RETURNING *
EO_SQL
            $parent_table_name,
            join(",\n", map { qq|($_->[0], '$_->[1]')| } @items),
        ),
    );

    my @expected = ();
    foreach my $item (@items) {
        push @expected, sprintf(
            q/%i|%s/,
            $item->[0],
            $item->[1],
        );
    }

    is(
        $res_stdout,
        join("\n", @expected),
        'Check multiple INSERT INTO ... RETURNING with table partitioned by range',
    );
}


# Check UPDATE which attempts to move tuple from local to foreign
# partition when local partition is "before" the foreign one
# ---------------------------------------------------------------
#
# On a table partitioned by list/range (presumably hash, but not checked),
# if the tuple is in a local partition which is located "before" the foreign
# partition, the update will fail with
# "cannot route tuples into foreign table to be updated" (postgres_fdw does
# this too).

sub check_update_local_to_foreign_partition_fail {
    my $partition_type = shift;
    my $parent_table_name = shift;
    my $strval = shift;
    my $new_intval = shift;

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            q|UPDATE %s SET id = %i WHERE val = '%s' RETURNING id, val|,
            $parent_table_name,
            $new_intval,
            $strval,
        ),
    );

    my $expected = q|cannot route tuples into foreign table to be updated|;

    like(
        $res_stderr,
        qr/$expected/,
        sprintf(
            q|Check UPDATE failure when moving tuple from local to foreign %s partition|,
            $partition_type,
        ),
    );
}


# Check UPDATE which attempts to move tuple from local to foreign
# partition where this should succeed
# ------------------------------------------------------------------

sub check_update_local_to_foreign_list_success {
    my $partition_type = shift;
    my $parent_table_name = shift;
    my $strval = shift;
    my $new_intval = shift;

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            q|UPDATE %s SET id = %i WHERE val = '%s' RETURNING id, val|,
            $parent_table_name,
            $new_intval,
            $strval,
        ),
    );

    my $expected = sprintf(
        q/%i|%s/,
        $new_intval,
        $strval,
    );

    is (
        $res_stdout,
        $expected,
        sprintf(
            q|Check UPDATE moving tuple from local to foreign %s partition|,
            $partition_type,
        ),
    );
}


sub check_update_foreign_to_local_partition_fail {
    my $partition_type = shift;
    my $parent_table_name = shift;
    my $strval = shift;
    my $new_intval = shift;

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            q|UPDATE %s SET id = %i WHERE val = '%s' RETURNING id, val|,
            $parent_table_name,
            $new_intval,
            $strval,
        ),
    );

    my $expected = sprintf(
        q|Operation violates CHECK constraint \S+? on view or table %s|,
        uc($parent_table_name),
    );

    like(
        $res_stderr,
        qr/$expected/,
        sprintf(
            q|Check UPDATE failure when moving tuple from foreign to local %s partition|,
            $partition_type,
        ),
    );
}


# Check BEFORE trigger
# --------------------

sub check_before_trigger {

    my $trigger_function_name = sprintf(
        q|%s_insert_trigfunc|,
        $child_range_table_1,
    );

    $node->safe_psql(
        sprintf(
            <<'EO_SQL',
CREATE FUNCTION %s()
  RETURNS TRIGGER
  LANGUAGE plpgsql
  AS
$$
  BEGIN
    new.val := new.val || ' TRIGGER';
        RETURN new;
    END
$$;
EO_SQL
            $trigger_function_name,
        ),
    );


    $node->safe_psql(
        sprintf(
            <<'EO_SQL',
CREATE TRIGGER %s_insert_trigger
  BEFORE INSERT ON %s
  FOR EACH ROW
     EXECUTE PROCEDURE %s();
EO_SQL
            $child_range_table_1,
            $child_range_table_1,
            $trigger_function_name,
        ),
    );

    $node->safe_psql(
        sprintf(
            <<'EO_SQL',
INSERT INTO %s
    VALUES(198, 'xxx'),
          (298, 'yyy');
EO_SQL
            $parent_range_table_name,
        ),
    );

    my ($res, $res_stdout, $res_stderr) = $node->psql(
        sprintf(
            q|SELECT * FROM %s WHERE id IN (198,298) ORDER BY id|,
            $parent_range_table_name,
        ),
    );

    my $expected = <<EO_TXT;
198|xxx TRIGGER
298|yyy
EO_TXT

    chomp $expected;

    is (
        $res_stdout,
        $expected,
        q|Check tuple routing with BEFORE INSERT trigger|
    );
}

# create_range_partition_table
# ----------------------------

sub create_range_partition_table {
    my $parent_table_name = shift;
    my $child_table_0 = shift;
    my $child_table_1 = shift;
    my $child_table_2 = shift;

    # Create Firebird tables

    $node->firebird_execute_sql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL CHECK (id BETWEEN 100 AND 199),
  val VARCHAR(32)
)
EO_SQL
            $child_table_1,
        ),
    );


    # Create partitioned PostgreSQL tables

    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL,
  val VARCHAR(32)
) PARTITION BY RANGE (id)
EO_SQL
            $parent_table_name,
        ),
    );


    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL CHECK (id BETWEEN 0 AND 99),
  val VARCHAR(32)
)
EO_SQL
            $child_table_0,
        ),
    );

    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL CHECK (id BETWEEN 200 AND 299),
  val VARCHAR(32)
)
EO_SQL
            $child_table_2,
        ),
    );

    # Create foreign table partition

    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE FOREIGN TABLE %s (
  id INT NOT NULL CHECK (id BETWEEN 100 AND 199),
  val VARCHAR(32)
)
SERVER %s
EO_SQL
            $child_table_1,
            $node->server_name(),
        ),
    );

    $node->safe_psql(
        sprintf(
            q|ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (0) TO (99)|,
            $parent_table_name,
            $child_table_0,
        ),
    );

    $node->safe_psql(
        sprintf(
            q|ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (100) TO (199)|,
            $parent_table_name,
            $child_table_1,
        ),
    );

    $node->safe_psql(
        sprintf(
            q|ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (200) TO (299)|,
            $parent_table_name,
            $child_table_2,
        ),
    );
}



# create_list_partition_table
# ----------------------------

sub create_list_partition_table {
    my $parent_table_name = shift;
    my $child_table_0 = shift;
    my $child_table_1 = shift;
    my $child_table_2 = shift;

    # Create Firebird table
    $node->firebird_execute_sql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL CHECK (id IN (1)),
  val VARCHAR(32)
)
EO_SQL
            $child_table_1,
        ),
    );


    # Create partitioned PostgreSQL tables

    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL,
  val VARCHAR(32)
) PARTITION BY LIST (id)
EO_SQL
            $parent_table_name,
        ),
    );


    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL CHECK (id IN (0)),
  val VARCHAR(32)
)
EO_SQL
            $child_table_0,
        ),
    );

    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE FOREIGN TABLE %s (
  id INT NOT NULL CHECK (id IN (1)),
  val VARCHAR(32)
)
SERVER %s
EO_SQL
            $child_table_1,
            $node->server_name(),
        ),
    );


    $node->safe_psql(
        sprintf(
            <<EO_SQL,
CREATE TABLE %s (
  id INT NOT NULL CHECK (id IN (2)),
  val VARCHAR(32)
)
EO_SQL
            $child_table_2,
        ),
    );


    $node->safe_psql(
        sprintf(
            q|ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (0)|,
            $parent_table_name,
            $child_table_0,
        ),
    );

    $node->safe_psql(
        sprintf(
            q|ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (1)|,
            $parent_table_name,
            $child_table_1,
        ),
    );

    $node->safe_psql(
        sprintf(
            q|ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (2)|,
            $parent_table_name,
            $child_table_2,
        ),
    );
}


# Clean up
# --------

sub cleanup {
    $node->firebird_drop_table($child_range_table_1);

    $node->firebird_drop_table($child_list_table_1);

    $node->drop_foreign_server();

    done_testing();
}
