#!/usr/bin/env perl

# 03-triggers.pl
#
# Check triggers are supported (9.4 and later)
#
# TODO: verify BEFORE triggers too

use strict;
use warnings;

use Cwd;
use Config;
use TestLib;
use Test::More;

use FirebirdFDWNode;
use FirebirdFDWDB;



# Initialize PostgreSQL node
# --------------------------

my $pg_node = get_new_fdw_node('pg_node');

$pg_node->init();
$pg_node->start();

my $pg_db = FirebirdFDWDB->new($pg_node);

# Get version
# -----------

my $version = $pg_db->version();
if ($version < 90400) {
    plan skip_all => sprintf(
        q|version is %i, tests for 9.4 and later|,
        $version,
    );
}
else {
    plan tests => 3;
}


# Prepare table
# --------------

my $table_name = $pg_db->init_table();

# Initialize trigger
# ------------------

my $results_tbl = <<EO_SQL;
CREATE TABLE results (
  subtest SERIAL,
  old_val TEXT NULL,
  new_val TEXT NULL
)
EO_SQL

$pg_db->safe_psql($results_tbl);

my $trigger_func = <<'EO_SQL';
CREATE OR REPLACE FUNCTION test_trigger() RETURNS TRIGGER AS $$
  BEGIN

    IF TG_OP = 'INSERT' THEN
      INSERT INTO results (subtest, new_val)
        VALUES (DEFAULT, NEW.name_native);
      RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
      INSERT INTO results (subtest, old_val, new_val)
        VALUES (DEFAULT, OLD.name_native, NEW.name_native);
      RETURN NEW;
    END IF;

    INSERT INTO results (subtest, old_val)
      VALUES (DEFAULT, OLD.name_native);

    RETURN OLD;
  END
$$ language plpgsql
EO_SQL

$pg_db->safe_psql($trigger_func);

my $trigger_def = sprintf(
    <<EO_SQL,
CREATE TRIGGER trigtest_after_stmt
  AFTER INSERT OR UPDATE OR DELETE
  ON public.%s
  FOR EACH ROW
    EXECUTE PROCEDURE test_trigger()
EO_SQL
    $table_name,
);

$pg_db->safe_psql($trigger_def);

# INSERT test
# -----------

my $subtest = 1;

my $insert_sql = sprintf(
    <<EO_SQL,
INSERT INTO %s (lang_id, name_english, name_native)
         VALUES('aa', 'foo', 'bar');
EO_SQL
    $table_name,
);

$pg_db->safe_psql($insert_sql);

my $insert_res = $pg_db->safe_psql(
    sprintf(q|SELECT * FROM results WHERE subtest = %i|, $subtest),
);

is (
    $insert_res,
    q/1||bar/,
    q|Check INSERT|,
);

# UPDATE test
# -----------

$subtest++;

my $update_sql  = sprintf(
    <<EO_SQL,
  UPDATE %s
     SET name_native = 'baz'
   WHERE lang_id = 'aa'
EO_SQL
    $table_name,
);

$pg_db->safe_psql($update_sql);

my $update_res = $pg_db->safe_psql(
    sprintf(q|SELECT * FROM results WHERE subtest = %i|, $subtest),
);

is (
    $update_res,
    q/2|bar|baz/,
    q|Check UPDATE|,
);

# DELETE test
# -----------

$subtest++;

my $delete_sql  = sprintf(
    <<EO_SQL,
  DELETE FROM %s
        WHERE lang_id = 'aa'
EO_SQL
    $table_name,
);

$pg_db->safe_psql($delete_sql);

my $delete_res = $pg_db->safe_psql(
    sprintf(q|SELECT * FROM results WHERE subtest = %i|, $subtest),
);

is (
    $delete_res,
    q/3|baz|/,
    q|Check DELETE|,
);

done_testing();
