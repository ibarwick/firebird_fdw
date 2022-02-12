#!/usr/bin/env perl

# 03-triggers.pl
#
# Check triggers are supported (9.4 and later)

use strict;
use warnings;

use Test::More;

use FirebirdFDWNode;


# Initialize nodes
# ----------------

my $node = FirebirdFDWNode->new();

# Get PostgreSQL version
# ----------------------

my $version = $node->pg_version();
if ($version < 90400) {
    plan skip_all => sprintf(
        q|version is %i, tests for 9.4 and later|,
        $version,
    );
}
else {
    plan tests => 5;
}


# Prepare table
# --------------

my $table_name = $node->init_table();

# Initialize trigger
# ------------------

my $results_tbl = <<'EO_SQL';
CREATE TABLE results (
  subtest SERIAL,
  old_val TEXT NULL,
  new_val TEXT NULL
)
EO_SQL

$node->safe_psql($results_tbl);

my $after_trigger_func = <<'EO_SQL';
CREATE OR REPLACE FUNCTION test_after_trigger_func()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
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
$$
EO_SQL

$node->safe_psql($after_trigger_func);

my $before_trigger_func = <<'EO_SQL';
CREATE OR REPLACE FUNCTION test_before_trigger_func()
  RETURNS TRIGGER
  LANGUAGE plpgsql
AS $$
  BEGIN
    IF TG_OP = 'INSERT' THEN
      NEW.name_native = NEW.name_english || ' ins';
    ELSIF TG_OP = 'UPDATE' THEN
      NEW.name_native = NEW.name_english || ' upd';
    END IF;
    RETURN NEW;
  END;
$$;
EO_SQL

$node->safe_psql($before_trigger_func);

my $after_trigger_def = sprintf(
    <<'EO_SQL',
CREATE TRIGGER trigtest_after_stmt
  AFTER INSERT OR UPDATE OR DELETE
  ON public.%s
  FOR EACH ROW
    EXECUTE PROCEDURE test_after_trigger_func()
EO_SQL
    $table_name,
);

$node->safe_psql($after_trigger_def);

my $before_trigger_def = sprintf(
    <<'EO_SQL',
CREATE TRIGGER trigtest_before_stmt
  BEFORE INSERT OR UPDATE OR DELETE
  ON public.%s
  FOR EACH ROW
    EXECUTE PROCEDURE test_before_trigger_func()
EO_SQL
    $table_name,
);

$node->safe_psql($before_trigger_def);

$node->safe_psql(
    sprintf(q|ALTER TABLE public.%s DISABLE TRIGGER trigtest_before_stmt|, $table_name),
);

# 1. AFTER ... INSERT test
# ------------------------

my $subtest = 1;

my $after_insert_sql = sprintf(
    <<'EO_SQL',
INSERT INTO %s (lang_id, name_english, name_native)
         VALUES('aa', 'foo', 'bar');
EO_SQL
    $table_name,
);

$node->safe_psql($after_insert_sql);

my ($after_insert_res, $after_insert_stdout, $after_insert_stderr) = $node->psql(
    sprintf(q|SELECT * FROM results WHERE subtest = %i|, $subtest),
);

is (
    $after_insert_stdout,
    q/1||bar/,
    q|Check AFTER ... INSERT|,
);

# 2. AFTER ... UPDATE test
# ------------------------

$subtest++;

my $after_update_sql  = sprintf(
    <<'EO_SQL',
  UPDATE %s
     SET name_native = 'baz'
   WHERE lang_id = 'aa'
EO_SQL
    $table_name,
);

$node->safe_psql($after_update_sql);

my ($after_update_res, $after_update_stdout, $after_update_stderr) = $node->psql(
    sprintf(q|SELECT * FROM results WHERE subtest = %i|, $subtest),
);

is (
    $after_update_stdout,
    q/2|bar|baz/,
    q|Check AFTER ... UPDATE|,
);

# 3. AFTER ... DELETE test
# ------------------------

$subtest++;

my $after_delete_sql = sprintf(
    <<'EO_SQL',
  DELETE FROM %s
        WHERE lang_id = 'aa'
EO_SQL
    $table_name,
);

$node->safe_psql($after_delete_sql);

my ($after_delete_res, $after_delete_stdout, $after_delete_stderr)= $node->psql(
    sprintf(q|SELECT * FROM results WHERE subtest = %i|, $subtest),
);

is (
    $after_delete_stdout,
    q/3|baz|/,
    q|Check AFTER ... DELETE|,
);

# Disable AFTER, enable BEFORE trigger
# ------------------------------------

$node->safe_psql(
    sprintf(q|ALTER TABLE public.%s DISABLE TRIGGER trigtest_after_stmt|, $table_name),
);

$node->safe_psql(
    sprintf(q|ALTER TABLE public.%s ENABLE TRIGGER trigtest_before_stmt|, $table_name),
);

# 4. BEFORE ... INSERT test
# --------------------------

my $before_insert_sql = sprintf(
    qq|INSERT INTO %s values('bb','foo')|,
    $table_name,
);

$node->safe_psql($before_insert_sql);

my $before_insert_check_sql = sprintf(
    q|SELECT name_native FROM %s WHERE lang_id = 'bb'|,
    $table_name,
);

my $before_insert_query = $node->firebird_conn()->prepare($before_insert_check_sql);

$before_insert_query->execute();

my $before_insert_res = $before_insert_query->fetchrow_array();

$before_insert_query->finish();

is (
    $before_insert_res,
    q|foo ins|,
    q|Check BEFORE ... INSERT|,
);


# 5. BEFORE ... UPDATE test
# --------------------------

my $before_update_sql = sprintf(
    q|UPDATE %s SET name_english = 'bar' WHERE lang_id = 'bb'|,
    $table_name,
);

$node->safe_psql($before_update_sql);

my $before_update_check_sql = sprintf(
    q|SELECT name_native FROM %s WHERE lang_id = 'bb'|,
    $table_name,
);

my $before_update_query = $node->firebird_conn()->prepare($before_update_check_sql);

$before_update_query->execute();

my $before_update_res = $before_update_query->fetchrow_array();

$before_update_query->finish();

is (
    $before_update_res,
    q|bar upd|,
    q|Check BEFORE ... UPDATE|,
);




# Clean up
# --------

$node->drop_foreign_server();
$node->firebird_drop_table($table_name);

done_testing();
