Firebird Foreign Data Wrapper for PostgreSQL
============================================

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [Firebird](https://firebirdsql.org/). It provides both read (`SELECT`) and
write (`INSERT`/`UPDATE`/`DELETE`) support, as well as pushdown of some
operations. While it appears to be working reliably, please be aware this is
still very much work-in-progress; *USE AT YOUR OWN RISK*.

`firebird_fdw` is designed to be compatible with PostgreSQL 9.5 ~ 15.
The range of `firebird_fdw` options available for a particular PostgreSQL
version depends on the state of the Foreign Data Wrapper (FDW) API for that
version; the more recent the version, the more features will be available.
However, not all FDW API features are currently supported.

`firebird_fdw` supports Firebird 2.5 and later. It will probably work with
Firebird 2.0 or later, and may work with earlier versions if the
`disable_pushdowns` option is set (see below), but has never been tested
with those versions.

`firebird_fdw` is developed against the core PostgreSQL community version
and may not be compatible with commercial forks.

This `README` represents the documentation for the current development version
of `firebird_fdw`. Documentation for stable releases is available at the following
links:

 - [1.3.0 README](https://github.com/ibarwick/firebird_fdw/blob/REL_1_3_STABLE/README.md) (2022-12-28)
 - [1.2.3 README](https://github.com/ibarwick/firebird_fdw/blob/REL_1_2_STABLE/README.md) (2022-02-20)
 - [1.1.0 README](https://github.com/ibarwick/firebird_fdw/blob/REL_1_1_STABLE/README.md) (2019-05-31)
 - [1.0.0 README](https://github.com/ibarwick/firebird_fdw/blob/REL_1_0_STABLE/README.md) (2018-11-09)

Contents
--------

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Functions](#functions)
6. [Identifier case handling](#identifier-case-handling)
7. [Generated columns](#generated-columns)
8. [Character set handling](#character-set-handling)
9. [Examples](#examples)
10. [Limitations](#limitations)
11. [TAP tests](#tap-tests)
12. [Development roadmap](#development-roadmap)
13. [Useful links](#useful-links)

Features
--------

- `UPDATE` and `DELETE` statements use Firebird's row identifier `RDB$DB_KEY`
  to operate on arbitrary rows
- `ANALYZE` support
- pushdown of some `WHERE` clause conditions to Firebird (including translation
  of built-in functions)
- Connection caching
- Supports triggers on foreign tables
- Supports `IMPORT FOREIGN SCHEMA` (PostgreSQL 9.5 and later)
- Supports `COPY` and partition tuple routing (PostgreSQL 11 and later)
- Supports `TRUNCATE` operations (PostgreSQL 14 and later)

Supported platforms
-------------------

`firebird_fdw` was developed on Linux and OS X, and should run on any
reasonably POSIX-compliant system.

While in theory it should work on Windows, I am not able to support that
platform. I am however happy to accept any assistance with porting it to
Windows.

Installation
------------

Specific installation instructions for the following operating
systems are provided separately:

- [CentOS/Redhat etc.](doc/INSTALL-centos-redhat.md)
- [Debian/Ubuntu etc.](doc/INSTALL-debian-ubuntu.md)
- [OS X](doc/INSTALL-osx.md)

### Source installation

Prerequisites:

- Firebird client library  and API header file (`ibase.h`)
- `libfq`, a `libpq`-like API wrapper for the Firebird C API; see:

    https://github.com/ibarwick/libfq

  *NOTE* the latest `libfq` version should be used with the current
  `firebird_fdw` version, as the two are usually developed in tandem.

The Firebird include/library files often end up in non-standard locations;
`PG_CPPFLAGS` and `SHLIB_LINK` can be used to provide the appropriate flags.
The Firebird utility [fb_config](https://firebirdsql.org/manual/fbscripts-fb-config.html)
can assist with locating the appropriate locations.

`firebird_fdw` is installed as a PostgreSQL extension; it requires the
`pg_config` binary for the target installation to be in the shell path.

`USE_PGXS=1 make install` should take care of the actual compilation and
installation.

*IMPORTANT*: you *must* build `firebird_fdw` against the PostgreSQL version
it will be installed on.

Usage
-----

## CREATE SERVER options

`firebird_fdw` accepts the following options via the `CREATE SERVER` command:

- **address** as *string*

  The Firebird server's address (default: `localhost`)

- **port** as *integer*

  The Firebird server's port (default: `3050`)

- **database** as *string*

  The name of the Firebird database to connect to.

- **updatable** as *boolean*

  A boolean value indicating whether the foreign server as a whole
  is updatable. Default is true. Note that this can be overridden
  by table-level settings.

- **disable_pushdowns** as *boolean*

  Turns off pushdowns of `WHERE` clause elements to Firebird. Useful
  mainly for debugging and benchmarking.

- **quote_identifiers** as *boolean*

  Quote all identifiers (table and column names) by default. This can
  be overridden with `quote_identifier = 'false'` for individual table
  and column names.

  See "[Identifier case handling](#identifier-case-handling)" for details.

  `firebird_fdw` 1.2.0 and later.

- **implicit_bool_type** as *boolean*

  Turns on implicit conversion of Firebird integer types to PostgreSQL
  `BOOLEAN` types. This is an experimental feature and is disabled by
  default. See column option `implicit_bool_type` for details.

  `firebird_fdw` 1.2.0 and later.

- **batch_size** as *integer*

  Specifies the number of rows which should be inserted in a single `INSERT`
  operation. This setting can be overridden for individual tables.

  `firebird_fdw` 1.3.0 and later / PostgreSQL 14 and later.

## CREATE USER MAPPING options

`firebird_fdw` accepts the following options via the `CREATE USER MAPPING`
command:

- **username** as *string*

  The Firebird username to connect as (not case-sensitive).

- **password** as *string*

  The Firebird user's password.


## CREATE FOREIGN TABLE options

`firebird_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command:

- **table_name** as *string*

  The Firebird table name, if different to the PostgreSQL foreign table
  name. Cannot be used together with the `query` option.

- **quote_identifier** as *boolean*

  Pass the table name to Firebird as a quoted identifier.
  See "[Identifier case handling](#identifier-case-handling)" for details.
  `firebird_fdw` 1.2.0 and later.

- **query** as *string*

  A Firebird SQL statement producing a result set which can be treated
  like a read-only view. Cannot be used together with the `table_name` option.

- **updatable** as *boolean*

  A boolean value indicating whether the table is updatable. Default is `true`.
  Note that this overrides the server-level setting. Cannot be set for the
  `query` option.

- **estimated_row_count** as *integer*

  An integer indicating the expected number of rows in the Firebird table, or
  rows which would be returned by the statement defined in `query`. If not
  set, an attempt will be made to determine the number of rows by executing
  `SELECT COUNT(*) FROM ...`, which can be inefficient, particularly for queries.

The following column-level options are available:

- **column_name** as *string*

  The Firebird column name, if different to the column name defined in the
  foreign table. This can also be used for foreign tables defined with the
  `query` option.

- **quote_identifier** as *boolean*

  Pass the column name to Firebird as a quoted identifier. See section
  See "[Identifier case handling](#identifier-case-handling)" for details.
  `firebird_fdw` 1.2.0 and later.

- **implicit_bool_type** as *boolean*

  Set this option on a `BOOLEAN` column to `true` to indicate that the
  corresponding column in the Firebird table is a integer column which
  should be treated as an implicit `BOOLEAN` type.

  It is assumed that the Firebird column contains one of:
    - `0` to indicate `FALSE`
    - any other value to indicate `TRUE`
    - `NULL`

  The implied boolean values will be transparently translated to PostgreSQL
  `BOOLEAN` values. `WHERE` clauses with implicit boolean expressions will
  be pushed down to Firebird in the same way as normal boolean expressions.

  Note that `firebird_fdw` will currently not push down a boolean scalar array
  operation expression such as `WHERE boolcol IN (TRUE, NULL)`. However the
  semantically equivalent `WHERE boolcol IS NOT FALSE` will be pushed down.

  This is an experimental feature in `firebird_fdw` 1.2.0 and requires
  that the server-level option `implicit_bool_type` is also set to `true`
  (Firebird 3.0 and later).

  If the Firebird server is version 2.5.x, this option does not need to be
  set and `firebird_fdw` will automatically assume that the Firebird column
  represents an implicit boolean. This functionality may work on earlier
  Firebird versions but has not been tested with them.

- **batch_size** as *integer*

  See `CREATE SERVER options` section for details.

  `firebird_fdw` 1.3.0 and later / PostgreSQL 14 and later.

Note that while PostgreSQL allows a foreign table to be defined without
any columns, `firebird_fdw` will raise an error as soon as any operations
are carried out on it.


## IMPORT FOREIGN SCHEMA options

`firebird_fdw` supports [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html)
(when running with PostgreSQL 9.5 or later) and accepts the following custom options:

- **import_not_null** as *boolean*

  Determines whether column `NOT NULL` constraints are included in the definitions
  of foreign tables imported from a Firebid server. The default is `true`.

- **import_views** as *boolean*

  Determines whether Firebird views are imported as foreign tables. The default is `true`.

- **updatable** as *boolean*

  If set to `false`, mark all imported foreign tables as not updatable. The default is `true`.
 
- **verbose** as *boolean*

  Logs the name of each table or view being imported at log level `INFO`.

`IMPORT FOREIGN SCHEMA` will quote Firebird table column names if required, and if the
Firebird name is entirely lower-case, will add the appropriate `quote_identifier`
option to the PostgreSQL table definition.

Note that when specifying the `LIMIT TO` option, any quoted table names will result in
the corresponding PostgreSQL foreign table being created with a quoted table name.
This is due to PostgreSQL's foreign data wrapper API, which filters the
table definitions passed back from the foreign data wrapper on the basis of
the table name provided in the `IMPORT FOREIGN SCHEMA` command. However, Firebird table
names which are entirely lower-case can currently not be provided as quoted
column names as PostgreSQL considers these as unquoted by default and the
foreign data wrapper has no way of knowing whether they were originally quoted.

## TRUNCATE support

`firebird_fdw` implements the foreign data wrapper `TRUNCATE` API, available
from PostgreSQL 14.

As Firebird does not provide a `TRUNCATE` command, it is simulated with a
simple unqualified `DELETE` operation.

Following restrictions apply:

 - `TRUNCATE ... CASCADE` is not supported
 - `TRUNCATE ... RESTART IDENTITY` is not supported
 - Firebird tables with foreign key references cannot be truncated

These restrictions may be removed in future releases.

Functions
---------

As well as the standard `firebird_fdw_handler()` and `firebird_fdw_validator()`
functions, `firebird_fdw` provides the following user-callable utility functions:

- **firebird_fdw_version()**

  Returns the version number as an integer.

- **firebird_fdw_close_connections()**

  Closes all cached connections from PostgreSQL to Firebird in the current session.

- **firebird_fdw_server_options(servername TEXT)**

  Returns the server-level option settings for the named server (either the options provided
  to `CREATE SERVER` or if not provided, the respective default values); example:

      postgres=# SELECT * FROM firebird_fdw_server_options('firebird_server');
              name        |                    value                     | provided
      --------------------+----------------------------------------------+----------
       address            | localhost                                    | t
       port               | 3050                                         | f
       database           | /var/lib/firebird/data/firebird_fdw_test.fdb | t
       updatable          | true                                         | f
       quote_identifiers  | false                                        | f
       implicit_bool_type | false                                        | f
       disable_pushdowns  | false                                        | t
      (7 rows)

  (`firebird_fdw` 1.2.0 and later)

- **firebird_fdw_diag()**

  Returns ad-hoc information about the Firebird FDW in key/value form, example:

      postgres=# SELECT * FROM firebird_fdw_diag();
                  name             | setting
      -----------------------------+---------
       firebird_fdw_version        | 10100
       firebird_fdw_version_string | 1.1.0
       libfq_version               | 400
       libfq_version_string        | 0.4.0
       cached_connection_count     | 1
      (5 rows)

- **firebird_version()**

  Returns the Firebird version numbers for each `firebird_fdw` foreign server
  defined in the current database, for example:

      postgres=# SELECT * FROM firebird_version();
         server_name   | firebird_version | firebird_version_string
      -----------------+------------------+-------------------------
       firebird_server |            30005 | 3.0.5
      (1 row)

  Note that this function will open a connection to each Firebird server
  if no previously cached connection exists. It will return a row for each
  user mapping defined, even if those map to the same Firebird server.

  (`firebird_fdw` 1.2.0 and later)


Identifier case handling
------------------------

As PostgreSQL and Firebird take opposite approaches to case folding (PostgreSQL
folds identifiers to lower case by default, Firebird to upper case), it's important
to be aware of potential issues with table and column names.

When defining foreign tables, PostgreSQL will pass any identifiers which do not
require quoting to Firebird as-is, defaulting to lower-case. Firebird will then
implictly fold these to upper case. For example, given the following table
definitions in Firebird and PostgreSQL:

    CREATE TABLE CASETEST1 (
      COL1 INT
    )

    CREATE FOREIGN TABLE casetest1 (
      col1 INT
    )
    SERVER fb_test

and given the PostgreSQL query:

    SELECT col1 FROM casetest1

`firebird_fdw` will generate the following Firebird query:

    SELECT col1 FROM casetest1

which is valid in both PostgreSQL and Firebird.

By default, PostgreSQL will pass any identifiers which do require quoting
according to PostgreSQL's definition as quoted identifiers to Firebird. For
example, given the following table definitions in Firebird and PostgreSQL:

    CREATE TABLE "CASEtest2" (
      "Col1" INT
    )

    CREATE FOREIGN TABLE "CASEtest2" (
      "Col1" INT
    )
    SERVER fb_test

and given the PostgreSQL query:

    SELECT "Col1" FROM "CASEtest2"

`firebird_fdw` will generate the following Firebird query:

    SELECT "Col1" FROM "CASEtest2"

which is also valid in both PostgreSQL and Firebird.

The same query will also be generated if the Firebird table and column names
are specified as options:

    CREATE FOREIGN TABLE casetest2a (
      col1 INT OPTIONS (column_name 'Col1')
    )
    SERVER fb_test
    OPTIONS (table_name 'CASEtest2')

However PostgreSQL will not quote lower-case identifiers by default. With the
following Firebird and PostgreSQL table definitions:

    CREATE TABLE "casetest3" (
      "col1" INT
    )

    CREATE FOREIGN TABLE "casetest3" (
      "col1" INT
    )
    SERVER fb_test

any attempt to access the foreign table `casetest3` will result in the Firebird
error `Table unknown: CASETEST3`, as Firebird is receiving the unquoted PostgreSQL
table name and folding it to upper case.

To ensure the correct table or column name is included in queries sent to Firebird,
from `firebird_fdw` 1.2.0 the table or column-level option `quote_identifier` can
be provided, which will force the table or column name to be passed as a quoted
identifier. The preceding foreign table should be defined like this:

    CREATE FOREIGN TABLE casetest3 (
      col1 INT OPTIONS (quote_identifier 'true')
    )
    SERVER fb_test
    OPTIONS (quote_identifier 'true')

and given the PostgreSQL query:

    SELECT col1 FROM casetest3

`firebird_fdw` will generate the following Firebird query:

    SELECT "col1" FROM "casetest3"

The server-level option `quote_identifiers` can be set to `true` to quote all identifiers
(table and column names) by default. This setting can be overridden for individual
table and column names by setting the respective `quote_identifier` option to `false`.


Generated columns
-----------------

`firebird_fdw` (1.2.0 and later) provides support for PostgreSQL's generated
columns (PostgreSQL 12 and later).

Note that while `firebird_fdw` will insert or update the generated column value
in Firebird, there is nothing to stop the value being modified within Firebird,
and hence no guarantee that in subsequent `SELECT` operations the column will
still contain the expected generated value. This limitation also applies to
`postgres_fdw`.

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)


Character set handling
----------------------

When `firebird_fdw` connects to a Firebird database, it will set the client
encoding to the PostgreSQL database's server encoding. As there is a broad
overlap between PostgreSQL and Firebird character set encodings, mostly
this will succeed, particularly with the more common encodings such as
`UTF8` and `LATIN1`. A small subset of PostgreSQL encodings for which Firebird
provides a corresponding encoding but no matching name or alias will be
rewritten transparently by `firebird_fdw`. For more details see the
file [PostgreSQL and Firebird character set encoding compatibility](doc/ENCODINGS.md).


Examples
--------

To install the extension in a database, connect *as superuser* and execute:

    CREATE EXTENSION firebird_fdw;

Create a foreign server *as superuser* with appropriate configuration:

    CREATE SERVER firebird_server
      FOREIGN DATA WRAPPER firebird_fdw
      OPTIONS (
        address 'localhost',
        database '/path/to/database'
     );

Create an appropriate user mapping *as superuser*:

    CREATE USER MAPPING FOR CURRENT_USER SERVER firebird_server
      OPTIONS(username 'sysdba', password 'masterkey');
      
Grant *as superuser* `usage` privelegy to non privileged user from the previous user mapping for creating foreign tables:

    GRANT USAGE ON FOREIGN SERVER firebird_server TO "a pg user";
    -- change "a pg user" to real user name you need

Create a foreign table referencing the Firebird table `fdw_test`:

    CREATE FOREIGN TABLE fb_test(
      id SMALLINT,
      val VARCHAR(2048)
    )
    SERVER firebird_server
    OPTIONS(
      table_name 'fdw_test'
    );

As above, but with aliased column names:

    CREATE FOREIGN TABLE fb_test_table(
      id SMALLINT OPTIONS (column_name 'test_id'),
      val VARCHAR(2048) OPTIONS (column_name 'test_val')
    )
    SERVER firebird_server
    OPTIONS(
      table_name 'fdw_test'
    );

Create a foreign table as a Firebird query:

    CREATE FOREIGN TABLE fb_test_query(
      id SMALLINT,
      val VARCHAR(2048)
    )
    SERVER firebird_server
    OPTIONS(
      query $$ SELECT id, val FROM fdw_test $$
    );

Import a Firebird schema:

    IMPORT FOREIGN SCHEMA someschema
      LIMIT TO (sometable)
      FROM SERVER firebird_server
      INTO public;

    IMPORT FOREIGN SCHEMA someschema
      FROM SERVER firebird_server
      INTO public
      OPTIONS (verbose 'true', import_views 'false');

Note: `someschema` has no particular meaning and can be set to an arbitrary value.

Limitations
-----------

- Works with Firebird 3.x, but does not yet support all 3.x features
- No support for Firebird `ARRAY` datatype
- The result of the Firebird query is copied into memory before being
  processed by PostgreSQL; this could be improved by using Firebird cursors

TAP tests
---------

Simple TAP tests are provided in the `t/` directory. These require a running
Firebird database to be available; provide connection details for this with
the standard Firebird environment variables `ISC_DATABASE`, `ISC_USER` and
`ISC_PASSWORD`. Additionally, the non-standard environment variable `ISC_PORT`
can be provided to specify a non-default port number.

The tests are designed for PostgreSQL 9.5 and later, and require it to have
been compiled with the `--enable-tap-tests` option.

Run with

    make prove_installcheck

The TAP tests will create temporary tables in the Firebird database and
remove them after test completion.

Development roadmap
-------------------

Haha, nice one. I should point out that `firebird_fdw` is an entirely personal
project carried out by myself in my (limited) free time for my own personal
gratification. While I'm happy to accept feedback, suggestions, feature
requests, bug reports and (especially) patches, please understand that
development is entirely at my own discretion depending on (but not limited
to) available free time and motivation.

However if you are a commercial entity and wish to have any improvements
etc. carried out within a plannable period of time, this can be arranged
via my employer.

Having said that, things I would like to do are:

 - improve support for Firebird 3.0 features
 - add support for missing data types
 - improve support for recent features added to the PostgreSQL FDW API.


Useful links
------------

### Source

 - https://github.com/ibarwick/firebird_fdw (public mirror)
 - https://pgxn.org/dist/firebird_fdw/

### Blog (including release notes)

 - https://sql-info.de/postgresql/firebird-fdw/index.html

### General FDW Documentation

 - https://www.postgresql.org/docs/current/ddl-foreign-data.html
 - https://www.postgresql.org/docs/current/sql-createforeigndatawrapper.html
 - https://www.postgresql.org/docs/current/sql-createforeigntable.html
 - https://www.postgresql.org/docs/current/sql-importforeignschema.html
 - https://www.postgresql.org/docs/current/fdwhandler.html
 - https://www.postgresql.org/docs/current/postgres-fdw.html

### Other FDWs

 - https://wiki.postgresql.org/wiki/Fdw
 - https://pgxn.org/tag/fdw/

If you appreciate PostgreSQL's `psql` client, why not try `fbsql`, a `psql`-style
client for Firebird? See: https://github.com/ibarwick/fbsql for details.
