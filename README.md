Firebird Foreign Data Wrapper for PostgreSQL
============================================

This is a foreign data wrapper (FDW) to connect PostgreSQL to Firebird.
It provides both read (`SELECT`) and write (`INSERT`/`UPDATE`/`DELETE`) support,
as well as pushdown of some operations. While it appears to be working
reliably, please be aware this is still very much work-in-progress;
*USE AT YOUR OWN RISK*.

`firebird_fdw` is designed to be compatible with PostgreSQL 9.2 ~ 11.
Write support is only available in PostgreSQL 9.3 and later. Note that
not all features of the PostgreSQL FDW API are supported.

It was written for Firebird 2.5 and will probably work with Firebird 2.0 or
later. It should work with earlier versions if the `disable_pushdowns` option
is set (see below). It works with Firebird 3.0.x but has not yet been extensively
tested with that version, and does not take advantage of all new Firebird 3
features. This will hopefully be addressed in future releases.

Features
--------

- `UPDATE` and `DELETE` statements use Firebird's row identifier `RDB$DB_KEY`
  to operate on arbitrary rows
- `ANALYZE` support
- pushdown of some `WHERE` clause conditions to Firebird (including translation
  of built-in functions)
- Connection caching
- Supports triggers on foreign tables (PostgreSQL 9.4 and later)
- Supports `IMPORT FOREIGN SCHEMA` (PostgreSQL 9.5 and later)


Supported platforms
-------------------

`firebird_fdw` was developed on Linux and OS X, and should run on any
reasonably POSIX-compliant system.

Installation
------------

### From packages

RPM packages for CentOS and derivatives are available via the Fedora "copr"
build system; for details see here:
<https://copr.fedorainfracloud.org/coprs/ibarwick/firebird_fdw/>

### From source

Prerequisites:

- Firebird client library (`libfbclient`) and API header file (`ibase.h`)
- `libfq`, a slightly saner API wrapper for the Firebird C API; see:

    https://github.com/ibarwick/libfq

  *NOTE* the latest `libfq` version should be used with the current
  `firebird_fdw` version, as the two are usually developed in tandem.

The Firebird include/library files often end up in non-standard locations;
`PG_CPPFLAGS` and `SHLIB_LINK` can be used to provide the appropriate flags.
For OS X they would look something like this:

    export PG_CPPFLAGS="-I /Library/Frameworks/Firebird.framework/Versions/A/Headers/"
    export SHLIB_LINK="-L/Library/Frameworks/Firebird.framework/Versions/A/Libraries/"

`firebird_fdw` is installed as a PostgreSQL extension; it requires the
`pg_config` binary for the target installation to be in the shell path.

`USE_PGXS=1 make install` should take care of the actual compilation and
installation.


Usage
-----

### CREATE SERVER options

`firebird_fdw` accepts the following options via the `CREATE SERVER` command:

    'address':
        The Firebird server's address (default: localhost)

    'database':
        The name of the database to connect to

    'username':
        The username to connect as (not case-sensitive)

    'password':
        The user's password (note that Firebird only recognizes the first 8
        characters of a password)

    'updatable':
        Boolean value indicating whether the foreign server as a whole
        is updatable. Default is true. Note that this can be overridden
        by table-level settings.


### CREATE FOREIGN TABLE options

`firebird_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command:

    'table_name':
        The Firebird table name (not case-sensitive). Cannot be used together
        with the 'query' option.

    'query':
        A Firebird SQL statement producing a result set which can be treated
        like a table. Cannot be used together with the 'table_name' option.

    'disable_pushdowns':
        Turns off pushdowns of WHERE clause elements to Firebird. Useful
        mainly for debugging and benchmarking.

    'updatable':
        Boolean value indicating whether the table is updatable. Default is `true`.
        Note that this overrides the server-level setting.

    'estimated_row_count':
        Integer indicating the expected number of rows in the Firebird table, or
        rows which would be returned by the statement defined in 'query'. If not
        set, an attempt will be made to determine the number of rows by executing
        "SELECT COUNT(*) FROM ...", which can be inefficient, particularly for
        queries.

The following column-level option is available:

    'column_name':
        The Firebird column name (not case-sensitive), if different to the column
        name defined in the foreign table. This can also be used for foreign
        tables defined with the `query` option.

Note that while PostgreSQL allows a foreign table to be defined without
any columns, `firebird_fdw` will raise an error as soon as any operations
are carried out on it.


### IMPORT FOREIGN SCHEMA options

`firebird_fdw` supports `IMPORT FOREIGN SCHEMA` (when running with PostgreSQL
9.5 or later), with the following options:

    'import_not_null':
        Determines whether column `NOT NULL` constraints are included in the definitions
        of foreign tables imported from a Firebid server. The default is `true`.

    'import_views':
        Determines whether Firebird views are imported as foreign tables. The default is `true`.

    'verbose':
        Logs the name of each table or view being imported at log level `INFO`.


Examples
--------

Install the extension:

    CREATE EXTENSION firebird_fdw;
    CREATE FOREIGN DATA WRAPPER firebird
      HANDLER firebird_fdw_handler
      VALIDATOR firebird_fdw_validator;

Create a foreign server with appropriate configuration:

    CREATE SERVER firebird_server
      FOREIGN DATA WRAPPER firebird
      OPTIONS (
        address 'localhost',
        database '/path/to/database'
     );

Create an appropriate user mapping:

    CREATE USER MAPPING FOR CURRENT_USER SERVER firebird_server
      OPTIONS(username 'sysdba', password 'masterke');

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

Note: `someschema` has no particular meaning and can be set to an arbitrary value.


Limitations
-----------

- Works with Firebird 3.x, but does not yet support all 3.x features
- No support for Firebird `ARRAY` datatype
- Display of messages returned by Firebird is not very pretty
  (this has been improved somewhat)
- No consideration given to object names which may require
  quoting when passed between PostgreSQL and Firebird


TAP tests
---------

Simple TAP tests are provided in the `t/` directory. These require a running
Firebird database to be available; provide connection details for this with
the standard Firebird environment variables `ISC_DATABASE`, `ISC_USER` and
`ISC_PASSWORD`. Additionally, PostgreSQL must have been compiled with
the `--enable-tap-tests` option.

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

Having said that, things I would like to do at some point are:

 - support Firebird 3.0
 - add support for missing data types
 - improve support for recent features to the PostgreSQL FDW API.


Useful links
------------

* Source
 - https://github.com/ibarwick/firebird_fdw (public mirror)
 - http://pgxn.org/dist/firebird_fdw/

* Blog (including release notes)
 - http://sql-info.de/postgresql/firebird-fdw/index.html

* General FDW Documentation
 - https://www.postgresql.org/docs/current/static/ddl-foreign-data.html
 - https://www.postgresql.org/docs/current/static/sql-createforeigndatawrapper.html
 - https://www.postgresql.org/docs/current/static/sql-createforeigntable.html
 - https://www.postgresql.org/docs/current/static/sql-importforeignschema.html
 - https://www.postgresql.org/docs/current/static/fdwhandler.html
 - https://www.postgresql.org/docs/current/static/postgres-fdw.html

* Other FDWs
 - https://wiki.postgresql.org/wiki/Fdw
 - http://pgxn.org/tag/fdw/

If you appreciate PostgreSQL's `psql` client, why not try `fbsql`, a `psql`-style
client for Firebird? See: https://github.com/ibarwick/fbsql for details.
