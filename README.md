Firebird Foreign Data Wrapper for PostgreSQL
============================================

This is an experimental foreign data wrapper (FDW) to connect PostgreSQL
to Firebird. It provides basic functionality, including both read (`SELECT`)
and write (`INSERT`/`UPDATE`/`DELETE`) support. However it is still very much
work-in-progress; *USE AT YOUR OWN RISK*.

`firebird_fdw` is designed to be compatible with PostgreSQL 9.2 ~ 10.
Write support is only available in PostgreSQL 9.3 and later. Note that
not all features of the PostgreSQL FDW API are supported.

It was written for Firebird 2.5 and will probably work with Firebird 2.0 or
later. It should work with earlier versions if the `disable_pushdowns` option
is set (see below). Currently (2018-04) it works with Firebird 3.0.x but has
not been extensively tested with that version, and does not take advantage
of any new Firebird 3 features.

Supported platforms
-------------------

`firebird_fdw` was developed on Linux and OS X, and should run on any
reasonably POSIX-compliant system.

Installation
------------

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

The usual `make && make install` should take care of the actual compilation.


Usage
-----

**NOTE: these options are provisional and may change**

`firebird_fdw` accepts the following options:

    'address':
        The Firebird server's address (default: localhost)

    'database':
        The name of the database to connect to

    'username':
        The username to connect as (not case-sensitive)

    'password':
        The user's password (note that Firebird only recognizes the first 8
        characters of a password)

    'table_name':
        The Firebird table name (not case-sensitive). Cannot be used together
        with the 'query' option.

    'query':
        A Firebird SQL statement producing a result set which can be treated
        like a table. Cannot be used together with the 'table_name' option.

    'column_name':
        The Firebird column name (not case-sensitive).

    'updatable':
        Boolean value indicating whether the foreign server as a whole,
        or an individual table, is updatable. Default is true. Note that
        table-level settings override server-level settings.

    'disable_pushdowns':
        Turns off pushdowns of WHERE clause elements to Firebird. Useful
        mainly for debugging and benchmarking.

Note that while PostgreSQL allows a foreign table to be defined without
any columns, `firebird_fdw`  will raise an error as soon as any operations
are carried out on it.


Example
-------

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
      query 'SELECT id, val FROM fdw_test'
    );


Features
--------

- `UPDATE` and `DELETE` statements use Firebird's row identifier `RDB$DB_KEY`
  to operate on arbitrary rows
- `ANALYZE` support
- pushdown of some `WHERE` clause conditions to Firebird (including translation
  of built-in functions)
- Connection caching
- Supports `IMPORT FOREIGN SCHEMA` (PostgreSQL 9.5 and later)


Limitations
-----------

- Works with Firebird 3.x, but does not yet support any 3.x features
- No support for Firebird datatypes (`BLOB`, `ARRAY`)
- Display of messages returned by Firebird is not very pretty
  (this has been improved somewhat)


Regression tests
----------------

A simple set of regression tests is provided. To run this, following
requirements must be met:

 - a running PostgreSQL cluster with `firebird_fdw` available
 - a running Firebird server

The Firebird server must be accessible via the default
`SYSDBA`/`MASTERKE` user/password combination and a database
`/tmp/firebird_fdw.fdb` must exist with following table defined:

    CREATE TABLE test1(id int);

Run with:

    gmake installcheck


Development roadmap
-------------------

Haha, nice one. I should point out that `firebird_fdw` is an entirely personal
project carried out by myself in my (limited) free time for my own personal
gratification. While I'm happy to accept feedback, suggestions, feature
requests, bug reports and (especially) patches, please understand that
development is entirely at my own discretion depending on (but not limited
to) available free time and motiviation.

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

* General FDW Documentation
 - http://www.postgresql.org/docs/current/interactive/ddl-foreign-data.html
 - http://www.postgresql.org/docs/current/interactive/sql-createforeigndatawrapper.html
 - http://www.postgresql.org/docs/current/interactive/sql-createforeigntable.html
 - http://www.postgresql.org/docs/current/interactive/fdwhandler.html
 - http://www.postgresql.org/docs/current/interactive/postgres-fdw.html
 - http://www.postgresql.org/docs/devel/static/sql-importforeignschema.html

* Other FDWs
 - https://wiki.postgresql.org/wiki/Fdw
 - http://pgxn.org/tag/fdw/

If you appreciate PostgreSQL's `psql` client, why not try `fbsql`, a `psql`-style
client for Firebird? See: https://github.com/ibarwick/fbsql for details.

