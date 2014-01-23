Firebird Foreign Data Wrapper for PostgreSQL
============================================

This is an experimental foreign data wrapper (FDW) to connect PostgreSQL
to Firebird. It provides basic functionality, including both read (SELECT)
and write (INSERT/UPDATE/DELETE) support.However it is still very much
work-in-progress; USE AT YOUR OWN RISK.

firebird_fdw will work with PostgreSQL 9.2 or later (it was developed
against the current development version). Write support is only available
in PostgreSQL 9.3 and later.

It was written for Firebird 2.5 and will probably work with Firebird 2.0 or
later. It should work with earlier versions if the 'disable_pushdowns' option
is set (see below).


Supported platforms
-------------------

firebird_fdw was developed on Linux and OS X, and should run on any
reasonably POSIX-compliant system.


Installation
------------

Prerequisites:

- Firebird client library (libfbclient) and API header file (ibase.h)
- libfq, a slightly saner API wrapper for the Firebird C API; see:
  https://github.com/ibarwick/libfq

The Firebird include/library files often end up in non-standard locations;
PG_CPPFLAGS and SHLIB_LINK can be used to provide the appropriate flags.
For OS X they would look something like this:

    export PG_CPPFLAGS="-I /Library/Frameworks/Firebird.framework/Versions/A/Headers/"
    export SHLIB_LINK="-L/Library/Frameworks/Firebird.framework/Versions/A/Libraries/"

firebird_fdw is installed as a PostgreSQL extension; it requires the
pg_config binary for the target installation to be in the shell path.

The usual 'make && make install' should take care of the actual compilation.


Usage
-----

**NOTE: these options are provisional and may change**

firebird_fdw accepts the following options:

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
        Turns off pushdowns of WHERE clause elements to Firebird. Increases
        stability at the expense of speed.

Note that while PostgreSQL allows a foreign table to be defined without
any columns, firebird_fdw  will raise an error as soon as any operations
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

Create a foreign table referencing the Firebird table_name 'fdw_test':

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

- UPDATE and DELETE statements use Firebird's row identifier RDB$DB_KEY
  to operate on arbitrary rows
- ANALYZE support
- pushdown of some WHERE clause conditions to Firebird (including translation
  of built-in functions)
- Connection caching


Limitations
-----------

Many; among the more egregious:

- No Firebird transaction support
- No explicit character set/encoding support
- No support for some Firebird datatypes (BLOB, ARRAY)
- TIMESTAMP/TIME: currently sub-second units will be truncated on
  insertion or update


Useful links
------------

* Source
 - https://github.com/ibarwick/firebird_fdw (public mirror)
 - http://pgxn.org/dist/firebird_fdw/

* Documentation
 - http://www.postgresql.org/docs/current/interactive/ddl-foreign-data.html
 - http://www.postgresql.org/docs/current/interactive/sql-createforeigndatawrapper.html
 - http://www.postgresql.org/docs/current/interactive/sql-createforeigntable.html
 - http://www.postgresql.org/docs/current/interactive/fdwhandler.html
 - http://www.postgresql.org/docs/current/interactive/postgres-fdw.html

* Other FDWs
 - https://wiki.postgresql.org/wiki/Fdw
 - http://pgxn.org/tag/fdw/