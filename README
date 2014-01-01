Firebird Foreign Data Wrapper for PostgreSQL
============================================

This is an experimental foreign data wrapper (FDW) to connect PostgreSQL
to Firebird. It provides basic functionality, including both read (SELECT)
and write (INSERT/UPDATE/DELETE) support. However it is still very much
work-in-progress; use at your own risk.

firebird_fdw will work with PostgreSQL 9.3 or later (it was developed
against the current development version) and in its current form will not
work with pre-9.3 versions (although it should be simple enough to add
read-only support).

It was written for Firebird 2.5 and may work with earlier versions.


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
        The database to connect to

    'username':
        The username to connect as (not case-sensitive)

    'password':
        The user's password (note that Firebird only recognizes
        the first 8 digits of a password)

    'table':
        The Firebird table name (not case-sensitive).
        Cannot be used together with the 'query' option.

    'query':
        A Firebird SQL statement producing a result set which can be
        treated like a table. Cannot be used together with the 'table'
        option.

Note that while a Firebird foreign table can be defined without any
columns, an error will be raised as soon as any operations are carried
out on it.


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

Create a foreign table referencing the Firebird table 'fdw_test':

    CREATE FOREIGN TABLE fb_test(
      id SMALLINT,
      val VARCHAR(2048)
    )
    SERVER firebird_server
    OPTIONS(
      table 'fdw_test'
    );

Create a foreign table as a Firebird query:

    CREATE FOREIGN TABLE fb_test(
      id SMALLINT,
      val VARCHAR(2048)
    )
    SERVER firebird_server
    OPTIONS(
      query 'SELECT id, val FROM fdw_test'
    );


Limitations
-----------

Many; among the more egregious:
- No Firebird transaction support
- No explicit character set/encoding support
- No support for some Firebird datatypes (BLOB, ARRAY)
- TIMESTAMP/TIME: currently sub-second units will be truncated
- No column aliases possible (column names defined in CREATE FOREIGN TABLE
  must exactly match the Firebird column names)
- No pushdowns
- No connection caching


Useful links
------------

* Source
 - https://github.com/ibarwick/firebird_fdw
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