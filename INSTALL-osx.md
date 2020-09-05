Installing firebird_fdw on OS X
===============================

Package installation
--------------------

Currently no packages are available for OS X.

Source installation
-------------------

### Prerequisites

- `libfq`, a `libpq`-like API wrapper for the Firebird C API; see:

    https://github.com/ibarwick/libfq

  This will need to be built from source too; see the instructions
  in the `libfq` repository.
  *NOTE* the latest `libfq` version should be used with the current
  `firebird_fdw` version, as the two are usually developed in tandem.

If PostgreSQL itself is not installed from source, the appropriate
`dev` package for the target PostgreSQL version is also required.

*IMPORTANT*: you *must* build `firebird_fdw` against the PostgreSQL version
it will be installed on.

### Building

Following environment variables should be set so that the PostgreSQL build system
can find the required Firebird files:

    export PG_CPPFLAGS="-I /Library/Frameworks/Firebird.framework/Versions/A/Headers/"
    export SHLIB_LINK="-L/Library/Frameworks/Firebird.framework/Versions/A/Libraries/"

Note that particularly on OS X, tthe Firebird include/library files often end up in
non-standard locations; the Firebird utility [fb_config](https://firebirdsql.org/manual/fbscripts-fb-config.html)
can assist with locating them.

Ensure the `pg_config` binary for the taregt PostgreSQL version is in
the shell path; then execute:

    `make && sudo make install`

which should build and install `firebird_fdw`.
