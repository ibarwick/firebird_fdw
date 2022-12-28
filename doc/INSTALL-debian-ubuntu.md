Installing firebird_fdw on Debian/Ubuntu
========================================

Package installation
--------------------

Currently no packages are available for Debian/Ubuntu.

Source installation
-------------------

### Prerequisites

- `libfq`, a `libpq`-like API wrapper for the Firebird C API; see:

    https://github.com/ibarwick/libfq

  This will need to be built from source too; see the instructions
  in the `libfq` repository.
  *NOTE* the latest `libfq` version should be used with the current
  `firebird_fdw` version, as the two are usually developed in tandem.

- following packages must be installed:
  - `firebird-dev`
  - `libfbclient2`

If PostgreSQL itself is not installed from source, the appropriate
`dev` package is also required:

  - `postgresql-server-dev-{VERSION}`

where `{VERSION}` corresponds to the PostgreSQL version `firebird_fdw`
is being built against (e.g.`12`, `9.6`).

*IMPORTANT*: you *must* build `firebird_fdw` against the PostgreSQL version
it will be installed on.

### Building

Ensure the `pg_config` binary for the target PostgreSQL version is in
the shell path; then execute:

    `make && sudo make install`

which should build and install `firebird_fdw`.
