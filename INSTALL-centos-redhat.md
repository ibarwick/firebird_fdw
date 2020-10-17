Installing firebird_fdw on CentOS/Redhat
========================================

Package installation
--------------------

RPM packages for CentOS/Redhat etc. are available via the Fedora "copr"
build system; for details see here:
<https://copr.fedorainfracloud.org/coprs/ibarwick/firebird_fdw/>

Note that packages are generally only built for PostgreSQL versions
currently under community support.

Source installation
-------------------

### Prerequisites

- `libfq`, a `libpq`-like API wrapper for the Firebird C API; see:

    https://github.com/ibarwick/libfq

  `libfq` packages are available via the Fedora "copr" build system.
  see: <https://copr.fedorainfracloud.org/coprs/ibarwick/firebird_fdw/>

  [Source installation instructions](https://github.com/ibarwick/libfq/blob/master/INSTALL.md)
  are also available.

  *NOTE* the latest `libfq` version should be used with the current
  `firebird_fdw` version, as the two are usually developed in tandem.

- following packages must be installed:
  - `firebird-devel`
  - `libfbclient2`

If PostgreSQL itself is not installed from source, the appropriate
`dev` package is also required:

  - `postgresql{VERSION}-devel`

where `{VERSION}` corresponds to the PostgreSQL version `firebird_fdw`
is being built against (e.g.`12`, `96`).

*IMPORTANT*: you *must* build `firebird_fdw` against the PostgreSQL version
it will be installed on.

#### Building

Ensure the `pg_config` binary for the taregt PostgreSQL version is in
the shell path; then execute:

    `make && sudo make install`

which should build and install `firebird_fdw`.
