Summary: A PostgreSQL foreign data wrapper (FDW) for Firebird
Name: postgresql13-firebird_fdw
Version: 1.3.0
Release: 1
Source: firebird_fdw-%{version}.tar.gz
URL: https://github.com/ibarwick/firebird_fdw
License: PostgreSQL
Group: Productivity/Databases/Tools
Packager: Ian Barwick
BuildRequires: postgresql13-devel
BuildRequires: firebird-devel
BuildRequires: libfq
%if 0%{?rhel} && 0%{?rhel} >= 8
BuildRequires: llvm
%else
%if 0%{?rhel} && 0%{?rhel} == 7
BuildRequires: llvm-toolset-7
BuildRequires: llvm5.0
%endif
%endif
BuildRoot: %{_tmppath}/%{name}-%{version}-build
Requires: postgresql13-server libfq

%define pgsql_path /usr/pgsql-13

%description
This is a foreign data wrapper (FDW) to connect PostgreSQL to Firebird.
It provides both read (SELECT) and write (INSERT/UPDATE/DELETE)
support, WHERE-clause pushdowns, connection caching and Firebird transaction
support.

This code is very much work-in-progress; USE AT YOUR OWN RISK.

%prep
%setup

%build
export PG_CONFIG=%{pgsql_path}/bin/pg_config
PG_CPPFLAGS="-I/usr/include/firebird" USE_PGXS=1 make

%install
rm -rf $RPM_BUILD_ROOT
export PG_CONFIG=%{pgsql_path}/bin/pg_config
USE_PGXS=1 make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-, root, root)
%{pgsql_path}/lib/firebird_fdw.so
%{pgsql_path}/share/extension/firebird_fdw--0.3.0.sql
%{pgsql_path}/share/extension/firebird_fdw--0.3.0--0.4.0.sql
%{pgsql_path}/share/extension/firebird_fdw--0.4.0.sql
%{pgsql_path}/share/extension/firebird_fdw--0.4.0--0.5.0.sql
%{pgsql_path}/share/extension/firebird_fdw--0.5.0.sql
%{pgsql_path}/share/extension/firebird_fdw--0.5.0--1.0.0.sql
%{pgsql_path}/share/extension/firebird_fdw--1.0.0.sql
%{pgsql_path}/share/extension/firebird_fdw--1.0.0--1.1.0.sql
%{pgsql_path}/share/extension/firebird_fdw--1.1.0.sql
%{pgsql_path}/share/extension/firebird_fdw--1.1.0--1.2.0.sql
%{pgsql_path}/share/extension/firebird_fdw--1.2.0.sql
%{pgsql_path}/share/extension/firebird_fdw--1.2.0--1.3.0.sql
%{pgsql_path}/share/extension/firebird_fdw--1.3.0.sql
%{pgsql_path}/share/extension/firebird_fdw.control

%if 0%{?rhel} && 0%{?rhel} >= 7
%exclude %{pgsql_path}/lib/bitcode
%endif

%changelog
* Wed Dec 28 2022 Ian Barwick (barwick@gmail.com)
- 1.3.0 release
* Sun Feb 20 2022 Ian Barwick (barwick@gmail.com)
- 1.2.3 release
* Tue Sep 14 2021 Ian Barwick (barwick@gmail.com)
- 1.2.2 release
* Wed Oct 21 2020 Ian Barwick (barwick@gmail.com)
- 1.2.1 release
* Sat Oct 17 2020 Ian Barwick (barwick@gmail.com)
- 1.2.0 release
* Fri May 31 2019 Ian Barwick (barwick@gmail.com)
- 1.1.0 release
* Fri Nov 9 2018 Ian Barwick (barwick@gmail.com)
- 1.0.0 release
* Fri Oct 12 2018 Ian Barwick (barwick@gmail.com)
- 0.5.0 release
* Tue Oct 2 2018 Ian Barwick (barwick@gmail.com)
- 0.4.0 release
* Sun Apr 22 2018 Ian Barwick (barwick@gmail.com)
- 0.3.0 release
* Sun Feb 2 2014 Ian Barwick (barwick@gmail.com)
- First draft
