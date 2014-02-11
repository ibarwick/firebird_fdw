Summary: A PostgreSQL foreign data wrapper (FDW) for Firebird
Name: postgresql92-firebird_fdw
Version: 0.2.5
Release: 1
Source: firebird_fdw-%{version}.tar.gz
URL: https://github.com/ibarwick/firebird_fdw
License: PostgreSQL
Group: Productivity/Databases/Tools
Packager: Ian Barwick
BuildRequires: postgresql92-devel
BuildRequires: libfq
BuildRoot: %{_tmppath}/%{name}-%{version}-build
Requires: postgresql92-server libfq

%description
This is an experimental foreign data wrapper (FDW) to connect PostgreSQL
to Firebird. It provides read-only (SELECT) support, WHERE-clause pushdowns,
connection caching and Firebird transaction support. (INSERT/UPDATE/DELETE
support is available with PostgreSQL 9.3 and later).

This code is very much work-in-progress; USE AT YOUR OWN RISK.

%prep
CFLAGS="-I/usr/include/firebird"
%setup

%build

make
%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install%clean
rm -rf $RPM_BUILD_ROOT
%files
%defattr(-, root, root)
/usr/lib/postgresql92/lib64/firebird_fdw.so
/usr/share/postgresql92/extension/firebird_fdw--%{version}.sql
/usr/share/postgresql92/extension/firebird_fdw.control

%changelog
* Tue Feb 11 2014 Ian Barwick (barwick@gmail.com)
- First draft
