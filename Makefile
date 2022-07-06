##############################################################################
#
# Firebird Foreign Data Wrapper for PostgreSQL
#
# Copyright (c) 2013-2021 Ian Barwick
#
# This software is released under the PostgreSQL Licence
#
# Author: Ian Barwick <barwick@gmail.com>
#
# IDENTIFICATION
#        firebird_fdw/Makefile
#
##############################################################################


EXTENSION    = firebird_fdw
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
MODULE_big   = $(EXTENSION)

OBJS         =  $(patsubst %.c,%.o,$(wildcard src/*.c))

ifdef FIREBIRD_FDW_DEBUG_BUILD
DEBUG_BUILD  = 1
else
DEBUG_BUILD  = 0
endif

SHLIB_LINK += -lfq -lfbclient

DATA = sql/firebird_fdw--0.3.0.sql \
	sql/firebird_fdw--0.3.0--0.4.0.sql \
	sql/firebird_fdw--0.4.0.sql \
	sql/firebird_fdw--0.4.0--0.5.0.sql \
	sql/firebird_fdw--0.5.0.sql \
	sql/firebird_fdw--0.5.0--1.0.0.sql \
	sql/firebird_fdw--1.0.0.sql \
	sql/firebird_fdw--1.0.0--1.1.0.sql \
	sql/firebird_fdw--1.1.0.sql \
	sql/firebird_fdw--1.1.0--1.2.0.sql \
	sql/firebird_fdw--1.2.0.sql \
	sql/firebird_fdw--1.2.0--1.3.0.sql \
	sql/firebird_fdw--1.3.0.sql


ifndef PG_CONFIG
PG_CONFIG    = pg_config
endif
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Sanity-check supported version

ifeq (,$(findstring $(MAJORVERSION),9.3 9.4 9.5 9.6 10 11 12 13 14 15 16))
$(error firebird_fdw supports PostgreSQL 9.3 and later)
endif

$(info Building against PostgreSQL $(MAJORVERSION))

# Fix for OS X and libfq
ifeq (-dead_strip_dylibs, $(findstring -dead_strip_dylibs, $(shell $(PG_CONFIG) --ldflags)))
LDFLAGS := $(subst -dead_strip_dylibs,-flat_namespace,$(LDFLAGS))
endif

PG_PROVE_FLAGS += -I $(srcdir)/t

prove_installcheck: all
	rm -rf $(srcdir)/tmp_check/log
	cd $(srcdir) && PG_VERSION_NUM='$(VERSION_NUM)' TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)

installcheck: prove_installcheck

clean: local_clean

local_clean:
	rm -rf tmp_check/

