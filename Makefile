##############################################################################
#
# Firebird Foreign Data Wrapper for PostgreSQL
#
# Copyright (c) 2013-2018 Ian Barwick
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
#DOCS         = $(wildcard doc/*.md)
USE_MODULE_DB = 1
MODULE_big      = $(EXTENSION)

OBJS         =  $(patsubst %.c,%.o,$(wildcard src/*.c))

ifndef PG_CONFIG
PG_CONFIG    = pg_config
endif

SHLIB_LINK += -lfq -lfbclient

DATA = sql/firebird_fdw--0.3.0.sql \
	sql/firebird_fdw--0.3.0--0.4.0.sql \
	sql/firebird_fdw--0.4.0.sql

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Fix for OS X and libfq
ifeq (-dead_strip_dylibs, $(findstring -dead_strip_dylibs, $(shell $(PG_CONFIG) --ldflags)))
LDFLAGS := $(subst -dead_strip_dylibs,-flat_namespace,$(LDFLAGS))
endif

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

PG_PROVE_FLAGS += -I $(srcdir)/t

prove_installcheck: install
		rm -rf $(CURDIR)/tmp_check/log
		cd $(srcdir) && TESTDIR='$(CURDIR)' PATH="$(bindir):$$PATH" PGPORT='6$(DEF_PGPORT)' PG_REGRESS='$(top_builddir)/src/test/regress/pg_regress' $(PROVE) $(PG_PROVE_FLAGS) $(PROVE_FLAGS) $(if $(PROVE_TESTS),$(PROVE_TESTS),t/*.pl)

installcheck: prove_installcheck

# we put all the tests in a test subdir, but pgxs expects us not to, darn it
override pg_regress_clean_files = test/results/ test/regression.diffs test/regression.out tmp_check/ log/
