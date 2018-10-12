/*-------------------------------------------------------------------------
 *
 * foreign-data wrapper for Firebird
 *
 * Copyright (c) 2013-2018 Ian Barwick
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Ian Barwick <barwick@sql-info.de>
 *
 * IDENTIFICATION
 *                firebird_fdw/sql/firebird_fdw--0.5.0.sql
 *
 *-------------------------------------------------------------------------
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION firebird_fdw" to load this file. \quit

CREATE FUNCTION firebird_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION firebird_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER firebird_fdw
  HANDLER firebird_fdw_handler
  VALIDATOR firebird_fdw_validator;

CREATE OR REPLACE FUNCTION firebird_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;
