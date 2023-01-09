/*-------------------------------------------------------------------------
 *
 * foreign-data wrapper for Firebird
 *
 * Copyright (c) 2013-2023 Ian Barwick
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Ian Barwick <barwick@sql-info.de>
 *
 * IDENTIFICATION
 *                firebird_fdw/sql/firebird_fdw--1.3.0.sql
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
  RETURNS pg_catalog.int4
  AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION firebird_fdw_close_connections()
  RETURNS void
  AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION firebird_fdw_server_options(
    IN server_name TEXT,
    OUT name TEXT,
    OUT value TEXT,
    OUT provided BOOL
  )
  RETURNS SETOF record
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT STABLE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION firebird_fdw_diag(
    OUT name TEXT,
    OUT setting TEXT
  )
  RETURNS SETOF record
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION firebird_version(
    OUT server_name TEXT,
    OUT firebird_version INT,
    OUT firebird_version_string TEXT
  )
  RETURNS SETOF record
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
