-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION firebird_fdw" to load this file. \quit

CREATE OR REPLACE FUNCTION firebird_fdw_close_connections()
  RETURNS void
  AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION firebird_fdw_diag(
    OUT name TEXT,
    OUT setting TEXT)
  RETURNS SETOF record
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT VOLATILE PARALLEL SAFE;
