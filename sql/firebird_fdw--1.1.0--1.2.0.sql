-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION firebird_fdw" to load this file. \quit

CREATE FUNCTION firebird_fdw_server_options(
    IN server_name TEXT,
    OUT name TEXT,
    OUT value TEXT,
    OUT provided BOOL)
  RETURNS SETOF record
  AS 'MODULE_PATHNAME'
  LANGUAGE C STRICT VOLATILE PARALLEL SAFE;
