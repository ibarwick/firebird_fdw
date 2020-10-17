-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION firebird_fdw" to load this file. \quit

ALTER FUNCTION firebird_fdw_server_options(
    IN server_name TEXT,
    OUT name TEXT,
    OUT value TEXT,
    OUT provided BOOL
  )
  STABLE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION firebird_fdw_server_options(
    IN server_name TEXT,
    OUT name TEXT,
    OUT value TEXT,
    OUT provided BOOL
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
