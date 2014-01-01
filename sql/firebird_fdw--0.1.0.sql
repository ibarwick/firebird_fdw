/*-------------------------------------------------------------------------
 *
 * foreign-data wrapper for Firebird
 *
 * Copyright (c) 2013 Ian Barwick
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Ian Barwick <barwick@sql-info.de>
 *
 * IDENTIFICATION
 *                firebird_fdw/sql/firebird_fdw.sql
 *
 *-------------------------------------------------------------------------
 */

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
