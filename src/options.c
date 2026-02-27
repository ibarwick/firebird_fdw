/*-------------------------------------------------------------------------
 *
 * Helper functions to validate and parse the FDW options
 *
 * Copyright (c) 2013-2026 Ian Barwick
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Ian Barwick <barwick@gmail.com>
 *
 * Public repository: https://github.com/ibarwick/firebird_fdw
 *
 * IDENTIFICATION
 *		  firebird_fdw/src/options.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include "firebird_fdw.h"

/*
 * Valid options for firebird_fdw
 *
 */
static struct FirebirdFdwOption valid_options[] =
{
	/* Connection options */
	{ "address",			 ForeignServerRelationId },
	{ "port",				 ForeignServerRelationId },
	{ "database",			 ForeignServerRelationId },
	{ "disable_pushdowns",	 ForeignServerRelationId },
	{ "updatable",			 ForeignServerRelationId },
	{ "quote_identifiers",	 ForeignServerRelationId },
	{ "implicit_bool_type",	 ForeignServerRelationId },
#if (PG_VERSION_NUM >= 140000)
	{ "batch_size",			 ForeignServerRelationId },
	{ "truncatable",		 ForeignServerRelationId },

#endif
	/* User options */
	{ "username",			 UserMappingRelationId	 },
	{ "password",			 UserMappingRelationId	 },
	/* Table options */
	{ "query",				 ForeignTableRelationId	 },
	{ "table_name",			 ForeignTableRelationId	 },
	{ "updatable",			 ForeignTableRelationId	 },
	{ "estimated_row_count", ForeignTableRelationId	 },
	{ "quote_identifier",	 ForeignTableRelationId	 },
#if (PG_VERSION_NUM >= 140000)
	{ "batch_size",			 ForeignTableRelationId  },
	{ "truncatable",		 ForeignTableRelationId  },
#endif
	/* Column options */
	{ "column_name",		 AttributeRelationId	 },
	{ "quote_identifier",	 AttributeRelationId	 },
	{ "implicit_bool_type",	 AttributeRelationId	 },
	{ NULL,					 InvalidOid }
};

extern Datum firebird_fdw_validator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(firebird_fdw_validator);
static bool firebirdIsValidOption(const char *option, Oid context);

/**
 * firebird_fdw_validator()
 *
 * Validates the options provided in a "CREATE FOREIGN ..." command.
 * It does not store the values anywhere.
 */
Datum
firebird_fdw_validator(PG_FUNCTION_ARGS)
{
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			 catalog = PG_GETARG_OID(1);
	ListCell	*cell;

	/*
	 * If an option is specified, store it in one of these variables so
	 * we can determine if it gets specified more than once.
	 */
	char		*svr_address = NULL;
	int			 svr_port = 0;
	char		*svr_username = NULL;
	char		*svr_password = NULL;
	char		*svr_database = NULL;
	char		*svr_query = NULL;
	char		*svr_table = NULL;
#if (PG_VERSION_NUM >= 140000)
	int			svr_batch_size = NO_BATCH_SIZE_SPECIFIED;
	bool		truncatable_set = false;
#endif

	bool		 disable_pushdowns_set = false;
	bool		 updatable_set = false;

	elog(DEBUG2, "entering function %s", __func__);

	/*
	 * Check that only options supported by firebird_fdw,
	 * and allowed for the current object type, are given.
	 */
	foreach (cell, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(cell);

		if (!firebirdIsValidOption(def->defname, catalog))
		{
			struct FirebirdFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
				{
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
				}
			}

			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid option \"%s\"", def->defname),
				 errhint("valid options in this context are: %s", buf.len ? buf.data : "<none>")));

			pfree(buf.data);
		}

		if (strcmp(def->defname, "address") == 0)
		{
			if (svr_address)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: address (%s)", defGetString(def))));

			svr_address = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting or redundant options: port (%s)", defGetString(def))));

			/*
			 * parse_int() accepts a pointer which may be set to a hint message,
			 * but for our use-case the only likely one is a general integer range check,
			 * which is not really exciting for us.
			 */
			if (parse_int(defGetString(def), &svr_port, 0, NULL) == false)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("an error was encountered when parsing the provided \"port\" value")));
			}
			else if (svr_port < 1 || svr_port > 65535)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"port\" must have a value between 1 and 65535")));
			}
		}

		if (strcmp(def->defname, "username") == 0)
		{
			if (svr_username)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: username (%s)", defGetString(def))));

			svr_username = defGetString(def);
		}

		if (strcmp(def->defname, "password") == 0)
		{
			if (svr_password)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: password")));

			svr_password = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			if (svr_database)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: database (%s)", defGetString(def))));

			svr_database = defGetString(def);
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			if (svr_table)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: 'query' cannot be used with 'table_name'")));

			if (svr_query)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: query (%s)", defGetString(def))));

			svr_query = defGetString(def);
		}
		else if (strcmp(def->defname, "table_name") == 0)
		{
			if (svr_query)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting options: table cannot be used with query")
					));

			if (svr_table)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: table (%s)", defGetString(def))));

			svr_table = defGetString(def);
		}
		else if (strcmp(def->defname, "disable_pushdowns") == 0)
		{
			if (disable_pushdowns_set)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("redundant option: 'disable_pushdowns' set more than once")));
			(void) defGetBoolean(def);

			disable_pushdowns_set = true;
		}
		else if (strcmp(def->defname, "updatable") == 0)
		{
			bool updatable;

			if (updatable_set)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant option: 'updatable' set more than once")));

			updatable = defGetBoolean(def);
			updatable_set = true;

			/* "updatable" not relevant for tables defined as queries */
			if (svr_query && updatable == true)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("foreign tables defined with the \"query\" option cannot be set as \"updatable\"")));
		}
#if (PG_VERSION_NUM >= 140000)
		else if (strcmp(def->defname, "batch_size") == 0)
		{
			if (svr_batch_size != NO_BATCH_SIZE_SPECIFIED)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant option: \"batch_size\" set more than once")));

			if (parse_int(defGetString(def), &svr_batch_size, 0, NULL) == false)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("an error was encountered when parsing the provided \"batch_size\" value")));
			}
			else if (svr_batch_size < 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"batch_size\" must have a value of 1 or greater")));
			}
		}
		else if (strcmp(def->defname, "truncatable") == 0)
		{
			if (truncatable_set)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("redundant option: 'truncatable' set more than once")));
			(void) defGetBoolean(def);

			truncatable_set = true;
		}
#endif
	}

	PG_RETURN_VOID();
}


/**
 * firebirdIsValidOption()
 *
 * Check if the provided option is valid.
 * "context" is the Oid of the catalog holding the object the option is for.
 */
static bool
firebirdIsValidOption(const char *option, Oid context)
{
	struct FirebirdFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext)
		{
			if (strcmp(opt->optname, option) == 0)
				return true;
		}
	}

	return false;
}


/**
 * firebirdGetServerOptions()
 *
 * Fetch the requested server-level options, and record whether they
 * were explicitly provided.
 */
void
firebirdGetServerOptions(ForeignServer *server,
						 fbServerOptions *options)
{
	ListCell   *lc;

	foreach (lc, server->options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		elog(DEBUG3, "server option: \"%s\"", def->defname);

		if (options->address.opt.strptr != NULL && strcmp(def->defname, "address") == 0)
		{
			*options->address.opt.strptr = defGetString(def);
			options->address.provided = true;
			continue;
		}

		if (options->port.opt.intptr != NULL && strcmp(def->defname, "port") == 0)
		{
			*options->port.opt.intptr = strtod(defGetString(def), NULL);
			options->port.provided = true;
			continue;
		}

		if (options->database.opt.strptr != NULL && strcmp(def->defname, "database") == 0)
		{
			*options->database.opt.strptr = defGetString(def);
			options->database.provided = true;
			continue;
		}

		if (options->disable_pushdowns.opt.boolptr != NULL && strcmp(def->defname, "disable_pushdowns") == 0)
		{
			*options->disable_pushdowns.opt.boolptr = defGetBoolean(def);
			options->disable_pushdowns.provided = true;
			continue;
		}

		if (options->updatable.opt.boolptr != NULL && strcmp(def->defname, "updatable") == 0)
		{
			*options->updatable.opt.boolptr = defGetBoolean(def);
			options->updatable.provided = true;
			continue;
		}

		if (options->quote_identifiers.opt.boolptr != NULL && strcmp(def->defname, "quote_identifiers") == 0)
		{
			*options->quote_identifiers.opt.boolptr = defGetBoolean(def);
			options->quote_identifiers.provided = true;
			continue;
		}

		if (options->implicit_bool_type.opt.boolptr != NULL && strcmp(def->defname, "implicit_bool_type") == 0 )
		{
			*options->implicit_bool_type.opt.boolptr = defGetBoolean(def);
			options->implicit_bool_type.provided = true;
			continue;
		}
#if (PG_VERSION_NUM >= 140000)
		if (options->batch_size.opt.intptr != NULL && strcmp(def->defname, "batch_size") == 0 )
		{
			*options->batch_size.opt.intptr = strtod(defGetString(def), NULL);
			options->batch_size.provided = true;
			continue;
		}
		if (options->truncatable.opt.boolptr != NULL && strcmp(def->defname, "truncatable") == 0 )
		{
			*options->truncatable.opt.boolptr = defGetBoolean(def);
			options->truncatable.provided = true;
			continue;
		}
#endif
	}
}


/**
 * firebirdGetTableOptions()
 *
 * Fetch the options which apply to a firebird_fdw foreign table.
 *
 * Note that "updatable" is handled in firebirdIsForeignRelUpdatable().
 */
void
firebirdGetTableOptions(ForeignTable *table,
						fbTableOptions *options)
{
	ListCell	  *lc;

	foreach (lc, table->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		elog(DEBUG3, "table option: \"%s\"", def->defname);

		/* table-level options */
		if (options->query.opt.strptr != NULL && strcmp(def->defname, "query") == 0)
		{
			*options->query.opt.strptr = defGetString(def);
			options->query.provided = true;
			continue;
		}

		if (options->table_name.opt.strptr != NULL && strcmp(def->defname, "table_name") == 0)
		{
			*options->table_name.opt.strptr = defGetString(def);
			options->table_name.provided = true;
			continue;
		}

		if (options->updatable.opt.boolptr != NULL && strcmp(def->defname, "updatable") == 0)
		{
			*options->updatable.opt.boolptr = defGetBoolean(def);
			options->updatable.provided = true;
			continue;
		}

		if (options->estimated_row_count.opt.intptr != NULL && strcmp(def->defname, "estimated_row_count") == 0)
		{
			*options->estimated_row_count.opt.intptr = strtod(defGetString(def), NULL);
			options->estimated_row_count.provided = true;
			continue;
		}

		if (options->quote_identifier.opt.boolptr != NULL && strcmp(def->defname, "quote_identifier") == 0 )
		{
			*options->quote_identifier.opt.boolptr = defGetBoolean(def);
			options->quote_identifier.provided = true;
			continue;
		}

#if (PG_VERSION_NUM >= 140000)
		if (options->batch_size.opt.intptr != NULL && strcmp(def->defname, "batch_size") == 0 )
		{
			*options->batch_size.opt.intptr = strtod(defGetString(def), NULL);
			options->batch_size.provided = true;
			continue;
		}

		if (options->truncatable.opt.boolptr != NULL && strcmp(def->defname, "truncatable") == 0 )
		{
			*options->truncatable.opt.boolptr = defGetBoolean(def);
			options->truncatable.provided = true;
			continue;
		}
#endif
	}

	/*
	 * If no query and no table name specified, default to the PostgreSQL
	 * table name.
	 */
	if (options->table_name.opt.strptr != NULL && options->query.opt.strptr != NULL)
	{
		if (!*options->table_name.opt.strptr && !*options->query.opt.strptr)
			*options->table_name.opt.strptr = get_rel_name(table->relid);
	}
}


void
firebirdGetColumnOptions(Oid foreigntableid, int varattno,
						 fbColumnOptions *options)
{
	List	   *options_list;
	ListCell   *lc;

	options_list = GetForeignColumnOptions(foreigntableid, varattno);

	foreach (lc, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (options->column_name != NULL &&	 strcmp(def->defname, "column_name") == 0)
		{
			*options->column_name = defGetString(def);
			continue;
		}

		if (options->quote_identifier != NULL && strcmp(def->defname, "quote_identifier") == 0 )
		{
			*options->quote_identifier = defGetBoolean(def);
			continue;
		}

		if (options->implicit_bool_type != NULL && strcmp(def->defname, "implicit_bool_type") == 0 )
		{
			*options->implicit_bool_type = defGetBoolean(def);
			continue;
		}
	}
}
