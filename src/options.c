/*-------------------------------------------------------------------------
 *
 * options.c
 *
 * Helper functions to validate and parse the FDW options
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "firebird_fdw.h"

/*
 * Valid options for firebird_fdw
 *
 */
static struct FirebirdFdwOption valid_options[] =
{
	/* Connection options */
	{ "address",		   ForeignServerRelationId },
	{ "port",			   ForeignServerRelationId }, /* not implemented (!) */
	{ "database",		   ForeignServerRelationId },
	{ "disable_pushdowns", ForeignServerRelationId },
	/* User options */
	{ "username",		   UserMappingRelationId   },
	{ "password",		   UserMappingRelationId   },
	/* Table options */
	{ "query",			   ForeignTableRelationId  },
	{ "table_name",		   ForeignTableRelationId  },
	{ "updatable",		   ForeignTableRelationId  },
	{ "estimated_row_count", ForeignTableRelationId },
	/* Table column options */
	{ "column_name",	   AttributeRelationId	   },
	{ NULL,				   InvalidOid }
};

extern Datum firebird_fdw_validator(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(firebird_fdw_validator);
static bool firebirdIsValidOption(const char *option, Oid context);

/**
 * firebird_fdw_validator()
 *
 * Validates the options provided in a "CREATE FOREIGN ..." command
 */
Datum
firebird_fdw_validator(PG_FUNCTION_ARGS)
{
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			 catalog = PG_GETARG_OID(1);
	ListCell	*cell;

	char		*svr_address = NULL;
	int			 svr_port = 0;
	char		*svr_username = NULL;
	char		*svr_password = NULL;
	char		*svr_database = NULL;
	char		*svr_query = NULL;
	char		*svr_table = NULL;

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
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
							 opt->optname);
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

			svr_port = atoi(defGetString(def));
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
					errmsg("redundant option: 'disable_pushdowns' set more than once")
					));
			(void) defGetBoolean(def);

			disable_pushdowns_set = true;
		}
		else if (strcmp(def->defname, "updatable") == 0)
		{
			if (updatable_set)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("redundant option: 'updatable' set more than once")));
			(void) defGetBoolean(def);
			updatable_set = true;
		}
	}

	PG_RETURN_VOID();
}



/**
 * firebirdGetOptions()
 *
 * Fetch the options for a firebird_fdw foreign table.
 */
void
firebirdGetOptions(Oid foreigntableid, char **query, char **table, bool *disable_pushdowns, int *estimated_row_count)
{
	ForeignTable  *f_table;
	ListCell	  *lc;

	f_table = GetForeignTable(foreigntableid);

	foreach (lc, f_table->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		elog(DEBUG2, "option: %s", def->defname);
		if (strcmp(def->defname, "query") == 0)
			*query = defGetString(def);

		else if (strcmp(def->defname, "table_name") == 0)
			*table = defGetString(def);

		else if (strcmp(def->defname, "disable_pushdowns") == 0 && disable_pushdowns != NULL)
			*disable_pushdowns = defGetBoolean(def);

		else if (strcmp(def->defname, "estimated_row_count") == 0 && estimated_row_count != NULL)
			*estimated_row_count = strtod(defGetString(def), NULL);
	}

	/*
	 * If no query and no table name specified, default to the PostgreSQL
	 * table name.
	 */
	if (!*table && !*query)
		*table = get_rel_name(foreigntableid);
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
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}

	return false;
}
