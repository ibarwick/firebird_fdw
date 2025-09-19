/*----------------------------------------------------------------------
 *
 * Foreign Data Wrapper for Firebird
 *
 * Copyright (c) 2013-2025 Ian Barwick
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Ian Barwick <barwick@gmail.com>
 *
 * Public repository: https://github.com/ibarwick/firebird_fdw
 *
 * IDENTIFICATION
 *		  firebird_fdw/src/firebird_fdw.c
 *
 *----------------------------------------------------------------------
 */

#include "postgres.h"
#include "libfq.h"

#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#if (PG_VERSION_NUM >= 120000)
#include "access/table.h"
#endif
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#if (PG_VERSION_NUM >= 180000)
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#else
#include "commands/explain.h"
#endif
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#if (PG_VERSION_NUM >= 140000)
#include "optimizer/appendinfo.h"
#endif
#include "optimizer/cost.h"
#if (PG_VERSION_NUM >= 160000)
#include "optimizer/inherit.h"
#endif
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#if (PG_VERSION_NUM >= 120000)
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "parser/parsetree.h"
#include "pgstat.h"
#include "pgtime.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "firebird_fdw.h"

PG_MODULE_MAGIC;


/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * We store various information in ForeignScan.fdw_private to pass it from
 * planner to executor.	 Currently we store:
 *
 * 1) SELECT statement text to be sent to the remote server
 * 2) Integer list of attribute numbers retrieved by the SELECT
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().	For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the remote SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Indicates whether RDB$DB_KEY retrieved by the remote SELECT */
	FdwScanDbKeyUsed
};

/*
 * This enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a firebird_fdw foreign table.
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE (NIL for a DELETE) */
	FdwModifyPrivateTargetAttnums,
	/* Indicate if there's a RETURNING clause */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs
};

/* FDW public functions */

extern Datum firebird_fdw_handler(PG_FUNCTION_ARGS);
extern Datum firebird_fdw_version(PG_FUNCTION_ARGS);
extern Datum firebird_fdw_close_connections(PG_FUNCTION_ARGS);
extern Datum firebird_fdw_server_options(PG_FUNCTION_ARGS);
extern Datum firebird_fdw_diag(PG_FUNCTION_ARGS);
extern Datum firebird_version(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(firebird_fdw_handler);
PG_FUNCTION_INFO_V1(firebird_fdw_version);
PG_FUNCTION_INFO_V1(firebird_fdw_close_connections);
PG_FUNCTION_INFO_V1(firebird_fdw_server_options);
PG_FUNCTION_INFO_V1(firebird_fdw_diag);
PG_FUNCTION_INFO_V1(firebird_version);

extern void _PG_init(void);

/* Callback functions */
static void firebirdGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);

static void firebirdGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);

static ForeignScan *firebirdGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses,
						Plan *outer_plan);

static void firebirdExplainForeignScan(ForeignScanState *node,
							struct ExplainState *es);

static void firebirdBeginForeignScan(ForeignScanState *node,
						  int eflags);

static TupleTableSlot *firebirdIterateForeignScan(ForeignScanState *node);

static void firebirdReScanForeignScan(ForeignScanState *node);

static void firebirdEndForeignScan(ForeignScanState *node);

static int	firebirdIsForeignRelUpdatable(Relation rel);


static bool firebirdAnalyzeForeignTable(Relation relation,
							 AcquireSampleRowsFunc *func,
							 BlockNumber *totalpages);

#if (PG_VERSION_NUM >= 140000)
static void firebirdAddForeignUpdateTargets(PlannerInfo *root,
								 Index rtindex,
								 RangeTblEntry *target_rte,
								 Relation target_relation);
#else
static void firebirdAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation);
#endif

static List *firebirdPlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index);

static void firebirdBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *rinfo,
							List *fdw_private,
							int subplan_index,
							int eflags);

static TupleTableSlot *firebirdExecForeignInsert(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

#if (PG_VERSION_NUM >= 140000)
static int firebirdGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo);
static TupleTableSlot **firebirdExecForeignBatchInsert(EState *estate,
													   ResultRelInfo *resultRelInfo,
													   TupleTableSlot **slots,
													   TupleTableSlot **planSlots,
													   int *numSlots);
#endif


static TupleTableSlot *firebirdExecForeignUpdate(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static TupleTableSlot *firebirdExecForeignDelete(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static void firebirdEndForeignModify(EState *estate,
						  ResultRelInfo *rinfo);

static void firebirdExplainForeignModify(ModifyTableState *mtstate,
							  ResultRelInfo *rinfo,
							  List *fdw_private,
							  int subplan_index,
							  struct ExplainState *es);
#if (PG_VERSION_NUM >= 140000)
static void firebirdExecForeignTruncate(List *rels,
										DropBehavior behavior,
										bool restart_seqs);
#endif

static List *firebirdImportForeignSchema(ImportForeignSchemaStmt *stmt,
										 Oid serverOid);

#if (PG_VERSION_NUM >= 110000)
static void
firebirdBeginForeignInsert(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo);
static void
firebirdEndForeignInsert(EState *estate,
						 ResultRelInfo *resultRelInfo);
#endif

/* Internal functions */

static void exitHook(int code, Datum arg);
static FirebirdFdwState *getFdwState(Oid foreigntableid);

static FirebirdFdwModifyState *
create_foreign_modify(EState *estate,
					  RangeTblEntry *rte,
					  ResultRelInfo *resultRelInfo,
					  CmdType operation,
					  Plan *subplan,
					  char *query,
					  List *target_attrs,
					  bool has_returning,
					  List *retrieved_attrs);


static void firebirdEstimateCosts(PlannerInfo *root, RelOptInfo *baserel,  Oid foreigntableid);

static const char **convert_prep_stmt_params(FirebirdFdwModifyState *fmstate,
											 ItemPointer tupleid,
											 ItemPointer tupleid2,
											 TupleTableSlot *slot);
static const int *get_stmt_param_formats(FirebirdFdwModifyState *fmstate,
						 ItemPointer tupleid,
						 TupleTableSlot *slot);

static HeapTuple
create_tuple_from_result(FBresult *res,
						   int row,
						   Relation rel,
						   AttInMetadata *attinmeta,
						   List *retrieved_attrs,
						   MemoryContext temp_context);

static void
store_returning_result(FirebirdFdwModifyState *fmstate,
					   TupleTableSlot *slot, FBresult *res);

static int
fbAcquireSampleRowsFunc(Relation relation, int elevel,
						HeapTuple *rows, int targrows,
						double *totalrows,
						double *totaldeadrows);
static void
convertResToArray(FBresult *res, int row, char **values);

static void
convertDbKeyValue(char *p, uint32_t *key_ctid_part, uint32_t *key_xmax_part);


static void
extractDbKeyParts(TupleTableSlot *planSlot,
				  FirebirdFdwModifyState *fmstate,
				  Datum *datum_ctid,
				  Datum *datum_oid);

#if (PG_VERSION_NUM < 110000)
/* These functions are not available exernally in Pg10 and earlier */
static int	time2tm(TimeADT time, struct pg_tm *tm, fsec_t *fsec);
static int	timetz2tm(TimeTzADT *time, struct pg_tm *tm, fsec_t *fsec, int *tzp);
#endif

#if (PG_VERSION_NUM >= 140000)
static int get_batch_size_option(Relation rel);
#endif

/**
 * firebird_fdw_version()
 *
 * Return the version number as an integer.
 */
Datum
firebird_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(FIREBIRD_FDW_VERSION);
}

/**
 * firebird_fdw_close_connections()
 *
 * Close all open connections
 */
Datum
firebird_fdw_close_connections(PG_FUNCTION_ARGS)
{
	firebirdCloseConnections(true);
	PG_RETURN_VOID();
}


/**
 * firebird_fdw_server_options()
 *
 * Returns the options provided to "CREATE SERVER".
 *
 * This is mainly useful for diagnostic/testing purposes.
 */
Datum
firebird_fdw_server_options(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	StringInfoData option;

	Datum		values[3];
	bool		nulls[3];

	char	   *address = NULL;
	int			port = FIREBIRD_DEFAULT_PORT;
	char	   *database = NULL;
	bool		updatable = true;
	bool		quote_identifiers = false;
	bool		implicit_bool_type = false;
	bool		disable_pushdowns = false;
#if (PG_VERSION_NUM >= 140000)
	int			batch_size = NO_BATCH_SIZE_SPECIFIED;
	bool		truncatable = true;
#endif

	ForeignServer *server;
	fbServerOptions server_options = fbServerOptions_init;
	const char	*server_name;

	/* check to see if caller supports this function returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	server_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	server = GetForeignServerByName(server_name, false);

	server_options.address.opt.strptr = &address;
	server_options.port.opt.intptr = &port;
	server_options.database.opt.strptr = &database;
	server_options.updatable.opt.boolptr = &updatable;
	server_options.quote_identifiers.opt.boolptr = &quote_identifiers;
	server_options.implicit_bool_type.opt.boolptr = &implicit_bool_type;
	server_options.disable_pushdowns.opt.boolptr = &disable_pushdowns;
#if (PG_VERSION_NUM >= 140000)
	server_options.batch_size.opt.intptr = &batch_size;
	server_options.truncatable.opt.boolptr = &truncatable;
#endif

	firebirdGetServerOptions(
		server,
		&server_options);

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for function's result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* address */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum("address");
	values[1] = CStringGetTextDatum(address);
	values[2] = BoolGetDatum(server_options.address.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/* port */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&option);
	appendStringInfo(&option,
					 "%i", port);

	values[0] = CStringGetTextDatum("port");
	values[1] = CStringGetTextDatum(option.data);
	values[2] = BoolGetDatum(server_options.port.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(option.data);

	/* database */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum("database");
	values[1] = CStringGetTextDatum(database);
	values[2] = BoolGetDatum(server_options.database.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/* updatable */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&option);
	appendStringInfoString(&option,
						   updatable ? "true" : "false");

	values[0] = CStringGetTextDatum("updatable");
	values[1] = CStringGetTextDatum(option.data);
	values[2] = BoolGetDatum(server_options.updatable.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(option.data);

#if (PG_VERSION_NUM >= 140000)
	/* truncatable */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&option);
	appendStringInfoString(&option,
						   truncatable ? "true" : "false");

	values[0] = CStringGetTextDatum("truncatable");
	values[1] = CStringGetTextDatum(option.data);
	values[2] = BoolGetDatum(server_options.truncatable.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(option.data);

	/* batch size */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&option);
	appendStringInfo(&option,
					 "%i", batch_size);

	values[0] = CStringGetTextDatum("batch_size");
	values[1] = CStringGetTextDatum(option.data);
	values[2] = BoolGetDatum(server_options.batch_size.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(option.data);

#endif

	/* quote_identifiers */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&option);
	appendStringInfoString(&option,
						   quote_identifiers ? "true" : "false");

	values[0] = CStringGetTextDatum("quote_identifiers");
	values[1] = CStringGetTextDatum(option.data);
	values[2] = BoolGetDatum(server_options.quote_identifiers.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(option.data);

	/* implicit_bool_type */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&option);
	appendStringInfoString(&option,
						   implicit_bool_type ? "true" : "false");

	values[0] = CStringGetTextDatum("implicit_bool_type");
	values[1] = CStringGetTextDatum(option.data);
	values[2] = BoolGetDatum(server_options.implicit_bool_type.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(option.data);

	/* disable_pushdowns */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&option);
	appendStringInfoString(&option,
						   disable_pushdowns ? "true" : "false");

	values[0] = CStringGetTextDatum("disable_pushdowns");
	values[1] = CStringGetTextDatum(option.data);
	values[2] = BoolGetDatum(server_options.disable_pushdowns.provided);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(option.data);

	return (Datum) 0;
}


/**
 * firebird_fdw_diag()
 *
 * Return diagnostic information
 */
Datum
firebird_fdw_diag(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	StringInfoData setting;

	Datum		values[2];
	bool		nulls[2];

	/* check to see if caller supports this function returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for function's result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* firebird_fdw version */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&setting);
	appendStringInfo(&setting,
					 "%i", FIREBIRD_FDW_VERSION);

	values[0] = CStringGetTextDatum("firebird_fdw_version");
	values[1] = CStringGetTextDatum(setting.data);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(setting.data);

	/* firebird_fdw version string*/
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum("firebird_fdw_version_string");
	values[1] = CStringGetTextDatum(FIREBIRD_FDW_VERSION_STRING);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/* libfq version */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&setting);
	appendStringInfo(&setting,
					 "%i", FQlibVersion());

	values[0] = CStringGetTextDatum("libfq_version");
	values[1] = CStringGetTextDatum(setting.data);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(setting.data);

	/* libfq version string*/
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum("libfq_version_string");
	values[1] = CStringGetTextDatum(FQlibVersionString());

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	/* number of cached connections */
	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&setting);
	appendStringInfo(&setting,
					 "%i", firebirdCachedConnectionsCount());

	values[0] = CStringGetTextDatum("cached_connection_count");
	values[1] = CStringGetTextDatum(setting.data);

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	pfree(setting.data);

	return (Datum) 0;
}



/**
 * firebird_version()
 *
 * Returns version information for the Firebird instances defined
 * as foreign servers
 */
Datum
firebird_version(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	StringInfoData buf;
	int			ret;

	/* check to see if caller supports this function returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));

	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;


	initStringInfo(&buf);
	appendStringInfoString(&buf,
						   "	 SELECT fs.oid, fs.srvname, um.umuser "
						   "	   FROM pg_catalog.pg_foreign_data_wrapper fdw "
						   " INNER JOIN pg_catalog.pg_foreign_server fs "
						   "		 ON fs.srvfdw = fdw.oid "
						   " INNER JOIN pg_catalog.pg_user_mappings um "
						   "			ON um.srvid = fs.oid "
						   "	  WHERE fdw.fdwname = 'firebird_fdw'");

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, buf.data);

	ret = SPI_execute(buf.data, false, 0);

	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
		elog(FATAL, "unable to query foreign data wrapper system catalog data");

	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for function's result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (SPI_processed > 0)
	{
		int i;
		Datum		values[3];
		bool		nulls[3];

		for (i = 0; i < SPI_processed; i++)
		{
			bool	isnull;
			Oid serverid;
			Oid userid;
			FBconn *conn;

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			serverid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
													   SPI_tuptable->tupdesc,
													   1, &isnull));

			userid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													3, &isnull));

			conn = firebirdInstantiateConnection(GetForeignServer(serverid),
												 GetUserMapping(userid, serverid));

			/* server_name */
			values[0] = CStringGetTextDatum(SPI_getvalue(SPI_tuptable->vals[i],
													  SPI_tuptable->tupdesc,
													  2));

			/* firebird_version */
			values[1] = FQserverVersion(conn);

			/* firebird_version_string */
			values[2] = CStringGetTextDatum(FQserverVersionString(conn));

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	SPI_finish();
	PopActiveSnapshot();

#if (PG_VERSION_NUM < 150000)
	pgstat_report_stat(false);
#endif

	pgstat_report_activity(STATE_IDLE, NULL);

	return (Datum) 0;
}


/**
 * firebird_fdw_handler()
 *
 * Entry point for the FDW: designate handlers for each FDW
 * action.
 */
Datum
firebird_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	elog(DEBUG2, "entering function %s", __func__);

	/* scanning functions  */
	fdwroutine->GetForeignRelSize = firebirdGetForeignRelSize;
	fdwroutine->GetForeignPaths = firebirdGetForeignPaths;
	fdwroutine->GetForeignPlan = firebirdGetForeignPlan;
	fdwroutine->ExplainForeignScan = firebirdExplainForeignScan;
	fdwroutine->BeginForeignScan = firebirdBeginForeignScan;
	fdwroutine->IterateForeignScan = firebirdIterateForeignScan;
	fdwroutine->ReScanForeignScan = firebirdReScanForeignScan;
	fdwroutine->EndForeignScan = firebirdEndForeignScan;

	/* support for ANALYZE */
	fdwroutine->AnalyzeForeignTable = firebirdAnalyzeForeignTable;

	/* support for insert / update / delete */
	fdwroutine->IsForeignRelUpdatable = firebirdIsForeignRelUpdatable;
	fdwroutine->AddForeignUpdateTargets = firebirdAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = firebirdPlanForeignModify;
	fdwroutine->BeginForeignModify = firebirdBeginForeignModify;
	fdwroutine->ExecForeignInsert = firebirdExecForeignInsert;
#if (PG_VERSION_NUM >= 140000)
	fdwroutine->GetForeignModifyBatchSize = firebirdGetForeignModifyBatchSize;
	fdwroutine->ExecForeignBatchInsert = firebirdExecForeignBatchInsert;
#endif
	fdwroutine->ExecForeignUpdate = firebirdExecForeignUpdate;
	fdwroutine->ExecForeignDelete = firebirdExecForeignDelete;
	fdwroutine->EndForeignModify = firebirdEndForeignModify;
	fdwroutine->ExplainForeignModify = firebirdExplainForeignModify;

#if (PG_VERSION_NUM >= 140000)
	fdwroutine->ExecForeignTruncate = firebirdExecForeignTruncate;
#endif

	/* support for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = firebirdImportForeignSchema;

#if (PG_VERSION_NUM >= 110000)
	/* Handle COPY */
	fdwroutine->BeginForeignInsert = firebirdBeginForeignInsert;
	fdwroutine->EndForeignInsert = firebirdEndForeignInsert;
#endif

	PG_RETURN_POINTER(fdwroutine);
}


/**
 * _PG_init()
 *
 * Library load-time initalization; sets exitHook() callback for
 * backend shutdown.
 */

void
_PG_init(void)
{
	on_proc_exit(&exitHook, PointerGetDatum(NULL));
}


/**
 * exitHook()
 *
 * Perform any necessary cleanup
 */
void
exitHook(int code, Datum arg)
{
	elog(DEBUG2, "entering function %s", __func__);
	firebirdCloseConnections(false);
}


/**
 * fbSigInt()
 *
 * This is basically the StatementCancelHandler() function from
 * "src/backend/tcop/postgres.c"; for reasons as yet undetermined,
 * if it is not implemented like this, issuing a SIGINT will cause
 * the backend process to exit with a segfault. There may be better
 * ways of handling this, but it seems to work for now.
 */
void
fbSigInt(SIGNAL_ARGS)
{
	int			save_errno = errno;

	elog(DEBUG2, "entering function %s", __func__);
	/*
	 * Don't joggle the elbow of proc_exit
	 */
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		QueryCancelPending = true;
	}

#if (PG_VERSION_NUM >= 90600)
	/* If we're still here, waken anything waiting on the process latch */
	SetLatch(MyLatch);
#endif

	errno = save_errno;
}

/**
 * getFdwState()
 *
 * initialize the FirebirdFdwState struct which gets passed around
 */
FirebirdFdwState *
getFdwState(Oid foreigntableid)
{
	FirebirdFdwState *fdw_state = palloc0(sizeof(FirebirdFdwState));

	ForeignTable  *table = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(table->serverid);

	fbServerOptions server_options = fbServerOptions_init;
	fbTableOptions table_options = fbTableOptions_init;

	elog(DEBUG3, "OID: %u", foreigntableid);

	/* Server-level options which apply to the table */
	fdw_state->disable_pushdowns = false;
	fdw_state->implicit_bool_type = false;

	/* Table-level options */
	fdw_state->svr_query = NULL;
	fdw_state->svr_table = NULL;
	fdw_state->estimated_row_count = -1;
	fdw_state->quote_identifier = false;
#if (PG_VERSION_NUM >= 140000)
	fdw_state->batch_size = 1;
#endif

	/* Retrieve server options */
	server_options.disable_pushdowns.opt.boolptr = &fdw_state->disable_pushdowns;
	server_options.implicit_bool_type.opt.boolptr = &fdw_state->implicit_bool_type;
	server_options.quote_identifiers.opt.boolptr = &fdw_state->quote_identifier;
#if (PG_VERSION_NUM >= 140000)
	server_options.batch_size.opt.intptr = &fdw_state->batch_size;
#endif

	firebirdGetServerOptions(
		server,
		&server_options);

	/*
	 * Retrieve table options; these may override server-level options
	 * retrieved in the previous step.
	 */
	table_options.query.opt.strptr = &fdw_state->svr_query;
	table_options.table_name.opt.strptr = &fdw_state->svr_table;
	table_options.estimated_row_count.opt.intptr = &fdw_state->estimated_row_count;
	table_options.quote_identifier.opt.boolptr = &fdw_state->quote_identifier;
#if (PG_VERSION_NUM >= 140000)
	table_options.batch_size.opt.intptr = &fdw_state->batch_size;
#endif

	firebirdGetTableOptions(
		table,
		&table_options);

	return fdw_state;
}


/**
 * firebirdEstimateCosts()
 *
 * Provide an estimate of the remote query cost.
 *
 * This is currently a very primitive implementation which selects
 * a slightly highter startup cost for non-local databases.
 */
static void
firebirdEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	FirebirdFdwState *fdw_state = (FirebirdFdwState *)baserel->fdw_private;
	ForeignServer *server;
	ForeignTable *table;
	char *svr_address  = NULL;

	fbServerOptions server_options = fbServerOptions_init;

	elog(DEBUG2, "entering function %s", __func__);

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	server_options.address.opt.strptr = &svr_address;
	firebirdGetServerOptions(
		server,
		&server_options);

	/* Set startup cost based on the localness of the database */
	/* XXX TODO:
		- is there an equivalent of socket connections?
		- other way of detecting local-hostedness, incluing IPv6
	*/
	if (svr_address && (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0))
		fdw_state->startup_cost = 10;
	else
		fdw_state->startup_cost = 25;

	fdw_state->total_cost = baserel->rows + fdw_state->startup_cost;
}


/**
 * firebirdGetForeignRelSize()
 *
 * Obtain relation size estimates for the foreign table.
 * Called at the beginning of planning for a query that scans a foreign table.
 *
 * Parameters:
 * (PlannerInfo *)root
 *	  The planner's global information about the query
 *
 * (RelOptInfo *)baserel
 *	  The planner's information about the foreign table
 *
 * (Oid) foreigntableid
 *	  The pg_class OID of the foreign table (provided for convenience)
 *
 * Returns:
 *	   void
 *
 * This function should update baserel->rows to be the expected number of
 * rows returned by the table scan, after accounting for the filtering
 * done by the restriction quals. The initial value of baserel->rows is
 * just a constant default estimate, which should be replaced if at all
 * possible. The function may also choose to update baserel->width if it
 * can compute a better estimate of the average result row width.
 */
static void
firebirdGetForeignRelSize(PlannerInfo *root,
						  RelOptInfo *baserel,
						  Oid foreigntableid)
{
	FirebirdFdwState *fdw_state;
	FBresult *res;
	StringInfoData query;
	ListCell *lc;

	Oid			userid;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;

	elog(DEBUG2, "entering function %s", __func__);

#if (PG_VERSION_NUM >= 160000)
	userid = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
#else
	{
		RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
		userid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
	}
#endif

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	fdw_state = getFdwState(foreigntableid);
	baserel->fdw_private = (void *) fdw_state;

	/* get connection options, connect and get the remote table description */
	fdw_state->conn = firebirdInstantiateConnection(server, user);
	fdw_state->firebird_version = FQserverVersion(fdw_state->conn);

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	identifyRemoteConditions(root,
							 baserel,
							 &fdw_state->remote_conds,
							 &fdw_state->local_conds,
							 fdw_state->disable_pushdowns,
							 fdw_state->firebird_version);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.	These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fdw_state->attrs_used = NULL;

#if (PG_VERSION_NUM >= 90600)
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fdw_state->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
				   &fdw_state->attrs_used);
#endif

	foreach (lc, fdw_state->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		elog(DEBUG1, "local conds");
		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fdw_state->attrs_used);
	}

	/* user has supplied "estimated_row_count" as a table option */
	if (fdw_state->estimated_row_count >= 0)
	{
		elog(DEBUG2, "estimated_row_count: %i", fdw_state->estimated_row_count);
		baserel->rows = fdw_state->estimated_row_count;
	}
	/*
	 * do a brute-force SELECT COUNT(*); Firebird doesn't provide any other
	 * way of estimating table size (see http://www.firebirdfaq.org/faq376/ )
	 */
	else
	{
		initStringInfo(&query);
		if (fdw_state->svr_query)
		{
			appendStringInfo(&query, "SELECT COUNT(*) FROM (%s)", fdw_state->svr_query);
		}
		else
		{
			appendStringInfo(&query,
							 "SELECT COUNT(*) FROM %s",
							 quote_fb_identifier(fdw_state->svr_table, fdw_state->quote_identifier));
		}

		fdw_state->query = pstrdup(query.data);
		pfree(query.data);
		elog(DEBUG1, "%s", fdw_state->query);

		res = FQexec(fdw_state->conn, fdw_state->query);

		if (FQresultStatus(res) != FBRES_TUPLES_OK)
		{
			StringInfoData detail;

			initStringInfo(&detail);
			appendStringInfoString(&detail,
								   FQresultErrorField(res, FB_DIAG_MESSAGE_PRIMARY));

			if (FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL) != NULL)
				appendStringInfo(&detail,
								 ": %s",
								 FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL));

			FQclear(res);

			if (fdw_state->svr_query)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						 errmsg("unable to execute query \"%s\"", fdw_state->svr_query),
						 errdetail("%s", detail.data)));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						 errmsg("unable to establish size of foreign table \"%s\"", fdw_state->svr_table),
						 errdetail("%s", detail.data)));
			}
		}

		if (FQntuples(res) != 1)
		{
			int returned_rows = FQntuples(res);
			FQclear(res);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("query returned unexpected number of rows"),
					 errdetail("%i row(s) returned", returned_rows)));
		}

		baserel->rows = atof(FQgetvalue(res, 0, 0));
		FQclear(res);
		pfree(fdw_state->query);
	}

	baserel->tuples = baserel->rows;
	elog(DEBUG1, "%s: rows estimated at %f", __func__, baserel->rows);
}


/**
 * firebirdGetForeignPaths()
 *
 * Create possible access paths for a scan on a foreign table. This is
 * called during query planning.
 *
 * Parameters:
 *
 * (PlannerInfo *)root
 *	  The planner's global information about the query
 *
 * (RelOptInfo *)baserel
 *	  The planner's information about the foreign table
 *
 * (Oid) foreigntableid
 *	  The pg_class OID of the foreign table (provided for convenience)
 *
 * NOTE: The parameters are the same as for GetForeignRelSize(), which was
 * previously called.
 *
 * Returns:
 *	   void
 *
 * This function must generate at least one access path (ForeignPath node)
 * for a scan on the foreign table and must call add_path to add each such
 * path to baserel->pathlist. It's recommended to use
 * create_foreignscan_path to build the ForeignPath nodes. The function
 * can generate multiple access paths, e.g., a path which has valid
 * pathkeys to represent a pre-sorted result. Each access path must
 * contain cost estimates, and can contain any FDW-private information
 * that is needed to identify the specific scan method intended.
 */

static void
firebirdGetForeignPaths(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid)
{
	FirebirdFdwState *fdw_state = (FirebirdFdwState *)baserel->fdw_private;

	elog(DEBUG2, "entering function %s", __func__);

	/* Estimate costs */
	firebirdEstimateCosts(root, baserel, foreigntableid);

	/* Create a ForeignPath node and add it as only possible path */

#if (PG_VERSION_NUM >= 180000)
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,		/* default pathtarget */
									 baserel->rows,
									 0,			/* disabled nodes */
									 fdw_state->startup_cost,
									 fdw_state->total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NULL,		/* no extra plan */
									 NIL,   /* no fdw_restrictinfo list */
									 NIL));		/* no fdw_private data */
#elif (PG_VERSION_NUM >= 170000)
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,		/* default pathtarget */
									 baserel->rows,
									 fdw_state->startup_cost,
									 fdw_state->total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NULL,		/* no extra plan */
									 NIL,   /* no fdw_restrictinfo list */
									 NIL));		/* no fdw_private data */
#elif (PG_VERSION_NUM >= 90600)
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,		/* default pathtarget */
									 baserel->rows,
									 fdw_state->startup_cost,
									 fdw_state->total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NULL,		/* no extra plan */
									 NIL));		/* no fdw_private data */
#else
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 fdw_state->startup_cost,
									 fdw_state->total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NULL,		/* no extra plan */
									 NIL));		/* no fdw_private data */
#endif
}


/**
 * firebirdGetForeignPlan()
 *
 * Create a ForeignScan plan node from the selected foreign access path.
 * This is called at the end of query planning.
 *
 * Parameters:
 *
 * (PlannerInfo *) root
 *	  The planner's global information about the query
 *
 * (RelOptInfo *) baserel
 *	  The planner's information about the foreign table
 *
 * (Oid) foreigntableid
 *	  The pg_class OID of the foreign table (provided for convenience)
 *
 * (ForeignPath *) best_path
 *	  the selected ForeignPath, previously produced by GetForeignPaths()
 *
 * (List *) tlist
 *	  The target list to be emitted by the plan node
 *
 * (List *) scan_clauses
 *	  The restriction clauses to be enforced by the plan node.
 *
 * Returns:
 *	  ForeignScan *
 */

static ForeignScan *
firebirdGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
{
	Index		scan_relid = baserel->relid;
	FirebirdFdwState *fdw_state = (FirebirdFdwState *)baserel->fdw_private;
	RangeTblEntry *rte;
	StringInfoData sql;
	List	   *fdw_private;
	List	   *local_exprs = NIL;
	List	   *remote_conds = NIL;
	List	   *params_list = NIL;
	List	   *retrieved_attrs;

	bool db_key_used;

	ListCell   *lc;
	elog(DEBUG2, "entering function %s", __func__);
	foreach (lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		elog(DEBUG1, "Processing a scan clause");
		Assert(IsA(rinfo, RestrictInfo));
		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
		{
			elog(DEBUG1, " - 'Tis a pseudoconstant, to be dealt with elsewhere");
			continue;
		}

		if (list_member_ptr(fdw_state->remote_conds, rinfo))
		{
			elog(DEBUG1, " - remote");
			remote_conds = lappend(remote_conds, rinfo);

			elog(DEBUG2, " - remote_conds ? %c", remote_conds ? 'Y' : 'N');
		}
		else if (list_member_ptr(fdw_state->local_conds, rinfo))
		{
			elog(DEBUG1, " - local");
			local_exprs = lappend(local_exprs, rinfo->clause);
		}
		else
		{
			Assert(isFirebirdExpr(root, baserel, rinfo->clause, fdw_state->firebird_version));
			elog(DEBUG1, " - remote, but not a member of fdw_state->remote_conds");
			remote_conds = lappend(remote_conds, rinfo);
		}
	}

	rte = planner_rt_fetch(baserel->relid, root);
	/* Build query */
	initStringInfo(&sql);
	buildSelectSql(&sql, rte, fdw_state, baserel, fdw_state->attrs_used,
				   &retrieved_attrs, &db_key_used);

	if (remote_conds)
		buildWhereClause(&sql, root, baserel, remote_conds, true, &params_list);

	elog(DEBUG2, "db_key_used? %c", db_key_used == true ? 'Y' : 'N');

	/*
	 * Build the fdw_private list which will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	fdw_private = list_make3(makeString(sql.data),
							 retrieved_attrs,
#if (PG_VERSION_NUM >= 150000)
							 makeBoolean(db_key_used));
#else
							 makeInteger(db_key_used));
#endif

/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							fdw_private,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}


/**
 * firebirdExplainForeignScan()
 *
 * Display additional EXPLAIN information; if VERBOSE specified, add Firebird's
 * somewhat rudimentary PLAN output.
 *
 * See also:
 *	 include/commands/explain.h
 */
static void
firebirdExplainForeignScan(ForeignScanState *node,
						   ExplainState *es)
{
	FirebirdFdwScanState *fdw_state = (FirebirdFdwScanState *) node->fdw_state;

	elog(DEBUG2, "entering function %s", __func__);

	ExplainPropertyText("Firebird query", fdw_state->query, es);

	/* Show the Firebird "PLAN" information" in VERBOSE mode */
	if (es->verbose)
	{
		char *plan = FQexplainStatement(fdw_state->conn, fdw_state->query);

		if (plan != NULL)
		{
			ExplainPropertyText("Firebird plan", plan, es);
			free(plan);
		}
		else
			ExplainPropertyText("Firebird plan", "no plan available", es);
	}
}


/**
 * firebirdBeginForeignScan()
 *
 * Begin executing a foreign scan; called during executor startup.
 *
 * Performs any initialization needed for firebirdIterateForeignScan().
 * The ForeignScanState node is already created, but its fdw_state field
 * is still NULL. Information about the table to scan is accessible through the
 * ForeignScanState node (in particular, from the underlying ForeignScan
 * plan node, which contains any FDW-private information provided by
 * firebirdGetForeignPlan()). eflags contains flag bits describing the
 * executor's operating mode for this plan node.
 *
 * This function (re)establishes a connection to the remote database (we
 * shouldn't really be doing that here, ideally the connection would
 * be cached already but this is still experimental code); initialises
 * the node's fdw_state fields; and generates the query to be used for
 * the scan.
 *
 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
 * should not perform any externally-visible actions; it should only do
 * the minimum required to make the node state valid for
 * firebirdExplainForeignScan() and firebirdEndForeignScan().
 *
 */
static void
firebirdBeginForeignScan(ForeignScanState *node,
						 int eflags)
{
	char	*svr_query = NULL;
	char	*svr_table = NULL;

	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	FirebirdFdwScanState *fdw_state;
	Oid		 foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);

	Relation rel;
	TupleDesc tupdesc;
	int i;

	EState	   *estate = node->ss.ps.state;
	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;

	ListCell *lc;
	fbTableOptions table_options = fbTableOptions_init;

	elog(DEBUG2, "entering function %s", __func__);

	rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);
#if (PG_VERSION_NUM >= 160000)
	userid = OidIsValid(fsplan->checkAsUser) ? fsplan->checkAsUser : GetUserId();
#else
	userid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
#endif

	table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* needed for svr_query */
	table_options.query.opt.strptr = &svr_query;
	table_options.table_name.opt.strptr = &svr_table;

	firebirdGetTableOptions(table, &table_options);

	/* Initialise FDW state */
	fdw_state = (FirebirdFdwScanState *) palloc0(sizeof(FirebirdFdwScanState));
	node->fdw_state = (void *) fdw_state;

	fdw_state->conn = firebirdInstantiateConnection(server, user);

	fdw_state->row = 0;
	fdw_state->result = NULL;

	/* Get information about table */

	fdw_state->table = (fbTable *) palloc0(sizeof(fbTable));

	fdw_state->table->foreigntableid = foreigntableid;

	fdw_state->table->pg_table_name = get_rel_name(foreigntableid);
	elog(DEBUG2, "Pg tablename: %s", fdw_state->table->pg_table_name);

	/* Get column information */

	rel = table_open(rte->relid, NoLock);

	tupdesc = rel->rd_att;
	fdw_state->table->pg_column_total = 0;
	fdw_state->table->columns = (fbTableColumn **)palloc0(sizeof(fbTableColumn *) * tupdesc->natts);

	for (i = 0; i < tupdesc->natts; i++)
	{
#if (PG_VERSION_NUM >= 110000)
		Form_pg_attribute att_tuple = TupleDescAttr(tupdesc, i);
#else
		Form_pg_attribute att_tuple = tupdesc->attrs[i];
#endif

		fdw_state->table->columns[fdw_state->table->pg_column_total] = (fbTableColumn *)palloc0(sizeof(fbTableColumn));

		fdw_state->table->columns[fdw_state->table->pg_column_total]->isdropped = att_tuple->attisdropped
			? true
			: false;
		fdw_state->table->columns[fdw_state->table->pg_column_total]->used = false;

		fdw_state->table->pg_column_total++;
	}

	table_close(rel, NoLock);

	/* Check if table definition contains at least one column */
	if (!fdw_state->table->pg_column_total)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("no column definitions provided for foreign table %s",
						fdw_state->table->pg_table_name)));
	}

	/* Construct query */

	if (svr_query)
	{
		fdw_state->db_key_used = false;
	}
	else
	{
#if (PG_VERSION_NUM >= 150000)
		fdw_state->db_key_used = boolVal(list_nth(fsplan->fdw_private,
													   FdwScanDbKeyUsed));
#else
		fdw_state->db_key_used = (bool)intVal(list_nth(fsplan->fdw_private,
													   FdwScanDbKeyUsed));
#endif
	}

	fdw_state->query = strVal(list_nth(fsplan->fdw_private,
									   FdwScanPrivateSelectSql));

	fdw_state->retrieved_attrs = (List *) list_nth(fsplan->fdw_private,
												   FdwScanPrivateRetrievedAttrs);

	/* Mark columns used in the query */
	foreach (lc, fdw_state->retrieved_attrs)
	{
		int attnum = lfirst_int(lc);

		if (attnum < 0)
			continue;
		elog(DEBUG2, "attnum %i used", attnum);
		fdw_state->table->columns[attnum - 1]->used = true;
	}

	elog(DEBUG2, "leaving function %s", __func__);
}


/**
 * firebirdIterateForeignScan()
 *
 * Fetches a single row from the foreign table, returned in the node's
 * ScanTupleSlot. Returns NULL if no more rows are available.
 *
 * The tuple table slot infrastructure
 * allows either a physical or virtual tuple to be returned; in most cases
 * the latter choice is preferable from a performance standpoint. Note
 * that this is called in a short-lived memory context that will be reset
 * between invocations. Create a memory context in BeginForeignScan if you
 * need longer-lived storage, or use the es_query_cxt of the node's
 * EState.
 *
 * The rows returned must match the column signature of the foreign table
 * being scanned. If you choose to optimize away fetching columns that are
 * not needed, you should insert nulls in those column positions.
 *
 * Note that PostgreSQL's executor doesn't care whether the rows returned
 * violate any NOT NULL constraints that were defined on the foreign table
 * columns — but the planner does care, and may optimize queries
 * incorrectly if NULL values are present in a column declared not to
 * contain them. If a NULL value is encountered when the user has declared
 * that none should be present, it may be appropriate to raise an error
 * (just as you would need to do in the case of a data type mismatch).
 */
static TupleTableSlot *
firebirdIterateForeignScan(ForeignScanState *node)
{
	FirebirdFdwScanState *fdw_state = (FirebirdFdwScanState *) node->fdw_state;
	TupleTableSlot	 *slot = node->ss.ss_ScanTupleSlot;

	char			**values;
	HeapTuple		  tuple;
	AttInMetadata	 *attinmeta;
	TupleDesc		  tupledesc;

	int row_total	= 0;
	int field_nr	= 0;
	int pg_field_nr = 0;
	int pg_column_total = 0;
	int field_total = 0;
	int last_field = 0;

	uint32_t key_ctid_part = 0;
	uint32_t key_xmax_part	= 0;

	elog(DEBUG2, "entering function %s", __func__);

	/* execute query, if this is the first run */
	if (!fdw_state->result)
	{
		elog(DEBUG1, "remote query:\n%s", fdw_state->query);

		fdw_state->result = FQexec(fdw_state->conn, fdw_state->query);

		elog(DEBUG1, "query result: %s", FQresStatus(FQresultStatus(fdw_state->result)));

		if (FQresultStatus(fdw_state->result) != FBRES_TUPLES_OK)
		{
			fbfdw_report_error(ERROR, ERRCODE_FDW_ERROR,
							   fdw_state->result,
							   fdw_state->conn,
							   fdw_state->query);
		}
	}

	row_total = FQntuples(fdw_state->result);

	ExecClearTuple(slot);

	/* The FDW API requires that we return NULL if no more rows are available */
	if (fdw_state->row == row_total)
	{
		elog(DEBUG2, "%s: no more rows available (%i fetched)", __func__, row_total);
		return NULL;
	}

	tupledesc = node->ss.ss_currentRelation->rd_att;
	elog(DEBUG2, "tuple has %i atts", tupledesc->natts);

	/* include/funcapi.h */
	attinmeta = TupleDescGetAttInMetadata(tupledesc);

	last_field = field_total = FQnfields(fdw_state->result);

	if (fdw_state->db_key_used == true)
		field_total--;

	pg_column_total = fdw_state->table->pg_column_total;

	/* Build the tuple */
	values = (char **) palloc0(sizeof(char *) * pg_column_total);
	elog(DEBUG2, " pg_column_total %i", pg_column_total);

	for (pg_field_nr = field_nr = 0; pg_field_nr < pg_column_total; pg_field_nr++)
	{
		/* Ignore dropped columns */
		if (fdw_state->table->columns[pg_field_nr]->isdropped == true)
		{
			values[pg_field_nr] = NULL;
			continue;
		}

		/* Ignore columns not used in the query */
		if (fdw_state->table->columns[pg_field_nr]->used == false)
		{
			elog(DEBUG2, " pg_column %i not used", pg_field_nr);
			values[pg_field_nr] = NULL;
			continue;
		}

		/* All result columns retrieved */
		if (field_nr >= field_total)
		{
			values[pg_field_nr] = NULL;
			continue;
		}

		if (FQgetisnull(fdw_state->result, fdw_state->row, field_nr))
		{
			elog(DEBUG2, " retrieved value (%i): NULL", pg_field_nr);
			values[pg_field_nr] = NULL;
		}
		else
		{
			values[pg_field_nr] = FQgetvalue(fdw_state->result, fdw_state->row, field_nr);
			elog(DEBUG2, " retrieved value (%i): %s", pg_field_nr, values[pg_field_nr]);
		}

		field_nr++;
	}

	if (fdw_state->db_key_used)
	{
		/* Final field contains the RDB$DB_KEY value - split into two
		 * uint64 values
		 */
		convertDbKeyValue(
			FQgetvalue(fdw_state->result, fdw_state->row, last_field - 1),
			&key_ctid_part,
			&key_xmax_part);

	}

	tuple = BuildTupleFromCStrings(
		attinmeta,
		values);

	pfree(values);

	if (fdw_state->db_key_used)
	{
		/* Store the  */
		tuple->t_self.ip_blkid.bi_hi = (uint16) (key_ctid_part >> 16);
		tuple->t_self.ip_blkid.bi_lo = (uint16) key_ctid_part;

		tuple->t_data->t_choice.t_heap.t_xmax = (TransactionId)key_xmax_part;
	}

#if (PG_VERSION_NUM >= 120000)
	ExecStoreHeapTuple(tuple, slot, false);
#else
	ExecStoreTuple(tuple, slot, InvalidBuffer, false);
#endif
	fdw_state->row++;

	elog(DEBUG2, "leaving function %s", __func__);

	return slot;
}


/**
 * convertDbKeyValue()
 *
 * Split the 8-byte RDB$DB_KEY value into two unsigned 32 bit integers
 *
 * Trivial note: from a Firebird point of view it would be more logical
 * to pass the first four bytes of the RDB$DB_KEY value as the XMAX, and
 * the last four bytes as the CTID, as RDB$DB_KEY appears to be
 * formatted as a table / row identifier, but that's a purely academic
 * point.
 */

void
convertDbKeyValue(char *p, uint32_t *key_ctid_part, uint32_t *key_xmax_part)
{
	unsigned char *t;
	uint64_t db_key = 0;

	for (t = (unsigned char *) p;  t < (unsigned char *) p + 8; t++)
	{
		db_key += (uint8_t)*t;

		if (t < (unsigned char *) p + 7)
			db_key = db_key << 8;
	}

	*key_ctid_part = (uint32_t) (db_key >> 32);
	*key_xmax_part = (uint32_t) db_key;
}


/**
 * firebirdReScanForeignScan()
 *
 * Restart the scan from the beginning. Note that any parameters the scan
 * depends on may have changed value, so the new scan does not necessarily
 * return exactly the same rows.
 */
static void
firebirdReScanForeignScan(ForeignScanState *node)
{
	FirebirdFdwScanState *fdw_state = (FirebirdFdwScanState *) node->fdw_state;

	elog(DEBUG2, "entering function %s", __func__);

	/* Clean up current query */

	if (fdw_state->result)
	{
		FQclear(fdw_state->result);
		fdw_state->result = NULL;
	}

	/* Begin new query */
	fdw_state->row = 0;
}


/**
 * firebirdEndForeignScan()
 *
 * End the scan and release external resources
 */
static void
firebirdEndForeignScan(ForeignScanState *node)
{
	FirebirdFdwScanState *fdw_state = (FirebirdFdwScanState *) node->fdw_state;

	elog(DEBUG2, "entering function %s", __func__);

	if (fdw_state->result)
	{
		FQclear(fdw_state->result);
		fdw_state->result = NULL;
	}

	elog(DEBUG2, "leaving function %s", __func__);
}


/**
 * firebirdsIsForeignRelUpdatable()
 *
 * Determines whether a foreign table supports INSERT, UPDATE and/or
 * DELETE operations.
 */
static int
firebirdIsForeignRelUpdatable(Relation rel)
{
	ForeignServer *server;
	ForeignTable  *table;
	bool		   updatable = true;
	fbServerOptions server_options = fbServerOptions_init;
	fbTableOptions table_options = fbTableOptions_init;

	elog(DEBUG2, "entering function %s", __func__);

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);

	/* Get server setting, if available */

	server_options.updatable.opt.boolptr = &updatable;

	firebirdGetServerOptions(
		server,
		&server_options);

	/* Table setting overrides server setting */

	table_options.updatable.opt.boolptr = &updatable;

	firebirdGetTableOptions(
		table,
		&table_options);

	elog(DEBUG2, "exiting function %s", __func__);

	return updatable ?
		(1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE) : 0;
}


/**
 * firebirdAddForeignUpdateTargets()
 *
 * Add two fake target columns - 'db_key_ctidpart' and 'db_key_xmaxpart' -
 * which we will use to smuggle Firebird's 8-byte RDB$DB_KEY row identifier
 * in the PostgreSQL tuple header. The fake columns are marked resjunk = true.
 *
 * This identifier is required so that rows previously fetched by the
 * table-scanning functions can be identified unambiguously for UPDATE
 * and DELETE operations.
 *
 * This is a bit of a hack, as it seems it's currently impossible to add
 * an arbitrary column as a resjunk column, despite what the documentation
 * implies.
 *
 * See:
 *	 - https://www.postgresql.org/message-id/flat/A737B7A37273E048B164557ADEF4A58B53860913%40ntex2010i.host.magwien.gv.at
 *	 - https://www.postgresql.org/message-id/flat/0389EF2F-BF41-4925-A5EB-1E9CF28CC171%40postgrespro.ru
 *	 - https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-UPDATE
 *
 * Note: in previous firebird_fdw releases, the tuple header OID was used
 * together with the CTID, however from PostgreSQL 12 this is no longer possible.
 *
 * Parameters:
 * (Query *)parsetree
 *	   The parse tree for the UPDATE or DELETE command
 *
 * (RangeTblEntry *)
 * (Relation)
 *	   These describe the target foreign table
 *
 * Returns:
 *	  void
 */
static void
#if (PG_VERSION_NUM >= 140000)
firebirdAddForeignUpdateTargets(PlannerInfo *root,
								Index rtindex,
								RangeTblEntry *target_rte,
								Relation target_relation)

#else
firebirdAddForeignUpdateTargets(Query *parsetree,
								RangeTblEntry *target_rte,
								Relation target_relation)
#endif
{
	Var		   *var_ctidjunk;
	Var		   *var_xmaxjunk;

	const char *attrname_ctid = "db_key_ctidpart";
	const char *attrname_xmax = "db_key_xmaxpart";

#if (PG_VERSION_NUM < 140000)
	TargetEntry *tle;
#endif /* (PG_VERSION_NUM < 140000) */

	/* This is the XMAX header column */
#if (PG_VERSION_NUM >= 140000)
	var_xmaxjunk = makeVar(rtindex,
						   MaxTransactionIdAttributeNumber,
						   INT4OID,
						   -1,
						   InvalidOid,
						   0);
	add_row_identity_var(root, var_xmaxjunk, rtindex, attrname_xmax);
#else
	var_xmaxjunk = makeVar(parsetree->resultRelation,
						   MaxTransactionIdAttributeNumber,
						   INT4OID,
						   -1,
						   InvalidOid,
						   0);

	tle = makeTargetEntry((Expr *) var_xmaxjunk,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname_xmax),
						  true);

	parsetree->targetList = lappend(parsetree->targetList, tle);
#endif	/* (PG_VERSION_NUM >= 140000) */

	/* This is the CTID attribute, which we are abusing to pass half the RDB$DB_KEY value */
#if (PG_VERSION_NUM >= 140000)
	var_ctidjunk = makeVar(rtindex,
						   SelfItemPointerAttributeNumber,
						   TIDOID,
						   -1,
						   InvalidOid,
						   0);
	add_row_identity_var(root, var_ctidjunk, rtindex, attrname_ctid);
#else
	var_ctidjunk = makeVar(parsetree->resultRelation,
						   SelfItemPointerAttributeNumber,
						   TIDOID,
						   -1,
						   InvalidOid,
						   0);

	tle = makeTargetEntry((Expr *) var_ctidjunk,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname_ctid),
						  true);

	parsetree->targetList = lappend(parsetree->targetList, tle);
#endif
}


/**
 * firebirdPlanForeignModify()
 *
 * Perform any additional planning actions needed for an insert, update,
 * or delete on a foreign table. This function generates the FDW-private
 * information that will be attached to the ModifyTable plan node that
 * performs the update action. This private information must have the form
 * of a List, and will be delivered to BeginForeignModify during the
 * execution stage.
 *
 * Parameters:
 * (PlannerInfo *) root
 *	  The planner's global information about the query
 *
 * (ModifyTable *) plan
 *	  The ModifyTable plan node, which is complete except for the
 *	  fdwPrivLists field generated in this function.
 *
 * (Index) resultRelation
 *	  Identifies the target foreign table by its rangetable index
 *
 * (int) subplan_index
 *	  Identifies which target of the ModifyTable plan node this is,
 *	  counting from zero. This can be used for indexing into plan->plans
 *	  or another substructure of the plan node.
 */
static List *
firebirdPlanForeignModify(PlannerInfo *root,
						  ModifyTable *plan,
						  Index resultRelation,
						  int subplan_index)
{
	/* include/nodes/nodes.h */
	CmdType		operation = plan->operation;

	/* include/nodes/parsenodes.h */
	/* "A range table is a List of RangeTblEntry nodes.*/

	/* planner_rt_fetch(): include/nodes/relation.h
	   -> macro include/parser/parsetree.h
	 */

	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	StringInfoData sql;

	Oid relid;
	FirebirdFdwState *fdw_state;

	List	   *targetAttrs = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;

	elog(DEBUG2, "entering function %s", __func__);

	/*
	 * INSERT ... ON CONFLICT is not supported as there's no equivalent
	 * in Firebird, and a workaround would be complex and possibly unreliable.
	 * Speculatively trying to insert the row would mess up transaction
	 * handling if it fails.
	 */
	if (plan->onConflictAction != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("INSERT with ON CONFLICT clause is not supported")));

	elog(DEBUG2, "RTE rtekind: %i; operation %i", rte->rtekind, operation);

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */

	rel = table_open(rte->relid, NoLock);

	relid = RelationGetRelid(rel);
	fdw_state = getFdwState(relid);

	if (fdw_state->svr_table == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("unable to modify a foreign table defined as a query")));

	/*
	 * Determine which columns to transmit.
	 */
	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		/*
		 * For an INSERT, or UPDATE on a foreign table with BEFORE ROW UPDATE
		 * triggers, transmit all columns.
		 *
		 * With an INSERT, it's necessary to transmit all columns to ensure
		 * any default values on columns not contained in the source statement
		 * are sent.
		 *
		 * With an UPDATE where a BEFORE ROW UPDATE trigger is present, it's
		 * possible the trigger might modify columns not contained in the source
		 * statement.
		 */

		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		elog(DEBUG2, " * operation is INSERT");

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
#if (PG_VERSION_NUM >= 110000)
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
#else
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];
#endif

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)

	{
		/*
		 * With an UPDATE, where no BEFORE ROW UPDATE triggers are present, send
		 * only the columns contained in the source query, to avoid sending
		 * data which won't be used anyway.
		 */
#if (PG_VERSION_NUM >= 160000)
		RelOptInfo *rel = find_base_rel(root, resultRelation);
		Bitmapset  *tmpset =  get_rel_all_updated_cols(root, rel);
#elif (PG_VERSION_NUM >= 120000)
		Bitmapset  *tmpset = bms_union(rte->updatedCols, rte->extraUpdatedCols);
#else
		Bitmapset  *tmpset = bms_copy(rte->updatedCols);
#endif
		int			attidx = -1;

		elog(DEBUG2, " * operation is UPDATE");

		while ((attidx = bms_next_member(tmpset, attidx)) >= 0)
		{
			/* include/access/sysattr.h:#define FirstLowInvalidHeapAttributeNumber (-8) */
			AttrNumber	col = attidx + FirstLowInvalidHeapAttributeNumber;

			/* include/access/attnum.h:#define InvalidAttrNumber   0 */

			if (col <= InvalidAttrNumber)		/* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, col);
		}
	}


	/* Extract the relevant RETURNING list, if any */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

	/* Construct the SQL command string */
	switch (operation)
	{
		case CMD_INSERT:
			buildInsertSql(&sql, rte, fdw_state, resultRelation, rel,
						   targetAttrs, returningList,
						   &retrieved_attrs);
			break;

		case CMD_UPDATE:
			buildUpdateSql(&sql, rte, fdw_state, resultRelation, rel,
						   targetAttrs, returningList,
						   &retrieved_attrs);
			break;

		case CMD_DELETE:
			buildDeleteSql(&sql, rte, fdw_state, resultRelation, rel,
						   returningList,
						   &retrieved_attrs);
			break;

		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	table_close(rel, NoLock);

	elog(DEBUG2, "Constructed the SQL command string");

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */

	return list_make4(makeString(sql.data),
					  targetAttrs,
#if (PG_VERSION_NUM >= 150000)
					  makeBoolean((returningList != NIL)),
#else
					  makeInteger((returningList != NIL)),
#endif
					  retrieved_attrs);
}


/**
 * create_foreign_modify()
 *
 */

static FirebirdFdwModifyState *
create_foreign_modify(EState *estate,
					  RangeTblEntry *rte,
					  ResultRelInfo *resultRelInfo,
					  CmdType operation,
					  Plan *subplan,
					  char *query,
					  List *target_attrs,
					  bool has_returning,
					  List *retrieved_attrs)
{
	FirebirdFdwModifyState *fmstate;

	Relation rel  = resultRelInfo->ri_RelationDesc;
	TupleDesc	tupdesc = RelationGetDescr(rel);

	Oid			userid;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;

	Oid			typefnoid;
	bool		isvarlena;

	AttrNumber	n_params;
	ListCell   *lc;


	/* Begin constructing FirebirdFdwModifyState. */
	fmstate = (FirebirdFdwModifyState *) palloc0(sizeof(FirebirdFdwModifyState));

	fmstate->rel = rel;

#if (PG_VERSION_NUM >= 160000)
	userid = ExecGetResultRelCheckAsUser(resultRelInfo, estate);
#else
	userid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
#endif

	elog(DEBUG2, "userid resolved to: %i", (int)userid);

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	fmstate->conn = firebirdInstantiateConnection(server, user);

	if (FQstatus(fmstate->conn) != CONNECTION_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("unable to connect to foreign server")));

	fmstate->conn->autocommit = true;
	fmstate->conn->client_min_messages = DEBUG1;

	fmstate->firebird_version = FQserverVersion(fmstate->conn);

	fmstate->query = query;
	fmstate->target_attrs = target_attrs;
	fmstate->has_returning = has_returning;
	fmstate->retrieved_attrs = retrieved_attrs;


	/* Create context for per-tuple temp workspace */
#if (PG_VERSION_NUM >= 110000)
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "firebird_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);
#else
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "firebird_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);
#endif

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->attinmeta = TupleDescGetAttInMetadata(tupdesc);

	/* Prepare for output conversion of parameters used in prepared stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	elog(DEBUG2, "n_params is: %i", n_params);
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		/* Set up for remaining transmittable parameters */
		foreach (lc, fmstate->target_attrs)
		{
			int				  attnum = lfirst_int(lc);
#if (PG_VERSION_NUM >= 110000)
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
#else
			Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];
#endif

			elog(DEBUG2, "ins/upd: attr %i, p_nums %i", attnum, fmstate->p_nums);
			Assert(!attr->attisdropped);

#ifdef HAVE_GENERATED_COLUMNS
			/* Ignore generated columns - these will not be transmitted to Firebird */
			if (attr->attgenerated)
				continue;
#endif

			getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);

			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
			fmstate->p_nums++;
		}
	}

	/*
	 * It's possible a top level UPDATE query is being executed which moves
	 * a tuple from a local to a foreign partition; in that case the resulting
	 * FDW-level action will actually be an INSERT, and we won't have a subplan.
	 */

	if (subplan && (operation == CMD_UPDATE || operation == CMD_DELETE))
	{
		/* Here we locate the resjunk columns containing the two
		   halves of the 8-byte RDB$DB_KEY value so update and delete
		   operations can locate the correct row
		 */
		fmstate->db_keyAttno_CtidPart = ExecFindJunkAttributeInTlist(
			subplan->targetlist,
			"db_key_ctidpart");

		if (!AttributeNumberIsValid(fmstate->db_keyAttno_CtidPart))
		{
			elog(ERROR, "Resjunk column \"db_key_ctidpart\" not found");
		}

		elog(DEBUG2, "Found resjunk db_key_ctidpart, attno %i", (int)fmstate->db_keyAttno_CtidPart);


		fmstate->db_keyAttno_XmaxPart = ExecFindJunkAttributeInTlist(
			subplan->targetlist,
			"db_key_xmaxpart");

		if (!AttributeNumberIsValid(fmstate->db_keyAttno_XmaxPart))
		{
			elog(ERROR, "Resjunk column \"db_key_xmaxpart\" not found");
		}

		elog(DEBUG2, "Found resjunk \"db_key_xmaxpart\", attno %i", (int)fmstate->db_keyAttno_XmaxPart);

		getTypeOutputInfo(OIDOID, &typefnoid, &isvarlena);

		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}

#if (PG_VERSION_NUM >= 140000)
	/* Set batch_size from foreign server/table options. */
	if (operation == CMD_INSERT)
		fmstate->batch_size = get_batch_size_option(rel);
#endif

	elog(DEBUG2, "	p_nums %i; n_params: %i", fmstate->p_nums, n_params);
	Assert(fmstate->p_nums <= n_params);

	return fmstate;
}


/**
 * firebirdBeginForeignModify()
 *
 * Preparation for executing a foreign table modification operation.
 * Called during executor startup. One of ExecForeignInsert(),
 * ExecForeignUpdate() or ExecForeignDelete() will subsequently be called
 * for each tuple to be processed.
 *
 * Parameters:
 * (ModifyTableState *) mtstate
 *	  overall state of the ModifyTable plan node being executed;
 *	  provides global data about the plan and execution state
 *
 * (ResultRelInfo) *resultRelInfo
 *	  The ResultRelInfo struct describing the  target foreign table.
 *	  The ri_FdwState field of ResultRelInfo can be used to store
 *	  the FDW's private state.
 *
 * (List *)fdw_private
 *	  contains private data generated by firebirdPlanForeignModify(), if any.
 *
 * (int) subplan_index
 *	  identifies which target of the ModifyTable plan node this is.
 *
 * (int) eflags
 *	  contains flag bits describing the executor's operating mode for
 *	  this plan node. See also comment about (eflags & EXEC_FLAG_EXPLAIN_ONLY)
 *	  in function body.
 *
 * Returns:
 *	   void
 */

static void
firebirdBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags)
{
	FirebirdFdwModifyState *fmstate;
	RangeTblEntry *rte;
	bool		has_returning;

	CmdType		operation = mtstate->operation;

	elog(DEBUG2, "entering function %s", __func__);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.
	 * resultRelInfo->ri_FdwState stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Find RTE. */
#if (PG_VERSION_NUM >= 120000)
	rte = exec_rt_fetch(resultRelInfo->ri_RangeTableIndex,
						mtstate->ps.state);
#else
	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex,
				   mtstate->ps.state->es_range_table);
#endif

#if (PG_VERSION_NUM >= 150000)
	/* see 941460fc */
	has_returning = boolVal(list_nth(fdw_private,
									 FdwModifyPrivateHasReturning));
#else
	has_returning = intVal(list_nth(fdw_private,
									FdwModifyPrivateHasReturning));
#endif

	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									resultRelInfo,
									operation,
#if (PG_VERSION_NUM >= 140000)
									outerPlanState(mtstate)->plan,
#else
									mtstate->mt_plans[subplan_index]->plan,
#endif
									strVal(list_nth(fdw_private,
													FdwModifyPrivateUpdateSql)),
									(List *) list_nth(fdw_private,
													  FdwModifyPrivateTargetAttnums),
									has_returning,
									(List *) list_nth(fdw_private,
													  FdwModifyPrivateRetrievedAttrs));


	/* Deconstruct fdw_private data. */
	/* this is the list returned by firebirdPlanForeignModify() */


	resultRelInfo->ri_FdwState = fmstate;
}


/**
 * firebirdExecForeignInsert()
 *
 * Inserts a single tuple into the foreign table.
 *
 * Parameters:
 * (Estate*) estate
 *	  Global execution state for the query

 * (ResultRelInfo*) resultRelInfo
 *	  ResultRelInfo struct describing the target foreign table
 *
 * (TupleTableSlot*) slot
 *	  Contains the tuple to be inserted
 *
 * (TupleTableSlot*) planSlot
 *	  Contains the tuple generated by the ModifyTable plan node's subplan;
 *	  it will carry any junk columns that were requested by
 *	  AddForeignUpdateTargets(). However this parameter is not
 *	  relevant for INSERT operations and can be ignored.
 *
 * Returns:
 *	  TupleTableSlot or NULL
 *
 * The return value is either a slot containing the data that was actually
 * inserted (this might differ from the data supplied, for example as a
 * result of trigger actions), or NULL if no row was actually inserted
 * (again, typically as a result of triggers). The passed-in slot can be
 * re-used for this purpose.
 *
 * The data in the returned slot is used only if the INSERT query has a
 * RETURNING clause. Hence, the FDW could choose to optimize away
 * returning some or all columns depending on the contents of the
 * RETURNING clause. However, some slot must be returned to indicate
 * success, or the query's reported rowcount will be wrong.
 */
static TupleTableSlot *
firebirdExecForeignInsert(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	FirebirdFdwModifyState *fmstate;
	const char * const *p_values;
	FBresult	 *result;

	elog(DEBUG2, "entering function %s", __func__);

	fmstate = (FirebirdFdwModifyState *) resultRelInfo->ri_FdwState;

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate,
										NULL,
										NULL,
										slot);

	elog(DEBUG1, "Executing: %s", fmstate->query);

#ifdef DEBUG_BUILD
	{
		int i;
		for (i = 0; i < fmstate->p_nums; i++)
		{
			elog(DEBUG2, "Param %i: %s", i, p_values[i] ? p_values[i] : "NULL");
		}
	}
#endif

	result = FQexecParams(fmstate->conn,
						  fmstate->query,
						  fmstate->p_nums,
						  NULL,
						  p_values,
						  NULL,
						  NULL,
						  0);

	elog(DEBUG2, " result status: %s", FQresStatus(FQresultStatus(result)));
	elog(DEBUG1, " returned rows: %i", FQntuples(result));

	switch(FQresultStatus(result))
	{
		case FBRES_EMPTY_QUERY:
		case FBRES_BAD_RESPONSE:
		case FBRES_NONFATAL_ERROR:
		case FBRES_FATAL_ERROR:
			fbfdw_report_error(ERROR,
							   ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							   result,
							   fmstate->conn,
							   fmstate->query);
			/* fbfdw_report_error() will never return here, but break anyway */
			break;
		default:
			elog(DEBUG1, "Query OK");
	}

	if (fmstate->has_returning)
	{
		if (FQntuples(result) > 0)
			store_returning_result(fmstate, slot, result);
	}

	if (result)
		FQclear(result);

	MemoryContextReset(fmstate->temp_cxt);

	return slot;
}


#if (PG_VERSION_NUM >= 140000)
/**
 * firebirdExecForeignBatchInsert()
 *
 */
TupleTableSlot **
firebirdExecForeignBatchInsert(EState *estate,
							   ResultRelInfo *resultRelInfo,
							   TupleTableSlot **slots,
							   TupleTableSlot **planSlots,
							   int *numSlots)
{
	FirebirdFdwModifyState *fmstate;
	const char * const *p_values;
	FBresult	 *result;
	int			i;

	elog(DEBUG2, "entering function %s", __func__);
	elog(DEBUG2, "firebirdExecForeignBatchInsert(): %i slots", *numSlots);

	fmstate = (FirebirdFdwModifyState *) resultRelInfo->ri_FdwState;
	elog(DEBUG1, "Executing: %s", fmstate->query);

	result = FQprepare(fmstate->conn,
					   fmstate->query,
					   fmstate->p_nums,
					   NULL);

	for (i = 0; i < *numSlots; i++)
	{

		/* Convert parameters needed by prepared statement to text form */
		p_values = convert_prep_stmt_params(fmstate,
											NULL,
											NULL,
											slots[i]);

		result = FQexecPrepared(fmstate->conn,
								result,
								fmstate->p_nums,
								p_values,
								NULL,
								NULL,
								0);

		elog(DEBUG2, " result status: %s", FQresStatus(FQresultStatus(result)));
		elog(DEBUG1, " returned rows: %i", FQntuples(result));
	}

	FQdeallocatePrepared(fmstate->conn, result);
	FQclear(result);

	return slots;
}


/**
 * firebirdGetForeignModifyBatchSize()
 *
 */
static int
firebirdGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo)
{
	FirebirdFdwModifyState *fmstate = (FirebirdFdwModifyState *) resultRelInfo->ri_FdwState;

	int			batch_size = 1;

	/* Disable batching when we have to use RETURNING. */
	if (resultRelInfo->ri_projectReturning != NULL ||
		(resultRelInfo->ri_TrigDesc &&
		 resultRelInfo->ri_TrigDesc->trig_insert_after_row))
		return 1;

	/*
	 * In EXPLAIN without ANALYZE, ri_FdwState is NULL, so we have to lookup
	 * the option directly in server/table options. Otherwise just use the
	 * value we determined earlier.
	 */
	if (fmstate)
		batch_size = fmstate->batch_size;
	else
		batch_size = get_batch_size_option(resultRelInfo->ri_RelationDesc);

	return batch_size;
}
#endif


/**
 * firebirdExecForeignUpdate()
 *
 * Updates a single tuple in the foreign table.
 *
 * Parameters:
 * (Estate*) estate
 *	  Global execution state for the query
 *
 * (ResultRelInfo*) resultRelInfo
 *	  ResultRelInfo struct describing the target foreign table
 *
 * (TupleTableSlot*) slot
 *	  contains the new data for the tuple; this will match the foreign table's
 *	  rowtype definition.
 *
 * (TupleTableSlot*) planSlot
 *	  contains the tuple that was generated by the ModifyTable plan node's
 *	  subplan; it may will carry any junk columns that were requested by
 *	  AddForeignUpdateTargets().
 *
 * Returns:
 *	  TupleTableSlot or NULL
 *
 * The return value is either a slot containing the row as it was actually
 * updated (this might differ from the data supplied, for example as a
 * result of trigger actions), or NULL if no row was actually updated
 * (again, typically as a result of triggers). The passed-in slot can be
 * re-used for this purpose.
 *
 * The data in the returned slot is used only if the UPDATE query has a
 * RETURNING clause. Hence, the FDW could choose to optimize away
 * returning some or all columns depending on the contents of the
 * RETURNING clause. However, some slot must be returned to indicate
 * success, or the query's reported rowcount will be wrong.
 *
 */

static TupleTableSlot *
firebirdExecForeignUpdate(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{

	FirebirdFdwModifyState *fmstate = (FirebirdFdwModifyState *) resultRelInfo->ri_FdwState;
	Datum		datum_ctid;
	Datum		datum_oid;
	FBresult	*result;
	const int	*paramFormats;
	const char * const *p_values;

	elog(DEBUG2, "entering function %s", __func__);

	extractDbKeyParts(planSlot, fmstate, &datum_ctid, &datum_oid);

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate,
										(ItemPointer) DatumGetPointer(datum_ctid),
										(ItemPointer) DatumGetPointer(datum_oid),
										slot);

	paramFormats = get_stmt_param_formats(fmstate,
										  (ItemPointer) DatumGetPointer(datum_ctid),
										  slot);

	elog(DEBUG1, "Executing:\n%s; p_nums: %i", fmstate->query, fmstate->p_nums);

	result = FQexecParams(fmstate->conn,
						  fmstate->query,
						  fmstate->p_nums,
						  NULL,
						  p_values,
						  NULL,
						  paramFormats,
						  0);

	elog(DEBUG1, "Result status: %s", FQresStatus(FQresultStatus(result)));

	switch(FQresultStatus(result))
	{
		case FBRES_EMPTY_QUERY:
		case FBRES_BAD_RESPONSE:
		case FBRES_NONFATAL_ERROR:
		case FBRES_FATAL_ERROR:
			fbfdw_report_error(ERROR,
							   ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							   result,
							   fmstate->conn,
							   fmstate->query);
			/* fbfdw_report_error() will never return here, but break anyway */
			break;
		default:
			elog(DEBUG1, "Query OK");
	}

	if (fmstate->has_returning)
	{
		if (FQntuples(result) > 0)
			store_returning_result(fmstate, slot, result);
	}

	if (result)
		FQclear(result);

	MemoryContextReset(fmstate->temp_cxt);

	return slot;
}


/**
 * firebirdExecForeignDelete()
 *
 * Delete one tuple from the foreign table.
 *
 * Parameters:
 * (Estate*) estate
 *	  Global execution state for the query.
 *
 * (ResultRelInfo*) resultRelInfo
 *	  ResultRelInfo struct describing the target foreign table
 *
 * (TupleTableSlot*) slot
 *	  Contains nothing useful, but can	be used to hold the returned tuple.
 *
 * (TupleTableSlot*) planSlot
 *	  Contains the tuple generated by the ModifyTable plan node's subplan;
 *	  in particular, it will carry any junk columns that were requested by
 *	  AddForeignUpdateTargets(). The junk column(s) must be used to
 *	  identify the tuple to be deleted.
 *
 * Returns:
 *	  TupleTableSlot or NULL
 *
 * The return value is either a slot containing the row that was deleted,
 * or NULL if no row was deleted (typically as a result of triggers). The
 * passed-in slot can be used to hold the tuple to be returned.
 *
 * A slot must be returned even if no data is returned by the query, to
 * ensure the correct rowcount for the query.
 */

static TupleTableSlot *
firebirdExecForeignDelete(EState *estate,
						  ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot,
						  TupleTableSlot *planSlot)
{
	FirebirdFdwModifyState *fmstate = (FirebirdFdwModifyState *) resultRelInfo->ri_FdwState;
	const char *const *p_values;
	const int  *paramFormats;
	Datum		datum_ctid;
	Datum		datum_oid;
	FBresult	*result;

	elog(DEBUG2, "entering function %s", __func__);

	extractDbKeyParts(planSlot, fmstate, &datum_ctid, &datum_oid);

	elog(DEBUG2, "preparing statement...");

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate,
										(ItemPointer) DatumGetPointer(datum_ctid),
										(ItemPointer) DatumGetPointer(datum_oid),
										slot);

	/* Generate array specifying the format of each parameter
	 * (this is mainly to specify the RDB$DB_KEY paramter)
	 */
	paramFormats = get_stmt_param_formats(fmstate,
										  (ItemPointer) DatumGetPointer(datum_ctid),
										  slot);

	elog(DEBUG1, "Executing: %s", fmstate->query);

	result = FQexecParams(fmstate->conn,
						  fmstate->query,
						  fmstate->p_nums,
						  NULL,
						  p_values,
						  NULL,
						  paramFormats,
						  0);

	elog(DEBUG2, " result status: %s", FQresStatus(FQresultStatus(result)));
	elog(DEBUG1, " returned rows: %i", FQntuples(result));

	switch(FQresultStatus(result))
	{
		case FBRES_EMPTY_QUERY:
		case FBRES_BAD_RESPONSE:
		case FBRES_NONFATAL_ERROR:
		case FBRES_FATAL_ERROR:
			fbfdw_report_error(ERROR,
							   ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION,
							   result,
							   fmstate->conn,
							   fmstate->query);
			/* fbfdw_report_error() will never return here, but break anyway */
			break;
		default:
			elog(DEBUG2, "Query OK");
			if (fmstate->has_returning)
			{
				if (FQntuples(result) > 0)
					store_returning_result(fmstate, slot, result);
			}
	}

	if (result)
		FQclear(result);

	MemoryContextReset(fmstate->temp_cxt);

	return slot;
}


/**
 * firebirdEndForeignModify()
 *
 * End the table update and release resources. It is normally not
 * important to release palloc'd memory, but for example open files and
 * connections to remote servers should be cleaned up.
 */

static void
firebirdEndForeignModify(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	FirebirdFdwModifyState *fm_state = (FirebirdFdwModifyState *) resultRelInfo->ri_FdwState;

	elog(DEBUG2, "entering function %s", __func__);

	if (fm_state == NULL)
		return;
}


/**
 * firebirdExplainForeignModify()
 *
 * Print additional EXPLAIN output for a foreign table update. This
 * function can call ExplainPropertyText and related functions to add
 * fields to the EXPLAIN output. The flag fields in es can be used to
 * determine what to print, and the state of the ModifyTableState node can
 * be inspected to provide run-time statistics in the EXPLAIN ANALYZE
 * case. The first four arguments are the same as for BeginForeignModify.
 *
 * If the ExplainForeignModify pointer is set to NULL, no additional
 * information is printed during EXPLAIN.
 */

static void
firebirdExplainForeignModify(ModifyTableState *mtstate,
							 ResultRelInfo *resultRelInfo,
							 List *fdw_private,
							 int subplan_index,
							 struct ExplainState *es)
{
	elog(DEBUG2, "entering function %s", __func__);

	ExplainPropertyText("Firebird query",
						strVal(list_nth(fdw_private,
										FdwScanPrivateSelectSql)),
						es);

#if (PG_VERSION_NUM >= 140000)
	if (es->verbose)
	{
		/*
		 * For INSERT we should always have batch size >= 1, but UPDATE and
		 * DELETE don't support batching so don't show the property.
		 */
		if (resultRelInfo->ri_BatchSize > 0)
			ExplainPropertyInteger("Batch Size", NULL, resultRelInfo->ri_BatchSize, es);
	}
#endif
}

#if (PG_VERSION_NUM >= 140000)
static void firebirdExecForeignTruncate(List *rels,
										DropBehavior behavior,
										bool restart_seqs)
{
	ForeignServer *server = NULL;
	Oid			serverid = InvalidOid;
	UserMapping *user = NULL;
	FBconn	   *conn = NULL;

	FirebirdFdwState *fdw_state = NULL;
	StringInfoData fkey_query;
	ListCell   *lc;

	/*
	 * TRUNCATE ... CASCADE not currently supported
	 */
	if (behavior == DROP_CASCADE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("TRUNCATE with CASCADE option not supported by firebird_fdw")));
	}

	/*
	 * TRUNCATE ... RESTART IDENTITY not supported
	 */
	if (restart_seqs == true)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("TRUNCATE with RESTART IDENTITY option not supported by firebird_fdw")));
	}

	/*
	 * For each provided table, verify if it has any foreign key references.
	 * We'll need to check if any of the references are from tables not
	 * contained in the provided list.
	 */
	initStringInfo(&fkey_query);

	appendStringInfoString(&fkey_query,
						   "     SELECT TRIM(from_table.rdb$relation_name) AS from_table, "
						   "            TRIM(from_field.rdb$field_name) AS from_field, "
						   "            TRIM(from_table.rdb$index_name) AS index_name, "
						   "            TRIM(to_field.rdb$field_name) AS to_field "
						   "       FROM rdb$indices from_table "
						   " INNER JOIN rdb$index_segments from_field "
						   "         ON (from_field.rdb$index_name = from_table.rdb$index_name) "
						   " INNER JOIN rdb$indices to_table "
						   "         ON (to_table.rdb$index_name = from_table.rdb$foreign_key) "
						   " INNER JOIN rdb$index_segments to_field "
						   "         ON (to_table.rdb$index_name = to_field.rdb$index_name)"
						   "      WHERE TRIM(to_table.rdb$relation_name) = ? "
						   "        AND from_table.rdb$foreign_key IS NOT NULL ");

	/*
	 * First pass: verify tables can be truncated
	 */
	foreach(lc, rels)
	{
		Relation	rel = lfirst(lc);
		ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
		Oid relid = RelationGetRelid(rel);
		fbTableOptions table_options = fbTableOptions_init;
		fbServerOptions server_options = fbServerOptions_init;

		bool truncatable = true;
		bool updatable = true;

		char **p_values = (char **) palloc0(sizeof(char *));
		FBresult   *res = NULL;

		elog(DEBUG3, "table is %s", get_rel_name(relid));

		/*
		 * On the first pass, fetch the server and user
		 */
		if (!OidIsValid(serverid))
		{
			serverid = table->serverid;
			server = GetForeignServer(serverid);
			user = GetUserMapping(GetUserId(), server->serverid);

			elog(DEBUG3, "server is %s", server->servername);

			fdw_state = getFdwState(relid);
			Assert(fdw_state != NULL);
		}

		/*
		 * Fetch the server options for each iteration; we could cache them
		 * but it doesn't seem worth the additional fuss.
		 */
		server_options.quote_identifiers.opt.boolptr = &fdw_state->quote_identifier;
		server_options.truncatable.opt.boolptr = &truncatable;
		server_options.updatable.opt.boolptr = &updatable;

		firebirdGetServerOptions(
			server,
			&server_options);

		table_options.query.opt.strptr = &fdw_state->svr_query;
		table_options.quote_identifier.opt.boolptr =  &fdw_state->quote_identifier;
		table_options.truncatable.opt.boolptr = &truncatable;
		table_options.updatable.opt.boolptr = &updatable;

		firebirdGetTableOptions(
			table,
			&table_options);

		/*
		 * Check the server/table options allow the table to be truncated.
		 * Foreign tables defined as queries are automatically considered as
		 * "updatable=false", so we don't need to check those explicitly.
		 */
		if (updatable == false)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("foreign table \"%s\" is not updatable",
							get_rel_name(relid))));

		if (truncatable == false)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("foreign table \"%s\" does not allow truncates",
							get_rel_name(relid))));

		conn = firebirdInstantiateConnection(server, user);

		/*
		 * Check the target table has no foreign key references
		 */
		p_values[0] = pstrdup(fdw_state->svr_table);
		unquoted_ident_to_upper(p_values[0]);

		elog(DEBUG3, "remote table is: %s", p_values[0]);

		res = FQexecParams(conn,
						   fkey_query.data,
						   1,
						   NULL,
						   (const char **)p_values,
						   NULL,
						   NULL,
						   0);

		if (FQresultStatus(res) != FBRES_TUPLES_OK)
		{
			FQclear(res);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("unable to execute foreign key metadata query for table \"%s\" on foreign server \"%s\"",
							p_values[0],
							server->servername)));
		}


		if (FQntuples(res) > 0)
		{
			StringInfoData detail;
			int row;

			elog(DEBUG3, "fkey references: %i", FQntuples(res));

			initStringInfo(&detail);
			appendStringInfo(&detail,
							 "remote table \"%s\" has following foreign key references:\n",
							 p_values[0]);

			for (row = 0; row < FQntuples(res); row++)
			{
				appendStringInfo(&detail,
								 "- table \"%s\" column \"%s\" to column \"%s\"\n",
								 FQgetvalue(res, row, 0),
								 FQgetvalue(res, row, 1),
								 FQgetvalue(res, row, 3));
			}

			FQclear(res);
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("foreign table \"%s\" has foreign key references",
							get_rel_name(relid)),
					 errdetail("%s", detail.data)));
		}
	}

	Assert(server != NULL);
	Assert(conn != NULL);

	foreach(lc, rels)
	{
		Relation	rel = lfirst(lc);
		Oid relid = RelationGetRelid(rel);

		FBresult   *res = NULL;
		StringInfoData delete_query;

		initStringInfo(&delete_query);

		buildTruncateSQL(&delete_query,
						 fdw_state, rel);

		elog(DEBUG3, "truncate query is: %s", delete_query.data);


		res = FQexec(conn, delete_query.data);

		pfree(delete_query.data);

		if (FQresultStatus(res) != FBRES_COMMAND_OK)
		{
			StringInfoData detail;

			initStringInfo(&detail);
			appendStringInfoString(&detail,
								   FQresultErrorField(res, FB_DIAG_MESSAGE_PRIMARY));

			if (FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL) != NULL)
				appendStringInfo(&detail,
								 ": %s",
								 FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL));

			FQclear(res);

			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("unable to truncate table \"%s\" on foreign server \"%s\"",
							get_rel_name(relid),
							server->servername),
					 errdetail("%s", detail.data)));
		}

		FQclear(res);
	}

	pfree(fkey_query.data);
}
#endif

#if (PG_VERSION_NUM >= 110000)
/**
 * firebirdBeginForeignInsert()
 *
 * Initialize the FDW state for COPY to a foreign table.
 *
 * Note we do not yet support the case where the table is the partition
 * chosen for tuple routing.
 */

static void
firebirdBeginForeignInsert(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo)
{
	FirebirdFdwModifyState *fmstate;

	ModifyTable *plan = castNode(ModifyTable, mtstate->ps.plan);
	Index		resultRelation;
	EState	   *estate = mtstate->ps.state;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	RangeTblEntry *rte;
	TupleDesc	tupdesc = RelationGetDescr(rel);

	int			attnum;

	List	   *targetAttrs = NIL;
	List	   *retrieved_attrs = NIL;

	StringInfoData sql;

	FirebirdFdwState *fdw_state = getFdwState(RelationGetRelid(rel));


	elog(DEBUG2, "%s: begin foreign table insert on %s",
		 __func__,
		 RelationGetRelationName(rel));

	/*
	 * If the foreign table we are about to insert routed rows into is also an
	 * UPDATE subplan result rel that will be updated later, proceeding with
	 * the INSERT will result in the later UPDATE incorrectly modifying those
	 * routed rows, so prevent the INSERT --- it would be nice if we could
	 * handle this case; but for now, throw an error for safety.
	 */
	if (plan && plan->operation == CMD_UPDATE &&
		(resultRelInfo->ri_usesFdwDirectModify ||
#if (PG_VERSION_NUM >= 140000)
		resultRelInfo->ri_FdwState))
#else
		resultRelInfo->ri_FdwState) &&
		resultRelInfo > mtstate->resultRelInfo + mtstate->mt_whichplan)
#endif
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route tuples into foreign table to be updated \"%s\"",
						RelationGetRelationName(rel))));


	/* no support for INSERT ... ON CONFLICT (9.5 and later) */
	if (plan && plan->onConflictAction != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("INSERT with ON CONFLICT clause is not supported")));

#if (PG_VERSION_NUM < 110000)
	resultRelation = resultRelInfo->ri_RangeTableIndex;
	rte = list_nth(estate->es_range_table, resultRelation - 1);
#endif

#if (PG_VERSION_NUM >= 110000)
	if (resultRelInfo->ri_RangeTableIndex == 0)
	{
		ResultRelInfo *rootResultRelInfo = resultRelInfo->ri_RootResultRelInfo;
#if (PG_VERSION_NUM > 120000)
		rte = exec_rt_fetch(rootResultRelInfo->ri_RangeTableIndex, estate);
#else
		rte = list_nth(estate->es_range_table, rootResultRelInfo->ri_RangeTableIndex - 1);
#endif
#else
	if (rte->relid != RelationGetRelid(rel))
	{
#endif

		rte = copyObject(rte);
		rte->relid = RelationGetRelid(rel);
		rte->relkind = RELKIND_FOREIGN_TABLE;

		/*
		 * For UPDATE, we must use the RT index of the first subplan target
		 * rel's RTE, because the core code would have built expressions for
		 * the partition, such as RETURNING, using that RT index as varno of
		 * Vars contained in those expressions.
		 */

#if (PG_VERSION_NUM >= 110000)
		if (plan && plan->operation == CMD_UPDATE &&
#if (PG_VERSION_NUM >= 120000)
			rootResultRelInfo->ri_RangeTableIndex == plan->rootRelation)
#else
			rootResultRelInfo->ri_RangeTableIndex == plan->nominalRelation)
#endif
			resultRelation = mtstate->resultRelInfo[0].ri_RangeTableIndex;
		else
			resultRelation = rootResultRelInfo->ri_RangeTableIndex;
#else
		if (plan && plan->operation == CMD_UPDATE &&
			resultRelation == plan->nominalRelation)
			resultRelation = mtstate->resultRelInfo[0].ri_RangeTableIndex;
#endif
	}
#if (PG_VERSION_NUM >= 110000)
	else
	{
		resultRelation = resultRelInfo->ri_RangeTableIndex;
#if (PG_VERSION_NUM >= 120000)
		rte = exec_rt_fetch(resultRelation, estate);
#else
		rte = list_nth(estate->es_range_table, resultRelation - 1);
#endif
	}
#endif

	/* Transmit all columns that are defined in the foreign table. */
	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		if (!attr->attisdropped)
		{
			elog(DEBUG3, "attribute is: %s", attr->attname.data);
			targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}

	initStringInfo(&sql);

	buildInsertSql(&sql,
				   rte,
				   fdw_state,
				   resultRelation,
				   rel,
				   targetAttrs,
				   resultRelInfo->ri_returningList,
				   &retrieved_attrs);

	elog(DEBUG2, "%s", sql.data);

	fmstate = create_foreign_modify(estate,
									rte,
									resultRelInfo,
									mtstate->operation,
									NULL,
									sql.data,
									targetAttrs,
									retrieved_attrs != NIL,
									retrieved_attrs);

	resultRelInfo->ri_FdwState = fmstate;
}

static void
firebirdEndForeignInsert(EState *estate,
						 ResultRelInfo *resultRelInfo)
{
	FirebirdFdwModifyState *fm_state = (FirebirdFdwModifyState *)resultRelInfo->ri_FdwState;

	MemoryContextDelete(fm_state->temp_cxt);
}

#endif


/**
 * firebirdAnalyzeForeignTable()
 *
 * Called when ANALYZE is executed on a foreign table. Provides a pointer
 * to 'firebirdAnalyzeForeignTable()', which does the actual analyzing.
 *
 * Currently foreign tables defined with the 'query' option are not analyzed,
 * although it could make sense to do that.
 */

static bool
firebirdAnalyzeForeignTable(Relation relation,
							AcquireSampleRowsFunc *func,
							BlockNumber *totalpages)
{
	Oid relid = RelationGetRelid(relation);
	FirebirdFdwState *fdw_state = getFdwState(relid);

	elog(DEBUG2, "entering function %s", __func__);

	/* ensure we are analyzing a table, not a query */
	if (fdw_state->svr_table == NULL)
		return false;

	*func = fbAcquireSampleRowsFunc;

	/*
	 * Need to provide positive page count to indicate that the table has
	 * been analyzed, however there's no reliable way of obtaining metadata
	 * about table size etc. in Firebird [*], so we'll return an arbitrary
	 * value.
	 *
	 * [*] see e.g. http://firebird.1100200.n4.nabble.com/How-can-i-find-size-of-table-in-firebird-td3323739.html
	 */
	*totalpages = 1;

	return true;
}


/**
 * fbAcquireSampleRowsFunc()
 *
 * Scans the foreign table and returns a random sample of rows.
 *
 * Up to 'targrows' rows are collected and placed as tuples into
 * 'rows'. Additionally the estimate number of live rows ('totalrows')
 * and dead rows ('totaldeadrows') is provided; although Firebird (probably)
 * has some concept of dead rows, there doesn't seem to be a way of
 * exposing this figure via the C API so we set it to zero.
 */
int
fbAcquireSampleRowsFunc(Relation relation, int elevel,
						HeapTuple *rows, int targrows,
						double *totalrows,
						double *totaldeadrows)
{
	FirebirdFdwState *fdw_state;
	StringInfoData analyze_query;
	FBresult *res;
	int collected_rows = 0, result_rows;
	double rstate, row_sample_interval = -1;

	TupleDesc tupdesc = RelationGetDescr(relation);
	AttInMetadata	 *attinmeta;
	char **tuple_values;
	Oid relid = RelationGetRelid(relation);
	int attnum;
	bool first = true;

	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;

	elog(DEBUG2, "entering function %s", __func__);

	fdw_state = getFdwState(relid);
	fdw_state->row = 0;

	table = GetForeignTable(RelationGetRelid(relation));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(relation->rd_rel->relowner, server->serverid);
	fdw_state->conn = firebirdInstantiateConnection(server, user);

	/* Prepare for sampling rows */
	/* src/backend/commands/analyze.c */
	rstate = anl_init_selection_state(targrows);
	*totalrows = 0;

	elog(DEBUG1, "analyzing foreign table with OID %i (%s)", relid, fdw_state->svr_table);
	elog(DEBUG2, "%i targrows to collect", targrows);

	/* initialize analyze query */

	initStringInfo(&analyze_query);
	appendStringInfoString(&analyze_query, "SELECT ");

	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
#if (PG_VERSION_NUM >= 110000)
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
#else
		Form_pg_attribute attr = tupdesc->attrs[attnum - 1];
#endif

		if (attr->attisdropped)
			continue;

		if (first == false)
			appendStringInfoString(&analyze_query, ", ");
		else
			first = false;

		convertColumnRef(&analyze_query, relid, attnum, fdw_state->quote_identifier);
	}

	appendStringInfo(&analyze_query,
					 " FROM %s",
					 quote_fb_identifier(fdw_state->svr_table, fdw_state->quote_identifier));

	fdw_state->query = analyze_query.data;
	elog(DEBUG1, "analyze query is: %s", fdw_state->query);

	res = FQexec(fdw_state->conn, fdw_state->query);

	if (FQresultStatus(res) != FBRES_TUPLES_OK)
	{
		FQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("unable to analyze remote table \"%s\"", fdw_state->svr_table)));
	}

	result_rows = FQntuples(res);

	elog(DEBUG1, "%i rows returned", result_rows);
	attinmeta = TupleDescGetAttInMetadata(tupdesc);
	tuple_values = (char **) palloc0(sizeof(char *) * FQnfields(res));

	for (fdw_state->row = 0; fdw_state->row < result_rows; fdw_state->row++)
	{
		/* allow user to interrupt ANALYZE */
#if (PG_VERSION_NUM >= 180000)
		vacuum_delay_point(true);
#else
		vacuum_delay_point();
#endif

		if (fdw_state->row == 0)
		   elog(DEBUG2, "result has %i cols; tupdesc has %i atts",
				FQnfields(res),
				tupdesc->natts);

		if (fdw_state->row < targrows)
		{
			/* Add first "targrows" tuples as samples */
			elog(DEBUG3, "Adding sample row %i", fdw_state->row);
			convertResToArray(res, fdw_state->row, tuple_values);
			rows[collected_rows++] = BuildTupleFromCStrings(attinmeta, tuple_values);
		}
		else
		{
			elog(DEBUG3, "Going to add a random sample");
			/*
			 * Once the initial "targrows" number of rows has been collected,
			 * replace random rows at "row_sample_interval" intervals.
			 */
			if (row_sample_interval < 0)
				row_sample_interval = anl_get_next_S(*totalrows, targrows, &rstate);

			if (row_sample_interval < 0)
			{
				int k = (int)(targrows * anl_random_fract());
				heap_freetuple(rows[k]);
				convertResToArray(res, fdw_state->row, tuple_values);
				rows[k] = BuildTupleFromCStrings(attinmeta, tuple_values);
			}

			elog(DEBUG3, "row_sample_interval: %f", row_sample_interval);
		}
	}

	FQclear(res);

	*totalrows = (double)result_rows;

	/* Firebird does not provide this information */
	*totaldeadrows = 0;

	elog(elevel,
		 "table contains %d rows, %d rows in sample",
		 result_rows,
		 collected_rows);

	return collected_rows;
}


/**
 * firebirdImportForeignSchema()
 *
 * Generate table definitions for import into PostgreSQL
 *
 * TODO:
 *	- verify data types, warn about ones which can't be imported
 *	- verify object names (FB is generally somewhat stricter than Pg,
 *	  so range of names valid in FB but not in Pg should be fairly small)
 *	- warn about comments
 */
List *
firebirdImportForeignSchema(ImportForeignSchemaStmt *stmt,
							Oid serverOid)
{
	ForeignServer *server;
	UserMapping *user;
	FBconn	   *conn;
	FBresult *res;
	int			row;
	/* number of table names specified in "LIMIT TO" or "EXCEPT" */
	int			specified_table_count = 0;
	int			params_ix = 0;

	StringInfoData table_query;

	List *firebirdTables = NIL;
	ListCell   *lc;
	char **p_values = NULL;

	bool		import_not_null = true;
	bool		import_views = true;
	bool		updatable = true;
	bool		verbose = false;

	/* Parse statement options */
	foreach(lc, stmt->options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "import_not_null") == 0)
			import_not_null = defGetBoolean(def);
		else if (strcmp(def->defname, "import_views") == 0)
			import_views = defGetBoolean(def);
		else if (strcmp(def->defname, "updatable") == 0)
			updatable = defGetBoolean(def);
		else if (strcmp(def->defname, "verbose") == 0)
			verbose = defGetBoolean(def);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
	}

	server = GetForeignServer(serverOid);
	user = GetUserMapping(GetUserId(), server->serverid);
	conn = firebirdInstantiateConnection(server, user);

	/*
	 * Query to list all non-system tables/views, potentially filtered by the values
	 * specified in IMPORT FOREIGN SCHEMA's "LIMIT TO" or "EXCEPT" clauses. We won't
	 * exclude views here so we can warn about any included in "LIMIT TO"/"EXCEPT", which
	 * will be excluded by "import_views = false".
	 */
	initStringInfo(&table_query);


	if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
		stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
	{
		foreach(lc, stmt->table_list)
			specified_table_count++;
	}

	if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
	{
		int max_identifier_length = FQserverVersion(conn) >= 40000 ? 63 : 31;
		/*
		 * If "LIMIT TO" is specified, we'll need to associate the
		 * provided table names with the corresponding names returned
		 * from Firebird, as the FDW API will actually check that
		 * the generated table definititions contain the exact same
		 * name as provided in the "LIMIT TO" clause.
		 *
		 * This is IMHO an unnecessary restriction and it should be
		 * optional for the FDW to decide whether it wants the PostgreSQL
		 * FDW API to second-guess the "correctness" of the table
		 * definitions it returns.
		 *
		 * CTEs available from at least Firebird 2.1.
		 */

		bool		first_item = true;

		p_values = (char **) palloc0(sizeof(char *) * (specified_table_count * 2));

		appendStringInfoString(&table_query,
							   "WITH pg_tables AS ( \n");

		foreach(lc, stmt->table_list)
		{
			RangeVar   *rv = (RangeVar *) lfirst(lc);

			if (first_item)
				first_item = false;
			else
				appendStringInfoString(&table_query, "	 UNION \n");

			appendStringInfo(&table_query,
							 "	SELECT CAST(? AS VARCHAR(%i)) AS pg_name, CAST(? AS VARCHAR(%i)) AS fb_name FROM rdb$database \n",
							 max_identifier_length,
							 max_identifier_length);

			/* name as provided in LIMIT TO */
			p_values[params_ix] = pstrdup(rv->relname);
			params_ix++;

			/* convert to UPPER if PostgreSQL would not quote this identifier */
			p_values[params_ix] = pstrdup(rv->relname);
			unquoted_ident_to_upper(p_values[params_ix]);
			params_ix++;
		}

		appendStringInfoString(&table_query,
							   ") \n");

		appendStringInfoString(&table_query,
							   "   SELECT TRIM(r.rdb$relation_name) AS relname, \n"
							   "		  CASE WHEN r.rdb$view_blr IS NULL THEN 'r' ELSE 'v' END AS type, \n"
							   "		  TRIM(t.pg_name) AS pg_name \n"
							   "	 FROM pg_tables t \n"
							   "	 JOIN rdb$relations r ON (TRIM(r.rdb$relation_name) = t.fb_name) \n"
							   "	WHERE (r.rdb$system_flag IS NULL OR r.rdb$system_flag = 0) \n");

	}
	else
	{
		appendStringInfoString(&table_query,
							   "   SELECT TRIM(r.rdb$relation_name) AS relname, \n"
							   "		  CASE WHEN r.rdb$view_blr IS NULL THEN 'r' ELSE 'v' END AS type \n"
							   "	 FROM rdb$relations r\n"
							   "	WHERE (r.rdb$system_flag IS NULL OR r.rdb$system_flag = 0) \n");
	}


	/* Apply restrictions for EXCEPT */
	if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
	{
		bool		first_item = true;

		appendStringInfoString(&table_query, " AND TRIM(rdb$relation_name) ");

		appendStringInfoString(&table_query, "NOT IN (");

		/* Append list of table names within IN clause */
		foreach(lc, stmt->table_list)
		{
			if (first_item)
				first_item = false;
			else
				appendStringInfoString(&table_query, ", ");

			appendStringInfoChar(&table_query, '?');
		}

		p_values = (char **) palloc0(sizeof(char *) * specified_table_count);

		foreach(lc, stmt->table_list)
		{
			RangeVar   *rv = (RangeVar *) lfirst(lc);
			char *relname = pstrdup(rv->relname);

			/* convert to UPPER if PostgreSQL would not quote this identifier */
			unquoted_ident_to_upper(relname);
			p_values[params_ix++] = relname;
		}

		appendStringInfoChar(&table_query, ')');
	}

	appendStringInfoString(&table_query,
						   " ORDER BY 1");

	elog(DEBUG3, "%s", table_query.data);

	/* Loop through tables */
	if (specified_table_count == 0)
	{
		res = FQexec(conn, table_query.data);
	}
	else
	{
		res = FQexecParams(conn,
						   table_query.data,
						   params_ix,
						   NULL,
						   (const char **)p_values,
						   NULL,
						   NULL,
						   0);
	}

	pfree(table_query.data);

	if (specified_table_count > 0)
	{
		int			i;
		for (i = 0; i < params_ix; i++)
		{
			pfree(p_values[i]);
		}
		pfree(p_values);
	}

	if (FQresultStatus(res) != FBRES_TUPLES_OK)
	{
		StringInfoData detail;

		initStringInfo(&detail);
		appendStringInfoString(&detail,
							   FQresultErrorField(res, FB_DIAG_MESSAGE_PRIMARY));

		if (FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL) != NULL)
			appendStringInfo(&detail,
							 ": %s",
							 FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL));

		FQclear(res);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("unable to execute metadata query on foreign server \"%s\"", server->servername),
				 errdetail("%s", detail.data)));
	}

	elog(DEBUG3, "returned tuples: %i", FQntuples(res));


	if (FQntuples(res) == 0)
	{
		elog(WARNING, "no objects available for import from server %s",
			 server->servername);
	}

	for (row = 0; row < FQntuples(res); row++)
	{
		char *object_name;
		char *object_type;
		char *pg_name = NULL;

		StringInfoData column_query;
		FBresult *colres;
		StringInfoData foreign_table_definition;

		object_name = FQgetvalue(res, row, 0);
		object_type = FQgetvalue(res, row, 1);

		/*
		 * If a LIMIT TO clause provided, transmit the name as provided
		 * there, as we'll need to use exactly that to generate the foreign
		 * table definition.
		 */
		if (params_ix > 0)
			pg_name = FQgetvalue(res, row, 2);

		elog(DEBUG3, "object: '%s'; type: '%c'", object_name, object_type[0]);

		if (import_views == false && object_type[0] == 'v')
		{
			if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
				elog(NOTICE,
					 "skipping view '%s' specified in LIMIT TO", object_name);
			continue;
		}

		/* List all columns for the table */
		initStringInfo(&column_query);
		generateColumnMetadataQuery(&column_query, object_name);

		elog(DEBUG3, "column query:\n%s", column_query.data);

		colres = FQexec(conn, column_query.data);

		if (FQresultStatus(colres) != FBRES_TUPLES_OK)
		{
			StringInfoData detail;

			initStringInfo(&detail);
			appendStringInfoString(&detail,
								   FQresultErrorField(colres, FB_DIAG_MESSAGE_PRIMARY));

			if (FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL) != NULL)
				appendStringInfo(&detail,
								 ": %s",
								 FQresultErrorField(colres, FB_DIAG_MESSAGE_DETAIL));

			FQclear(res);
			FQclear(colres);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("unable to execute metadata query on foreign server \"%s\" for table \"%s\"",
							server->servername,
							object_name),
					 errdetail("%s", detail.data)));
		}

		if (verbose == true)
		{
			elog(INFO,
				 "importing %s '%s'",
				 object_type[0] == 'r' ? "table" : "view",
				 object_name);
		}

		initStringInfo(&foreign_table_definition);

		convertFirebirdObject(
			server->servername,
			stmt->local_schema,
			object_name,
			object_type[0],
			pg_name,
			import_not_null,
			updatable,
			colres,
			&foreign_table_definition);

		firebirdTables = lappend(firebirdTables,
								 pstrdup(foreign_table_definition.data));
		pfree(foreign_table_definition.data);
	}

	FQclear(res);

	return firebirdTables;
}


/**
 * convertResToArray()
 *
 * Convert an FBresult row to an array of char pointers
 */
void
convertResToArray(FBresult *res, int row, char **values)
{
	int field_total, i;

	field_total = FQnfields(res);

	for (i = 0; i < field_total; i++)
	{
		/* Handle NULL values */
		if (FQgetisnull(res, row, i))
		{
			values[i] = NULL;
			continue;
		}
		values[i] = pstrdup(FQgetvalue(res, row, i));
	}
}


/**
 * convert_prep_stmt_params()
 *
 * Create array of text strings representing parameter values
 *
 * "tupleid_ctid" and "tupleid_oid" are used to form the generated RDB$DB_KEY, or
 * NULL if none.
 * "slot" is slot to get remaining parameters from, or NULL if none.
 *
 * Data is constructed in temp_cxt; caller should reset that after use.
 */
static const char **
convert_prep_stmt_params(FirebirdFdwModifyState *fmstate,
						 ItemPointer tupleid_ctid,
						 ItemPointer tupleid_oid,
						 TupleTableSlot *slot)
{
	const char **p_values;
	int			pindex = 0;
	MemoryContext oldcontext;

	elog(DEBUG2, "entering function %s", __func__);

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	p_values = (const char **) palloc0(sizeof(char *) * fmstate->p_nums);

	/* get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
#if (PG_VERSION_NUM >= 110000)
		TupleDesc	tupdesc = RelationGetDescr(fmstate->rel);
#endif
		ListCell   *lc;

		foreach (lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Datum		value;
			bool		isnull;

#if (PG_VERSION_NUM >= 110000)
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
#else
			Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];
#endif

#ifdef HAVE_GENERATED_COLUMNS
			/* Ignore generated columns - these will not be transmitted to Firebird */
			if (attr->attgenerated)
				continue;
#endif
			value = slot_getattr(slot, attnum, &isnull);

			if (isnull)
			{
				p_values[pindex] = NULL;
			}
			else
			{
				bool		value_output = false;

				/*
				 * If the column is a boolean, we may need to convert it into an integer if
				 * implicit_bool_type is in use
				 */

				switch (attr->atttypid)
				{
					case BOOLOID:
					{
						ForeignTable *table = GetForeignTable(RelationGetRelid(fmstate->rel));
						ForeignServer *server = GetForeignServer(table->serverid);
						fbServerOptions server_options = fbServerOptions_init;
						bool use_implicit_bool_type = false;

						server_options.implicit_bool_type.opt.boolptr = &use_implicit_bool_type;

						firebirdGetServerOptions(server, &server_options);

						if (use_implicit_bool_type)
						{
							bool col_implicit_bool_type = false;

							/* Firebird before 3.0 has no BOOLEAN datatype */
							if (fmstate->firebird_version < 30000)
							{
								col_implicit_bool_type = true;
							}
							else
							{
								fbColumnOptions column_options = fbColumnOptions_init;

								column_options.implicit_bool_type = &col_implicit_bool_type;

								firebirdGetColumnOptions(table->relid, attnum,
														 &column_options);
							}

							if (col_implicit_bool_type)
							{
								char *bool_value = OutputFunctionCall(&fmstate->p_flinfo[pindex],
																	  value);
								if (bool_value[0] == 'f')
									p_values[pindex] = "0";
								else
									p_values[pindex] = "1";

								value_output = true;
							}
						}
						break;
					}
					case TIMEOID:
					{
						StringInfoData fb_ts;
						TimeADT       time = DatumGetTimeADT(value);
						struct pg_tm tt, *tm = &tt;
						fsec_t		 fsec;

						time2tm(time, tm, &fsec);

						initStringInfo(&fb_ts);

						appendStringInfo(&fb_ts,
										 "%02d:%02d:%02d.%04d",
										 tt.tm_hour,
										 tt.tm_min,
										 tt.tm_sec,
										 /* Firebird has deci-millsecond granularity */
										 fsec / 100);

						p_values[pindex] = fb_ts.data;
						value_output = true;

						break;
					}
					case TIMETZOID:
					{
						StringInfoData fb_ts;
						TimeTzADT  *time = DatumGetTimeTzADTP(value);
						int			 tz;
						struct pg_tm tt, *tm = &tt;
						fsec_t		 fsec;

						bool offset_positive;
						int offset_raw;
						int offset_hours;
						int offset_minutes;

						timetz2tm(time, tm, &fsec, &tz);

						if (tz < 0)
							tz = abs(tz);
						else
							tz = 0 - tz;

						if (tz > 0)
						{
							offset_positive = true;
							offset_raw = tz;
						}
						else
						{
							offset_positive = false;
							offset_raw = abs(tz);
						}

						offset_hours = offset_raw / 3600;
						offset_minutes = (offset_raw - (offset_hours * 3600)) / 60;

						initStringInfo(&fb_ts);

						appendStringInfo(&fb_ts,
										 "%02d:%02d:%02d.%04d %c%02d:%02d",
										 tt.tm_hour,
										 tt.tm_min,
										 tt.tm_sec,
										  /* Firebird has deci-millsecond granularity */
										 fsec / 100,
										 offset_positive ? '+' : '-',
										 offset_hours,
										 offset_minutes);

						p_values[pindex] = fb_ts.data;
						value_output = true;

						break;
					}
					case TIMESTAMPOID:
					case TIMESTAMPTZOID:
					{
						StringInfoData fb_ts;
						TimestampTz	 valueTimestamp = DatumGetTimestampTz(value);
						int			 tz;
						struct pg_tm tt, *tm = &tt;
						fsec_t		 fsec;
						const char  *tzn;
						pg_tz       *utc_tz = NULL;

						/*
						 * For TIMESTAMP WITHOUT TIME ZONE, prevent conversion to the
						 * session time zone.
						 */
						if (attr->atttypid == TIMESTAMPOID)
							utc_tz = pg_tzset("utc");

						timestamp2tm(valueTimestamp, &tz, tm, &fsec, &tzn,
									 utc_tz);

						initStringInfo(&fb_ts);
						appendStringInfo(&fb_ts,
										 "%04d-%02d-%02d %02d:%02d:%02d.%04d",
										 tt.tm_year,
										 tt.tm_mon,
										 tt.tm_mday,
										 tt.tm_hour,
										 tt.tm_min,
										 tt.tm_sec,
										  /* Firebird has deci-millsecond granularity */
										 fsec / 100);

						if (attr->atttypid == TIMESTAMPTZOID)
						{
							bool offset_positive;
							int offset_raw;
							int offset_hours;
							int offset_minutes;

							if (tt.tm_gmtoff > 0)
							{
								offset_positive = true;
								offset_raw = tt.tm_gmtoff;
							}
							else
							{
								offset_positive = false;
								offset_raw = labs(tt.tm_gmtoff);
							}

							offset_hours = offset_raw / 3600;
							offset_minutes = offset_raw - (offset_hours * 3600);

							appendStringInfo(&fb_ts,
											 " %c%02d:%02d",
											 offset_positive ? '+' : '-',
											 offset_hours,
											 offset_minutes);
						}

						p_values[pindex] = fb_ts.data;
						value_output = true;
						break;
					}
				}

				/*
				 * The value was not fetched by code handling a specific data type.
				 */
				if (value_output == false)
					p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
														  value);

				elog(DEBUG1, " stmt param %i: %s", pindex, p_values[pindex]);
			}
			pindex++;
		}
	}

	/* last parameter should be db_key, if used */
	if (tupleid_ctid != NULL && tupleid_oid != NULL)
	{
		char *oidout;
		char *db_key = (char *)palloc0(FB_DB_KEY_LEN + 1);

		elog(DEBUG2, "extracting RDB$DB_KEY...");

		oidout = OutputFunctionCall(&fmstate->p_flinfo[pindex],
								 PointerGetDatum(tupleid_oid));

		sprintf(db_key,
				"%08x%08x",
				BlockIdGetBlockNumber(&tupleid_ctid->ip_blkid),
				(unsigned int)atol(oidout));

		p_values[pindex] = db_key;
		elog(DEBUG2, "RDB$DB_KEY is: %s", db_key);

		pindex++;
	}

	Assert(pindex == fmstate->p_nums);

	MemoryContextSwitchTo(oldcontext);

	return p_values;
}


/**
 * get_stmt_param_formats()
 *
 * Generate a list to pass as FQexecParams()'s 'paramFormats'
 * parameter. Basically marking all fields as text except the
 * last one, which holds the binary RDB$DB_KEY value.
 */
const int *
get_stmt_param_formats(FirebirdFdwModifyState *fmstate,
					   ItemPointer tupleid,
					   TupleTableSlot *slot)
{
	int *paramFormats = NULL;

	int			pindex = 0;
	MemoryContext oldcontext;

	elog(DEBUG2, "entering function %s", __func__);

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	paramFormats = (int *) palloc0(sizeof(int) * fmstate->p_nums);

	/* get parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach (lc, fmstate->target_attrs)
		{
#ifdef HAVE_GENERATED_COLUMNS
			int			attnum = lfirst_int(lc);
			TupleDesc	tupdesc = RelationGetDescr(fmstate->rel);
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (attr->attgenerated)
				continue;
#endif
			paramFormats[pindex] = 0;
			pindex++;
		}
	}

	/* last parameter should be db_key, if used */
	if (tupleid != NULL)
	{
		paramFormats[pindex] = -1;
		pindex++;
	}


	Assert(pindex == fmstate->p_nums);

	MemoryContextSwitchTo(oldcontext);

	return paramFormats;
}


/**
 * store_returning_result()
 *
 * Store the result of a RETURNING clause
 *
 * On error, be sure to release the FBresult on the way out.  Callers do not
 * have PG_TRY blocks to ensure this happens.
 */
static void
store_returning_result(FirebirdFdwModifyState *fmstate,
					   TupleTableSlot *slot, FBresult *res)
{
	/* FBresult must be released before leaving this function. */
	PG_TRY();
	{
		HeapTuple	newtup;

		newtup = create_tuple_from_result(res, 0,
										  fmstate->rel,
										  fmstate->attinmeta,
										  fmstate->retrieved_attrs,
										  fmstate->temp_cxt);

		/* tuple will be deleted when it is cleared from the slot */
#if (PG_VERSION_NUM >= 120000)
		/*
		 * The returning slot will not necessarily be suitable to store
		 * heaptuples directly, so allow for conversion.
		 */
		ExecForceStoreHeapTuple(newtup, slot, true);
#else
		ExecStoreTuple(newtup, slot, InvalidBuffer, true);
#endif
	}
	PG_CATCH();
	{
		if (res)
			FQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/**
 * create_tuple_from_result()
 *
 * Create a tuple from the specified result row
 *
 * Parameters:
 * (FBresult *) res
 *	   Pointer to the libfq result object
 *
 * (int) row
 *	   Row number to process
 *
 * (Relation) rel
 *	   Local representation of the foreign table
 *
 * (AttInMetadata *) attinmeta
 *	   conversion data for the rel's tupdesc
 *
 * (List *)retrieved_attrs
 *	   An integer list of the table column numbers present in the
 *	   FBresult object
 *
 * (MemoryContext) tmp_context
 *	   A working context that can be reset after each tuple.
 *
 * Returns:
 *	   HeapTuple
 */
static HeapTuple
create_tuple_from_result(FBresult *res,
						 int row,
						 Relation rel,
						 AttInMetadata *attinmeta,
						 List *retrieved_attrs,
						 MemoryContext tmp_context)
{
	HeapTuple	tuple;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Datum	   *values;
	bool	   *nulls;
	MemoryContext orig_context;
	ListCell   *lc;
	int			res_col;

	/* Make sure we're not working with an invalid row... */
	Assert(row < FQntuples(res));

	/* Create a temp context for each tuple operation to clean up data
	 * and avoid potential memory leaks
	 */
	orig_context = MemoryContextSwitchTo(tmp_context);

	values = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc0(tupdesc->natts * sizeof(bool));

	/* Initialize columns not present in result as NULLs */
	memset(nulls, true, tupdesc->natts * sizeof(bool));


	res_col = 0;
	foreach (lc, retrieved_attrs)
	{
		int			i = lfirst_int(lc);
		char	   *valstr;

		/* fetch next column's textual value */
		if (FQgetisnull(res, row, res_col))
			valstr = NULL;
		else
			valstr = FQgetvalue(res, row, res_col);

		/* convert value to internal representation */
		if (i > 0)
		{
			/* ordinary column */
			Assert(i <= tupdesc->natts);
			nulls[i - 1] = (valstr == NULL);
			values[i - 1] = InputFunctionCall(&attinmeta->attinfuncs[i - 1],
											  valstr,
											  attinmeta->attioparams[i - 1],
											  attinmeta->atttypmods[i - 1]);
		}

		res_col++;
	}

	/*
	 * Verify the expected number of columns was returned.	Note: res_col == 0 and
	 * FQnfields == 1 is expected, since deparse emits a NULL if no columns.
	 */
	if (res_col > 0 && res_col != FQnfields(res))
		elog(ERROR, "remote query result does not match the foreign table");

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(orig_context);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/* Clean up */
	MemoryContextReset(tmp_context);

	return tuple;
}


/**
 * extractDbKeyParts()
 *
 * Retrieve RDB$DB_KEY smuggled through in the CTID and XMAX fields
 */
void
extractDbKeyParts(TupleTableSlot *planSlot,
				  FirebirdFdwModifyState *fmstate,
				  Datum *datum_ctid,
				  Datum *datum_oid)
{
	bool		isNull;

	*datum_ctid = ExecGetJunkAttribute(planSlot,
									  fmstate->db_keyAttno_CtidPart,
									  &isNull);

	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "db_key (CTID part) is NULL");

	*datum_oid = ExecGetJunkAttribute(planSlot,
									 fmstate->db_keyAttno_XmaxPart,
									 &isNull);

	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "db_key (XMAX part) is NULL");
}


#if (PG_VERSION_NUM < 110000)
/* time2tm()
 * Convert time data type to POSIX time structure.
 *
 * For dates within the range of pg_time_t, convert to the local time zone.
 * If out of this range, leave as UTC (in practice that could only happen
 * if pg_time_t is just 32 bits) - thomas 97/05/27
 */
static int
time2tm(TimeADT time, struct pg_tm *tm, fsec_t *fsec)
{
	tm->tm_hour = time / USECS_PER_HOUR;
	time -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = time / USECS_PER_MINUTE;
	time -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = time / USECS_PER_SEC;
	time -= tm->tm_sec * USECS_PER_SEC;
	*fsec = time;
	return 0;
}

/* timetz2tm()
 * Convert TIME WITH TIME ZONE data type to POSIX time structure.
 */
static int
timetz2tm(TimeTzADT *time, struct pg_tm *tm, fsec_t *fsec, int *tzp)
{
	TimeOffset	trem = time->time;

	tm->tm_hour = trem / USECS_PER_HOUR;
	trem -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = trem / USECS_PER_MINUTE;
	trem -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = trem / USECS_PER_SEC;
	*fsec = trem - tm->tm_sec * USECS_PER_SEC;

	if (tzp != NULL)
		*tzp = time->zone;

	return 0;
}
#endif



#if (PG_VERSION_NUM >= 140000)
/*
 * Return the determined batch size established when the FDW state
 * was created.
 */
static int
get_batch_size_option(Relation rel)
{
	Oid			foreigntableid = RelationGetRelid(rel);
	FirebirdFdwState *fdw_state = getFdwState(foreigntableid);

	return fdw_state->batch_size;
}
#endif
