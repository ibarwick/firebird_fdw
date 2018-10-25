/*----------------------------------------------------------------------
 *
 * Foreign Data Wrapper for Firebird
 *
 * Copyright (c) 2013-2018 Ian Barwick
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Ian Barwick <barwick@gmail.com>
 *
 * Public repository: https://github.com/ibarwick/firebird_fdw
 *
 * IDENTIFICATION
 *        firebird_fdw/src/firebird_fdw.c
 *
 *----------------------------------------------------------------------
 */


#include "postgres.h"

#include "fmgr.h"

#include "funcapi.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#if (PG_VERSION_NUM >= 90300)
#include "access/htup_details.h"
#endif

#include "libfq.h"

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


#if (PG_VERSION_NUM >= 90300)
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
#endif

/* FDW handler/validator functions */

extern Datum firebird_fdw_handler(PG_FUNCTION_ARGS);
extern Datum firebird_fdw_version(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(firebird_fdw_handler);
PG_FUNCTION_INFO_V1(firebird_fdw_version);


extern void _PG_init(void);

/* Callback functions */
static void firebirdGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);

static void firebirdGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);

#if (PG_VERSION_NUM >= 90500)
static ForeignScan *firebirdGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses,
						Plan *outer_plan);
#else
static ForeignScan *firebirdGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses);
#endif

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

#if (PG_VERSION_NUM >= 90300)
static void firebirdAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation);

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
#endif

#if (PG_VERSION_NUM >= 90500)
static List *firebirdImportForeignSchema(ImportForeignSchemaStmt *stmt,
										 Oid serverOid);
#endif

/* Internal functions */

static void exitHook(int code, Datum arg);
static FirebirdFdwState *getFdwState(Oid foreigntableid);

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
convertDbKeyValue(char *p, uint32_t *key_ctid_part, uint32_t *key_oid_part);


static void
extractDbKeyParts(TupleTableSlot *planSlot,
				  FirebirdFdwModifyState *fmstate,
				  Datum *datum_ctid,
				  Datum *datum_oid);


/*
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

#if (PG_VERSION_NUM >= 90200)
	/* support for ANALYZE */
	fdwroutine->AnalyzeForeignTable = firebirdAnalyzeForeignTable;
#endif

#if (PG_VERSION_NUM >= 90300)
	/* support for insert / update / delete */
	fdwroutine->IsForeignRelUpdatable = firebirdIsForeignRelUpdatable;
	fdwroutine->AddForeignUpdateTargets = firebirdAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = firebirdPlanForeignModify;
	fdwroutine->BeginForeignModify = firebirdBeginForeignModify;
	fdwroutine->ExecForeignInsert = firebirdExecForeignInsert;
	fdwroutine->ExecForeignUpdate = firebirdExecForeignUpdate;
	fdwroutine->ExecForeignDelete = firebirdExecForeignDelete;
	fdwroutine->EndForeignModify = firebirdEndForeignModify;
	fdwroutine->ExplainForeignModify = firebirdExplainForeignModify;
#endif

#if (PG_VERSION_NUM >= 90500)
	/* support for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = firebirdImportForeignSchema;
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
	firebirdCloseConnections();
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
	int                     save_errno = errno;

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

	elog(DEBUG2, "OID: %u", foreigntableid);

	/* Initialise */
	fdw_state->svr_query = NULL;
	fdw_state->svr_table = NULL;
	fdw_state->disable_pushdowns = false;

	firebirdGetOptions(
		foreigntableid,
		&fdw_state->svr_query,
		&fdw_state->svr_table,
		&fdw_state->disable_pushdowns);

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
	ListCell   *lc;
	elog(DEBUG2, "entering function %s", __func__);

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	foreach (lc, server->options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "address") == 0)
			svr_address = defGetString(def);
	}

	/* Set startup cost based on the localness of the database */
	/* XXX TODO:
		- is there an equivalent of socket connections?
		- other way of detecting local-hostedness, incluing IPv6
	*/
	if (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0)
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
 *    The planner's global information about the query
 *
 * (RelOptInfo *)baserel
 *    The planner's information about the foreign table
 *
 * (Oid) foreigntableid
 *    The pg_class OID of the foreign table (provided for convenience)
 *
 * Returns:
 *     void
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

	RangeTblEntry *rte;
	Oid			userid;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;

	elog(DEBUG2, "entering function %s", __func__);

	rte = planner_rt_fetch(baserel->relid, root);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	fdw_state = getFdwState(foreigntableid);
	baserel->fdw_private = (void *) fdw_state;

	/* get connection options, connect and get the remote table description */

	fdw_state->conn = firebirdInstantiateConnection(server, user);
	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	identifyRemoteConditions(root,
							 baserel,
							 &fdw_state->remote_conds,
							 &fdw_state->local_conds,
							 fdw_state->disable_pushdowns,
							 FQserverVersion(fdw_state->conn));


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

	initStringInfo(&query);
	if (fdw_state->svr_query)
	{
		appendStringInfo(&query, "SELECT COUNT(*) FROM (%s)", fdw_state->svr_query);
	}
	else
	{
		appendStringInfo(&query, "SELECT COUNT(*) FROM %s", quote_identifier(fdw_state->svr_table));
	}

	fdw_state->query = pstrdup(query.data);
	pfree(query.data);
	elog(DEBUG1, "%s", fdw_state->query);


	res = FQexec(fdw_state->conn, fdw_state->query);

	if (FQresultStatus(res) != FBRES_TUPLES_OK)
	{
		FQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
				 errmsg("Unable to establish size of foreign table %s", fdw_state->svr_table)));
		return;
	}

	if (FQntuples(res) != 1)
	{
		FQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
				 errmsg("Query returned unexpected number of rows")));
		return;
	}

	baserel->rows = atof(FQgetvalue(res, 0, 0));
	baserel->tuples = baserel->rows;
	FQclear(res);
	pfree(fdw_state->query);
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
 *    The planner's global information about the query
 *
 * (RelOptInfo *)baserel
 *    The planner's information about the foreign table
 *
 * (Oid) foreigntableid
 *    The pg_class OID of the foreign table (provided for convenience)
 *
 * NOTE: The parameters are the same as for GetForeignRelSize(), which was
 * previously called.
 *
 * Returns:
 *     void
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
#if (PG_VERSION_NUM >= 90600)
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
#elif (PG_VERSION_NUM >= 90500)
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
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
									 NULL));	/* no extra plan */
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
 *    The planner's global information about the query
 *
 * (RelOptInfo *) baserel
 *    The planner's information about the foreign table
 *
 * (Oid) foreigntableid
 *    The pg_class OID of the foreign table (provided for convenience)
 *
 * (ForeignPath *) best_path
 *    the selected ForeignPath, previously produced by GetForeignPaths()
 *
 * (List *) tlist
 *    The target list to be emitted by the plan node
 *
 * (List *) scan_clauses
 *    The restriction clauses to be enforced by the plan node.
 *
 * Returns:
 *    ForeignScan *
 */

static ForeignScan *
#if (PG_VERSION_NUM >= 90500)
firebirdGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses,
					   Plan *outer_plan)
#else
firebirdGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid,
					   ForeignPath *best_path,
					   List *tlist,
					   List *scan_clauses)
#endif
{
	Index		scan_relid = baserel->relid;
	FirebirdFdwState *fdw_state = (FirebirdFdwState *)baserel->fdw_private;

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
		}
		else if (list_member_ptr(fdw_state->local_conds, rinfo))
		{
			elog(DEBUG1, " - local");
			local_exprs = lappend(local_exprs, rinfo->clause);
		}
		else
		{
			Assert(isFirebirdExpr(root, baserel, rinfo->clause, FQserverVersion(fdw_state->conn)));
			elog(DEBUG1, " - remote, but not a member of fdw_state->remote_conds");
			remote_conds = lappend(remote_conds, rinfo);
		}
	}

	/* Build query */
	initStringInfo(&sql);
	buildSelectSql(&sql, root, baserel, fdw_state->attrs_used,
				   &retrieved_attrs, &db_key_used);

	if (remote_conds)
		buildWhereClause(&sql, root, baserel, remote_conds, true, &params_list, FQserverVersion(fdw_state->conn));

	elog(DEBUG2, "db_key_used? %c", db_key_used == true ? 'Y' : 'N');

	/*
	 * Build the fdw_private list which will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	fdw_private = list_make3(makeString(sql.data),
							 retrieved_attrs,
							 makeInteger(db_key_used));

	/* Create the ForeignScan node */
#if (PG_VERSION_NUM >= 90500)
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							fdw_private,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
#else
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							fdw_private);
#endif
}


/**
 * firebirdExplainForeignScan()
 *
 * Display additional EXPLAIN information; if VERBOSE specified, add Firebird's
 * somewhat rudimentary PLAN output.
 *
 * See also:
 *   include/commands/explain.h
 */
static void
firebirdExplainForeignScan(ForeignScanState *node,
						   ExplainState *es)
{
	FirebirdFdwScanState *fdw_state = (FirebirdFdwScanState *) node->fdw_state;

	elog(DEBUG2, "entering function %s", __func__);

	ExplainPropertyText("Firebird query", fdw_state->query, es);

	/* Show Firebird's "PLAN" information" in verbose mode */
	if (es->verbose)
	{
		char *plan = NULL;

		FQstartTransaction(fdw_state->conn);
		plan = FQexplainStatement(fdw_state->conn, fdw_state->query);
		FQrollbackTransaction(fdw_state->conn);

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
	bool	 disable_pushdowns = false;

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

	elog(DEBUG2, "entering function %s", __func__);

	rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
	table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* needed for svr_query */
	firebirdGetOptions(foreigntableid,
					   &svr_query,
					   &svr_table,
					   &disable_pushdowns);

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

	rel = heap_open(foreigntableid, NoLock);

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

		char	 *pg_colname = NULL;
		char	 *fb_colname = NULL;

		fdw_state->table->columns[fdw_state->table->pg_column_total] = (fbTableColumn *)palloc0(sizeof(fbTableColumn));

		pg_colname = NameStr(att_tuple->attname);
		elog(DEBUG2, "PG column: %s", pg_colname);

		fb_colname = getFirebirdColumnName(foreigntableid, i + 1);

		if (fb_colname == NULL)
			fb_colname = pg_colname;

		elog(DEBUG2, "FB column: %s", fb_colname);
		fdw_state->table->columns[fdw_state->table->pg_column_total]->fbname   = fb_colname;
		fdw_state->table->columns[fdw_state->table->pg_column_total]->pgname   = pg_colname;
		fdw_state->table->columns[fdw_state->table->pg_column_total]->pgtype   = att_tuple->atttypid;
		fdw_state->table->columns[fdw_state->table->pg_column_total]->pgtypmod = att_tuple->atttypmod;
		fdw_state->table->columns[fdw_state->table->pg_column_total]->pgattnum = att_tuple->attnum;

		fdw_state->table->columns[fdw_state->table->pg_column_total]->isdropped = att_tuple->attisdropped
			? true
			: false;
		fdw_state->table->columns[fdw_state->table->pg_column_total]->used = false;

		fdw_state->table->pg_column_total++;
	}

	heap_close(rel, NoLock);

	/* Check if table definition contains at least one column */
	if (!fdw_state->table->pg_column_total)
	{
		ereport(ERROR,
				(errmsg("No column definitions provided for foreign table %s", fdw_state->table->pg_table_name)));
		return;
	}

	/* Construct query */

	if (svr_query)
	{
		fdw_state->query = svr_query;
		fdw_state->db_key_used = false;
	}
	else
	{
		fdw_state->query = strVal(list_nth(fsplan->fdw_private,
										   FdwScanPrivateSelectSql));

		fdw_state->db_key_used = (bool)intVal(list_nth(fsplan->fdw_private,
													   FdwScanDbKeyUsed));
	}

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
	uint32_t key_oid_part  = 0;

	elog(DEBUG2, "entering function %s", __func__);

	/* execute query, if this is the first run */
	if (!fdw_state->result)
	{
		elog(DEBUG1, "remote query:\n%s", fdw_state->query);

		fdw_state->result = FQexec(fdw_state->conn, fdw_state->query);

		elog(DEBUG1, "query result: %s", FQresStatus(FQresultStatus(fdw_state->result)));

		if (FQresultStatus(fdw_state->result) != FBRES_TUPLES_OK)
		{
			fbfdw_report_error(ERROR, ERRCODE_FDW_ERROR, fdw_state->result, fdw_state->conn);
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
	elog(DEBUG2, "Tuple has %i atts", tupledesc->natts);

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
			&key_oid_part);

		/* Ensure this tuple has an OID, which we will use in conjunction with
		 * the CTID to smuggle through Firebird's RDB$DB_KEY value
		 *
		 * Trivial note: from a Firebird point of view it would be more logical
		 * to pass the first four bytes of the RDB$DB_KEY value as the OID, and
		 * the last four bytes as the CTID, as RDB$DB_KEY appears to be
		 * formatted as a table / row identifier, but that's a purely academic
		 * point.
		 */
		attinmeta->tupdesc->tdhasoid = true;
	}

	tuple = BuildTupleFromCStrings(
		attinmeta,
		values);

	pfree(values);

	if (fdw_state->db_key_used)
	{
		/* include/storage/itemptr.h */

#if (PG_VERSION_NUM >= 100000)
		ItemPointer ctid_dummy = palloc0(sizeof(ItemPointerData));
#else
		ItemPointer ctid_dummy = palloc0(SizeOfIptrData);
#endif

		/* Set CTID and OID with values extrapolated from RDB$DB_KEY */
		ctid_dummy->ip_blkid.bi_hi = (uint16) (key_ctid_part >> 16);
		ctid_dummy->ip_blkid.bi_lo = (uint16) key_ctid_part;
		ctid_dummy->ip_posid = 0;

		tuple->t_self = *ctid_dummy;

		HeapTupleSetOid(tuple, (Oid)key_oid_part);
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
 */
void
convertDbKeyValue(char *p, uint32_t *key_ctid_part, uint32_t *key_oid_part)
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
	*key_oid_part  = (uint32_t) db_key;
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
        FQclear(fdw_state->result);

    /* Begin new query */

    fdw_state->result = FQexec(fdw_state->conn, fdw_state->query);
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
	bool		   updatable = true;
	ForeignServer *server;
	ForeignTable  *table;
	ListCell	  *lc;

	elog(DEBUG2, "entering function %s", __func__);

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);
	/* Get server setting, if available */
	foreach (lc, server->options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "updatable") == 0)
		{
			updatable = defGetBoolean(def);
			break;
		}
	}

	/* Table setting overrides server setting */

	foreach (lc, table->options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "updatable") == 0)
		{
			updatable = defGetBoolean(def);
			break;
		}
	}

	return updatable ?
		(1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE) : 0;
}


#if (PG_VERSION_NUM >= 90300)
/**
 * firebirdAddForeignUpdateTargets()
 *
 * Add two fake target columns - 'db_key_ctidpart' and 'db_key_oidpart' -
 * which we will use to smuggle Firebird's 8-byte RDB$DB_KEY row identifier
 * in the PostgreSQL tuple header. The fake columns are marked resjunk = true.
 *
 * This identifier is required so that rows previously fetched by the
 * table-scanning functions can be identified unambiguously for UPDATE
 * and DELETE operations.
 *
 * This is a bit of a hack, as I'm not sure if it's feasible to add a
 * non-system column as a resjunk column. There have been indications in the
 * mailing lists that it might be possible, but I haven't been able to get
 * it to work or seen any examples in the wild. This requires futher
 * investigation.
 *
 * Parameters:
 * (Query *)parsetree
 *     The parse tree for the UPDATE or DELETE command
 *
 * (RangeTblEntry *)
 * (Relation)
 *     These describe the target foreign table
 *
 * Returns:
 *    void
 */
static void
firebirdAddForeignUpdateTargets(Query *parsetree,
                                RangeTblEntry *target_rte,
                                Relation target_relation)
{
	Var		   *var_ctidjunk;
	Var		   *var_oidjunk;
	const char *attrname1;
	const char *attrname2;
	TargetEntry *tle;

	elog(DEBUG2, "entering function %s", __func__);

	/*
	 * In Firebird, transactionally unique row values are returned
	 * by the RDB$DB_KEY pseudo-column. This is actually a string of
	 * unsigned byte values which we coerce into two uint32 variables
	 */

	/* Make a Var representing the desired value */
	var_ctidjunk = makeVar(parsetree->resultRelation,
				   /* This is the CTID attribute, which we are abusing to pass half the RDB$DB_KEY value */
				   SelfItemPointerAttributeNumber,
				   TIDOID,
				   -1,
				   InvalidOid,
				   0);

	/* Wrap it in a resjunk TLE with the right name ... */
	attrname1 = "db_key_ctidpart";
	elog(DEBUG2, "list_length(parsetree->targetList) %i", list_length(parsetree->targetList));

	/* backend/nodes/makefuncs.c */
	tle = makeTargetEntry((Expr *) var_ctidjunk,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname1),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);


	/* And this is the OID attribute, which we abusing
	 * to pass the other half the RDB$DB_KEY value */
	var_oidjunk = makeVar(parsetree->resultRelation,
				   ObjectIdAttributeNumber,
				   OIDOID,
				  -1,
				   InvalidOid,
				   0);

	attrname2 = "db_key_oidpart";

	/* backend/nodes/makefuncs.c */
	tle = makeTargetEntry((Expr *) var_oidjunk,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname2),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
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
 *    The planner's global information about the query
 *
 * (ModifyTable *) plan
 *    The ModifyTable plan node, which is complete except for the
 *    fdwPrivLists field generated in this function.
 *
 * (Index) resultRelation
 *    Identifies the target foreign table by its rangetable index
 *
 * (int) subplan_index
 *    Identifies which target of the ModifyTable plan node this is,
 *    counting from zero. This can be used for indexing into plan->plans
 *    or another substructure of the plan node.
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

	List	   *targetAttrs = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;

	elog(DEBUG2, "entering function %s", __func__);
	elog(DEBUG2, "RTE rtekind: %i; operation %i", rte->rtekind, operation);

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	/* access/heapam.h
	   backend/access/heapam.c
	   heap_open		- open a heap relation by relation OID

	   *		heap_open - open a heap relation by relation OID
	   *
	   *		This is essentially relation_open plus check that the relation
	   *		is not an index nor a composite type.  (The caller should also
	   *		check that it's not a view or foreign table before assuming it has
	   *		storage.)
	 */

	rel = heap_open(rte->relid, NoLock);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
	if (operation == CMD_INSERT)
	{
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
#if (PG_VERSION_NUM >= 90500)
		Bitmapset  *tmpset = bms_copy(rte->updatedCols);
#else
		Bitmapset  *tmpset = bms_copy(rte->modifiedCols);
#endif
		AttrNumber	col;

		elog(DEBUG2, " * operation is UPDATE");

		while ((col = bms_first_member(tmpset)) >= 0)
		{
			/* include/access/sysattr.h:#define FirstLowInvalidHeapAttributeNumber (-8) */

			col += FirstLowInvalidHeapAttributeNumber;
			/* include/access/attnum.h:#define InvalidAttrNumber   0 */

			if (col <= InvalidAttrNumber)		/* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, col);
		}
	}


	/* Extract the relevant RETURNING list, if any */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);


	elog(DEBUG1, "Construct the SQL command string ");
	/* Construct the SQL command string */
	switch (operation)
	{
		case CMD_INSERT:
			buildInsertSql(&sql, root, resultRelation, rel,
						   targetAttrs, returningList,
						   &retrieved_attrs);
			break;

		case CMD_UPDATE:
			buildUpdateSql(&sql, root, resultRelation, rel,
						   targetAttrs, returningList,
						   &retrieved_attrs);
			break;

		case CMD_DELETE:
			buildDeleteSql(&sql, root, resultRelation, rel,
						   returningList,
						   &retrieved_attrs);
			break;

		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	heap_close(rel, NoLock);
	elog(DEBUG1, "Constructed the SQL command string ");

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */

	return list_make4(makeString(sql.data),
					  targetAttrs,
					  makeInteger((returningList != NIL)),
					  retrieved_attrs);
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
 *    overall state of the ModifyTable plan node being executed;
 *    provides global data about the plan and execution state
 *
 * (ResultRelInfo) *resultRelInfo
 *    The ResultRelInfo struct describing the  target foreign table.
 *    The ri_FdwState field of ResultRelInfo can be used to store
 *    the FDW's private state.
 *
 * (List *)fdw_private
 *    contains private data generated by firebirdPlanForeignModify(), if any.
 *
 * (int) subplan_index
 *    identifies which target of the ModifyTable plan node this is.
 *
 * (int) eflags
 *    contains flag bits describing the executor's operating mode for
 *    this plan node. See also comment about (eflags & EXEC_FLAG_EXPLAIN_ONLY)
 *    in function body.
 *
 * Returns:
 *     void
 */

static void
firebirdBeginForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *resultRelInfo,
						   List *fdw_private,
						   int subplan_index,
						   int eflags)
{
	FirebirdFdwModifyState *fmstate;
	EState	   *estate = mtstate->ps.state;

	Relation	rel = resultRelInfo->ri_RelationDesc;
#if (PG_VERSION_NUM >= 110000)
	TupleDesc	tupdesc = RelationGetDescr(rel);
#endif

	CmdType		operation = mtstate->operation;
	RangeTblEntry *rte;
	Oid			userid;

	ForeignTable *table;
	ForeignServer *server;

	UserMapping *user;

	AttrNumber	n_params;

	Oid			typefnoid;
	bool		isvarlena;

	ListCell   *lc;

	elog(DEBUG2, "entering function %s", __func__);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.
	 * resultRelInfo->ri_FdwState stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Begin constructing FirebirdFdwModifyState. */
	fmstate = (FirebirdFdwModifyState *) palloc0(sizeof(FirebirdFdwModifyState));
	fmstate->rel = rel;

	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	elog(DEBUG2, "userid resolved to: %i", (int)userid);

	table = GetForeignTable(RelationGetRelid(rel));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	fmstate->conn = firebirdInstantiateConnection(server, user);

	if (FQstatus(fmstate->conn) != CONNECTION_OK)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			errmsg("Unable to connect to foreign server")
			));


	fmstate->conn->autocommit = true;
	fmstate->conn->client_min_messages = DEBUG1;

	/* Deconstruct fdw_private data. */
	/* this is the list returned by firebirdPlanForeignModify() */
	fmstate->query = strVal(list_nth(fdw_private,
									 FdwModifyPrivateUpdateSql));

	fmstate->target_attrs = (List *) list_nth(fdw_private,
											  FdwModifyPrivateTargetAttnums);
	fmstate->has_returning = intVal(list_nth(fdw_private,
											 FdwModifyPrivateHasReturning));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private,
											 FdwModifyPrivateRetrievedAttrs);


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
		fmstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(rel));

	/* Prepare for output conversion of parameters used in prepared stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	elog(DEBUG2,"  n_params is: %i", n_params);
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		/* Set up for remaining transmittable parameters */
		foreach (lc, fmstate->target_attrs)
		{
			int				  attnum = lfirst_int(lc);
#if (PG_VERSION_NUM >= 110000)
			Form_pg_attribute attr	 = TupleDescAttr(tupdesc, attnum - 1);
#else
			Form_pg_attribute attr	 = RelationGetDescr(rel)->attrs[attnum - 1];
#endif

			elog(DEBUG2, "ins/upd: attr %i, p_nums %i", attnum, fmstate->p_nums);
			Assert(!attr->attisdropped);
			getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);

			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
			fmstate->p_nums++;
		}
	}

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		/* Here we locate the resjunk columns containing the two
		   halves of the 8-byte RDB$DB_KEY value so update and delete
		   operations can locate the correct row
		 */
		Plan *subplan = mtstate->mt_plans[subplan_index]->plan;

		fmstate->db_keyAttno_CtidPart = ExecFindJunkAttributeInTlist(
			subplan->targetlist,
			"db_key_ctidpart");

		if (!AttributeNumberIsValid(fmstate->db_keyAttno_CtidPart))
		{
			elog(ERROR, "Resjunk column \"db_key_ctidpart\" not found");
			return;
		}

		elog(DEBUG2, "Found resjunk db_key_ctidpart, attno %i", fmstate->db_keyAttno_CtidPart);

		fmstate->db_keyAttno_OidPart = ExecFindJunkAttributeInTlist(subplan->targetlist,
																	"db_key_oidpart");

		if (!AttributeNumberIsValid(fmstate->db_keyAttno_OidPart))
		{
			elog(ERROR, "Resjunk column \"db_key_oidpart\" not found");
			return;
		}

		elog(DEBUG2, "Found resjunk db_key_oidpart, attno %i", fmstate->db_keyAttno_OidPart);

		getTypeOutputInfo(OIDOID, &typefnoid, &isvarlena);

		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}


	elog(DEBUG2, "	p_nums %i; n_params: %i", fmstate->p_nums, n_params);
	Assert(fmstate->p_nums <= n_params);

	resultRelInfo->ri_FdwState = fmstate;
}


/**
 * firebirdExecForeignInsert()
 *
 * Inserts a single tuple into the foreign table.
 *
 * Parameters:
 * (Estate*) estate
 *    Global execution state for the query

 * (ResultRelInfo*) resultRelInfo
 *    ResultRelInfo struct describing the target foreign table
 *
 * (TupleTableSlot*) slot
 *    Contains the tuple to be inserted
 *
 * (TupleTableSlot*) planSlot
 *    Contains the tuple generated by the ModifyTable plan node's subplan;
 *    it will carry any junk columns that were requested by
 *    AddForeignUpdateTargets(). However this parameter is not
 *    relevant for INSERT operations and can be ignored.
 *
 * Returns:
 *    TupleTableSlot or NULL
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
	int			 i;
	FBresult	 *result;

	elog(DEBUG2, "entering function %s", __func__);

	fmstate = (FirebirdFdwModifyState *) resultRelInfo->ri_FdwState;

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate,
										NULL,
										NULL,
										slot);

	elog(DEBUG1, "Executing: %s", fmstate->query);

	for (i = 0; i < fmstate->p_nums; i++)
	{
		if (p_values[i] != NULL)
			elog(DEBUG2, "Param %i: %s", i, p_values[i]);
	}

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
			fbfdw_report_error(ERROR, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION, result, fmstate->conn);
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
 * firebirdExecForeignUpdate()
 *
 * Updates a single tuple in the foreign table.
 *
 * Parameters:
 * (Estate*) estate
 *    Global execution state for the query
 *
 * (ResultRelInfo*) resultRelInfo
 *    ResultRelInfo struct describing the target foreign table
 *
 * (TupleTableSlot*) slot
 *    contains the new data for the tuple; this will match the foreign table's
 *    rowtype definition.
 *
 * (TupleTableSlot*) planSlot
 *    contains the tuple that was generated by the ModifyTable plan node's
 *    subplan; it may will carry any junk columns that were requested by
 *    AddForeignUpdateTargets().
 *
 * Returns:
 *    TupleTableSlot or NULL
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

	elog(DEBUG1, "Executing:\n%s", fmstate->query);

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
			fbfdw_report_error(ERROR, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION, result, fmstate->conn);
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
 *    Global execution state for the query.
 *
 * (ResultRelInfo*) resultRelInfo
 *    ResultRelInfo struct describing the target foreign table
 *
 * (TupleTableSlot*) slot
 *    Contains nothing useful, but can  be used to hold the returned tuple.
 *
 * (TupleTableSlot*) planSlot
 *    Contains the tuple generated by the ModifyTable plan node's subplan;
 *    in particular, it will carry any junk columns that were requested by
 *    AddForeignUpdateTargets(). The junk column(s) must be used to
 *    identify the tuple to be deleted.
 *
 * Returns:
 *    TupleTableSlot or NULL
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
			fbfdw_report_error(ERROR, ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION, result, fmstate->conn);
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

	/* need to provide positive page count to indicate that the table has
	 * been ANALYZEd */
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

	TupleDesc tupDesc = RelationGetDescr(relation);
	AttInMetadata	 *attinmeta;
	char **tuple_values;
	Oid relid = RelationGetRelid(relation);

	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;

	fdw_state = getFdwState(relid);
	fdw_state->row = 0;

	table = GetForeignTable(RelationGetRelid(relation));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(relation->rd_rel->relowner, server->serverid);
	fdw_state->conn = firebirdInstantiateConnection(server, user);
	elog(DEBUG2, "here");
	/* Prepare for sampling rows */
	/* src/backend/commands/analyze.c */
	rstate = anl_init_selection_state(targrows);
	*totalrows = 0;

	elog(DEBUG1, "Analyzing foreign table with OID %i (%s)", relid, fdw_state->svr_table);
	elog(DEBUG2, "%i targrows to collect", targrows);

	/* initialize analyze query */
	initStringInfo(&analyze_query);

	/* XXX explicitly select known columns */
	appendStringInfo(&analyze_query, "SELECT * FROM %s", quote_identifier(fdw_state->svr_table));
	fdw_state->query = analyze_query.data;
	elog(DEBUG1, "Analyze query is: %s", fdw_state->query);
	elog(DEBUG1, "%s", FQserverVersionString(fdw_state->conn));
	res = FQexec(fdw_state->conn, fdw_state->query);

	if (FQresultStatus(res) != FBRES_TUPLES_OK)
	{
		FQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
				 errmsg("Unable to analyze foreign table %s", fdw_state->svr_table)));
	}

	result_rows = FQntuples(res);

	elog(DEBUG1, "%i rows returned", result_rows);
	attinmeta = TupleDescGetAttInMetadata(tupDesc);
	tuple_values = (char **) palloc0(sizeof(char *) * FQnfields(res));

	for (fdw_state->row = 0; fdw_state->row < result_rows; fdw_state->row++)
	{
		/* allow user to interrupt ANALYZE */
		vacuum_delay_point();

		if (fdw_state->row == 0)
		   elog(DEBUG2, "Result has %i cols; tupDesc has %i atts",
				FQnfields(res),
				tupDesc->natts
			   );

		if (fdw_state->row < targrows)
		{
			/* Add first "targrows" tuples as samples */
			elog(DEBUG1, "Adding sample row %i", fdw_state->row);
			convertResToArray(res, fdw_state->row, tuple_values);
			rows[collected_rows++] = BuildTupleFromCStrings(attinmeta, tuple_values);
		}
		else
		{
			elog(DEBUG2, "Going to add a random sample");
			/*
			 * Once the initial "targrows" number of rows has been collected,
			 * replace random rows at "row_sample_interval" intervals
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

			elog(DEBUG2, "row_sample_interval: %f", row_sample_interval);
		}
	}

	FQclear(res);
	elog(DEBUG1, "%i rows collected", collected_rows);

	*totalrows = (double)result_rows;
	*totaldeadrows = 0;

	return collected_rows;
}


#if (PG_VERSION_NUM >= 90500)
/**
 * firebirdImportForeignSchema()
 *
 * Generate table definitions for import into PostgreSQL
 *
 * TODO:
 *  - preserve quoted names
 *  - verify data types, warn about ones which can't be imported
 *  - verify object names (FB is generally somewhat stricter than Pg,
 *    so range of names valid in FB but not in Pg should be fairly small)
 *  - warn about comments
 */
List *
firebirdImportForeignSchema(ImportForeignSchemaStmt *stmt,
							Oid serverOid)
{
	ForeignServer *server;
	UserMapping *user;
	FBconn	   *conn;
	FBresult *res;
	int row;
	StringInfoData table_query;

	List *firebirdTables = NIL;

	server = GetForeignServer(serverOid);
	user = GetUserMapping(GetUserId(), server->serverid);
	conn = firebirdInstantiateConnection(server, user);

	/* Query to list all non-system tables
	 *
	 * TODO: use "stmt->list_type" and "stmt->table_list" to
	 * pre-filter tables
	 */
	initStringInfo(&table_query);
	appendStringInfoString(&table_query,
						   "   SELECT TRIM(LOWER(rdb$relation_name)) AS table_name \n"
						   "     FROM rdb$relations  \n"
						   "    WHERE rdb$view_blr IS NULL \n"
						   "      AND rdb$system_flag = 0 \n"
						   " ORDER BY 1");

	/* Loop through tables */
	res = FQexec(conn, table_query.data);
	pfree(table_query.data);

	if (FQresultStatus(res) != FBRES_TUPLES_OK)
	{
		FQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("Unable to execute metadata query on %s", server->servername)));
	}

	elog(DEBUG1, "%s: %i", server->servername, FQntuples(res));

	for (row = 0; row < FQntuples(res); row++)
	{
		char *table_name;
		char *column_query;
		FBresult *colres;

		table_name = FQgetvalue(res, row, 0);

		/* List all columns for the table */
		column_query = _dataTypeSQL(table_name);
		colres = FQexec(conn, column_query);

		if (FQresultStatus(colres) != FBRES_TUPLES_OK)
		{
			FQclear(res);
			FQclear(colres);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("Unable to execute metadata query on %s for table %s",
							server->servername,
							table_name)));
		}

		if (IsImportableForeignTable(table_name, stmt))
		{
			char *foreign_table_definition = convertFirebirdTable(server->servername,
																  stmt->local_schema,
																  table_name, colres);

			firebirdTables = lappend(firebirdTables, foreign_table_definition);
		}
	}

	FQclear(res);

	return firebirdTables;
}
#endif


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
 * tupleid is db_key to send, or NULL if none
 * slot is slot to get remaining parameters from, or NULL if none
 *
 * Data is constructed in temp_cxt; caller should reset that after use.
 */
static const char **
convert_prep_stmt_params(FirebirdFdwModifyState *fmstate,
						 ItemPointer tupleid,
						 ItemPointer tupleid2,
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
		ListCell   *lc;
		foreach (lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Datum		value;
			bool		isnull;

			value = slot_getattr(slot, attnum, &isnull);

			if (isnull)
				p_values[pindex] = NULL;
			else
			{
				/* include/fmgr.h:extern char *OutputFunctionCall(FmgrInfo *flinfo, Datum val); */
				/* backend/utils/fmgr/fmgr.c */

				// XXX need to determine type, if BOOLOID convert to TRUE/FALSE

				p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex],
													  value);
				elog(DEBUG1, " stmt param %i: %s", pindex, p_values[pindex]);
			}
			pindex++;
		}
	}

	/* last parameter should be db_key, if used */
	if (tupleid != NULL)
	{
		char *oidout;
		char *db_key = (char *)palloc0(17);
		elog(DEBUG2, "extracting RDB$DB_KEY...");
		oidout = OutputFunctionCall(&fmstate->p_flinfo[pindex],
								 PointerGetDatum(tupleid2));
		// XXX use strtol?
		sprintf(db_key, "%08x%08x", BlockIdGetBlockNumber(&tupleid->ip_blkid), (unsigned int)atol(oidout));

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

	paramFormats = (int *) palloc0(sizeof(int *) * fmstate->p_nums);

	/* get parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach (lc, fmstate->target_attrs)
		{
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
	/* PGresult must be released before leaving this function. */
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
		ExecStoreHeapTuple(newtup, slot, true);
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
 *     Pointer to the libfq result object
 *
 * (int) row
 *     Row number to process
 *
 * (Relation) rel
 *     Local representation of the foreign table
 *
 * (AttInMetadata *) attinmeta
 *     conversion data for the rel's tupdesc
 *
 * (List *)retrieved_attrs
 *     An integer list of the table column numbers present in the
 *     FBresult object
 *
 * (MemoryContext) tmp_context
 *     A working context that can be reset after each tuple.
 *
 * Returns:
 *     HeapTuple
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

	/* Clean up */
	MemoryContextReset(tmp_context);

	return tuple;
}


/**
 * extractDbKeyParts()
 *
 * Retrieve RDB$DB_KEY smuggled through in the CTID and OID headers
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
									 fmstate->db_keyAttno_OidPart,
									 &isNull);

	/* shouldn't ever get a null result... */
	if (isNull)
		elog(ERROR, "db_key (OID part) is NULL");
}
