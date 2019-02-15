/*-------------------------------------------------------------------------
 *
 * convert.c
 *
 * Helper functions to:
 *   - examine WHERE clauses for expressions which can be sent to Firebird
 *     for execution;
 *   - for these expressions, generate Firebird SQL queries from the
 *     PostgreSQL parse tree
 *   - convert Firebird table definitions to PostgreSQL foreign table
 *     definitions to support IMPORT FOREIGN SCHEMA (PostgreSQL 9.5 and
 *     later)
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"


#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#if (PG_VERSION_NUM >= 90300)
#include "access/htup_details.h"
#endif

#include "firebird_fdw.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	int firebird_version;		/* Firebird version integer provided by libfq (e.g. 20501) */
} foreign_glob_cxt;


/*
 * Context for convertExpr
 */
typedef struct convert_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	StringInfo	buf;			/* cumulative final output */
	List	  **params_list;	/* exprs that will become remote Params */
	int firebird_version;		/* Firebird version integer provided by libfq (e.g. 20501) */
} convert_expr_cxt;

static char *convertDatum(Datum datum, Oid type);

static void convertColumnRef(StringInfo buf, int varno, int varattno,
                 PlannerInfo *root);
static void convertRelation(StringInfo buf, Relation rel);
static void convertStringLiteral(StringInfo buf, const char *val);
static void convertOperatorName(StringInfo buf, Form_pg_operator opform, char *left, char *right);
static void convertReturningList(StringInfo buf, PlannerInfo *root,
								 Index rtindex, Relation rel,
								 List *returningList,
								 List **retrieved_attrs);
static void convertTargetList(StringInfo buf,
							  PlannerInfo *root,
							  Index rtindex,
							  Relation rel,
							  Bitmapset *attrs_used,
							  List **retrieved_attrs,
							  bool *db_key_used);

static void convertExpr(Expr *node, convert_expr_cxt *context);
static void convertExprRecursor(Expr *node, convert_expr_cxt *context, char **result);

static void convertBoolExpr(BoolExpr *node, convert_expr_cxt *context, char **result);
static void convertConst(Const *node, convert_expr_cxt *context, char **result);
static void convertNullTest(NullTest *node, convert_expr_cxt *context, char **result);
static void convertOpExpr(OpExpr *node, convert_expr_cxt *context, char **result);
static void convertRelabelType(RelabelType *node, convert_expr_cxt *context, char **result);
static void convertScalarArrayOpExpr(ScalarArrayOpExpr *node, convert_expr_cxt *context, char **result)
;
static void convertFunction(FuncExpr *node, convert_expr_cxt *context, char **result);
static void convertVar(Var *node, convert_expr_cxt *context, char **result);

static char *convertFunctionConcat(FuncExpr *node, convert_expr_cxt *context);
static char *convertFunctionPosition(FuncExpr *node, convert_expr_cxt *context);
static char *convertFunctionSubstring(FuncExpr *node, convert_expr_cxt *context);
static char *convertFunctionTrim(FuncExpr *node, convert_expr_cxt *context, char *where);

static bool foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt);

static bool canConvertOp(OpExpr *oe, int firebird_version);
static bool is_builtin(Oid procid);


/**
 * buildSelectSql()
 *
 * Build Firebird select statement
 */
void
buildSelectSql(StringInfo buf,
			   PlannerInfo *root,
			   RelOptInfo *baserel,
			   Bitmapset *attrs_used,
			   List **retrieved_attrs,
			   bool *db_key_used)
{
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	Relation	rel;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/* Construct SELECT list */
	appendStringInfoString(buf, "SELECT ");
	convertTargetList(buf, root, baserel->relid, rel, attrs_used,
					  retrieved_attrs, db_key_used);

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	convertRelation(buf, rel);

	heap_close(rel, NoLock);
}


/**
 * buildInsertSql()
 *
 * Build Firebird INSERT statement
 */
void
buildInsertSql(StringInfo buf, PlannerInfo *root,
			   Index rtindex, Relation rel,
			   List *targetAttrs, List *returningList,
			   List **retrieved_attrs)
{
	bool		first;
	ListCell   *lc;

	appendStringInfoString(buf, "INSERT INTO ");
	convertRelation(buf, rel);
	appendStringInfoString(buf, " (");
	first = true;
	foreach (lc, targetAttrs)
	{
		int			attnum = lfirst_int(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		else
			first = false;

		convertColumnRef(buf, rtindex, attnum, root);
	}

	appendStringInfoString(buf, ")\n VALUES (");

	first = true;
	foreach (lc, targetAttrs)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		else
			first = false;

		appendStringInfoString(buf, "?");
	}

	appendStringInfoString(buf, ")");

	convertReturningList(buf, root, rtindex, rel,
						 returningList, retrieved_attrs);
}


/**
 * buildUpdateSql()
 *
 * Build Firebird UPDATE statement
 */
void
buildUpdateSql(StringInfo buf, PlannerInfo *root,
               Index rtindex, Relation rel,
               List *targetAttrs, List *returningList,
               List **retrieved_attrs)
{
	bool		first;
	ListCell   *lc;

	appendStringInfoString(buf, "UPDATE ");
	convertRelation(buf, rel);
	appendStringInfoString(buf, " SET ");

	first = true;
	foreach (lc, targetAttrs)
	{
		int attnum = lfirst_int(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		else
			first = false;

		convertColumnRef(buf, rtindex, attnum, root);
		appendStringInfo(buf, " = ?");
	}

	appendStringInfoString(buf, " WHERE rdb$db_key = ?");

	convertReturningList(buf, root, rtindex, rel,
						 returningList, retrieved_attrs);
}


/**
 * buildDeleteSql()
 *
 * build Firebird DELETE statement
 *
 * NOTE:
 *   Firebird only seems to support DELETE ... RETURNING ...
 *   but raises an error if more than one row is returned:
 *     SQL> delete from module where module_id>10000 returning module_id;
 *     Statement failed, SQLSTATE = 21000
 *     multiple rows in singleton select
 *     SQL> delete from module where module_id=2000 returning module_id;
 *     MODULE_ID
 *     =========
 *     2000
 *
 *  However the FDW deletes each row individually based on the RDB$DB_KEY
 *  value, so the syntax works as expected.
 */

void
buildDeleteSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List *returningList,
				 List **retrieved_attrs)
{

	appendStringInfoString(buf, "DELETE FROM ");
	convertRelation(buf, rel);
	appendStringInfoString(buf, " WHERE rdb$db_key = ?");

	convertReturningList(buf, root, rtindex, rel,
						 returningList, retrieved_attrs);
}


/**
 * buildWhereClause()
 *
 * Convert WHERE clauses in given list of RestrictInfos and append them to buf.
 *
 * baserel is the foreign table we're planning for.
 *
 * If no WHERE clause already exists in the buffer, is_first should be true.
 *
 * If params is not NULL, it receives a list of Params and other-relation Vars
 * used in the clauses; these values must be transmitted to the remote server
 * as parameter values.
 *
 * If params is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 */
void
buildWhereClause(StringInfo output,
				 PlannerInfo *root,
				 RelOptInfo *baserel,
				 List *exprs,
				 bool is_first,
				 List **params,
				 int firebird_version)
{
	convert_expr_cxt context;
	ListCell   *lc;

	if (params)
		*params = NIL;			/* initialize result list to empty */

	/* Set up context struct for recursion */
	context.root = root;
	context.foreignrel = baserel;
	context.buf = output;
	context.params_list = params;
	context.firebird_version = firebird_version;

	foreach (lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (is_first)
		{
			appendStringInfoString(output, " WHERE ");
			is_first = false;
		}
		else
			appendStringInfoString(output, " AND ");

		appendStringInfoChar(output, '(');
		convertExpr(ri->clause, &context);
		appendStringInfoChar(output, ')');
	}
}


#if (PG_VERSION_NUM >= 90500)
/**
 * convertFirebirdObject()
 *
 * Convert table or view to PostgreSQL format to implement IMPORT FOREIGN SCHEMA
 */
char *
convertFirebirdObject(char *server_name, char *schema, char *object_name, char object_type, bool import_not_null, bool updatable, FBresult *colres)
{
	int colnr, coltotal;

	StringInfoData create_table;

	initStringInfo(&create_table);

	/* XXX consider quoting schema and table names */
	appendStringInfo(&create_table,
					 "CREATE FOREIGN TABLE %s.%s (\n",
					 schema,
					 object_name);

	coltotal = FQntuples(colres);
	for (colnr = 0; colnr < coltotal; colnr++)
	{
		char *datatype;

		/* Column name and datatype */
		datatype = FQgetvalue(colres, colnr, 2);
		appendStringInfo(&create_table,
						 "	%s %s",
						 FQgetvalue(colres, colnr, 0),
						 datatype);

		if (object_type == 'r')
		{
			/* Default value */
			char *default_value = FQgetvalue(colres, colnr, 3);

			if (strlen(default_value))
			{
				appendStringInfo(&create_table, " %s", default_value);
			}

			/* NOT NULL */
			if (import_not_null == true && FQgetvalue(colres, colnr, 4) != NULL)
			{
				appendStringInfoString(&create_table, " NOT NULL");
			}
		}

		if (colnr < (coltotal - 1))
		{
			appendStringInfoString(&create_table, ",\n");
		}
		else
		{
			appendStringInfoString(&create_table, "\n");
		}
	}

	appendStringInfo(&create_table,
					 ") SERVER %s",
					 server_name);

	if (updatable == false)
	{
		appendStringInfo(&create_table,
						 "\n OPTIONS(updatable 'false')");
	}

	elog(DEBUG1, "%s", create_table.data);

	return create_table.data;
}
#endif

/**
 * convertDatum()
 *
 * Convert a PostgreSQL Datum to a string suitable for passing to
 * Firebird
 */
static char *
convertDatum(Datum datum, Oid type)
{
	StringInfoData result;
	regproc typoutput;
	HeapTuple tuple;
	char *str, *p;

	elog(DEBUG2, "entering function %s", __func__);

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type %u", type);

	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
	ReleaseSysCache(tuple);

	switch (type) {
		case TEXTOID:
		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case NAMEOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			/* quote string */
			initStringInfo(&result);
			appendStringInfoChar(&result, '\'');
			for (p=str; *p; ++p)
			{
				if (*p == '\'')
					appendStringInfoChar(&result, '\'');
				appendStringInfoChar(&result, *p);
			}
			appendStringInfoChar(&result, '\'');
			break;

		case INT8OID:
		case INT2OID:
		case INT4OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);
			appendStringInfoString(&result, str);
			break;

		case TIMESTAMPOID:
		case TIMEOID:
		case DATEOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);
			appendStringInfo(&result, "'%s'", str);
			break;

		case BOOLOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);

			if (*str == 't')
				appendStringInfoString(&result, "TRUE");
			else
				appendStringInfoString(&result, "FALSE");

			elog(INFO, "BOOL! '%s' %s", str, result.data);
			break;

		default:
			return NULL;
	}

	return result.data;
}


/**
 * convertColumnRef()
 *
 * Construct name to use for given column, and emit it into 'buf'.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
convertColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo *root)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	elog(DEBUG2, "entering function %s", __func__);

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/* Use Firebird column name if defined */
	colname = getFirebirdColumnName(rte->relid, varattno);

	/* otherwise use Postgres column name */
	if (colname == NULL)
#if (PG_VERSION_NUM >= 110000)
		colname = get_attname(rte->relid, varattno, false);
#else
		colname = get_relid_attribute_name(rte->relid, varattno);
#endif

	appendStringInfoString(buf, quote_identifier(colname));
}


/**
 * convertRelation()
 *
 * Append the Firebird name of specified foreign table to 'buf'.
 * Firebird does not have schemas, so we will only return the table
 * name itself.
 */
static void
convertRelation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *relname = NULL;
	ListCell   *lc;

	elog(DEBUG2, "entering function %s", __func__);

	/* If remote table name defined in the 'table' option provided, use this
	 * instead of the PostgreSQL name
	 */

	table = GetForeignTable(RelationGetRelid(rel));

	foreach (lc, table->options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "table_name") == 0)
			relname = defGetString(def);
	}

	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	appendStringInfoString(buf, quote_identifier(relname));
}


/**
 * convertStringLiteral()
 *
 * Append a SQL string literal representing "val" to buf.
 */
static void
convertStringLiteral(StringInfo buf, const char *val)
{
	const char *valptr;

	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}


/**
 * convertExpr()
 *
 * Convert node expression into Firebird-compatible SQL.
 *
 * This is a recursive function.
 */
static void
convertExpr(Expr *node, convert_expr_cxt *context)
{
	char *result = NULL;
	elog(DEBUG2, "entering function %s", __func__);

	if (node == NULL)
		return;

	convertExprRecursor(node, context, &result);

	if (result != NULL)
	{
		elog(DEBUG2, "result: %s", result);
		appendStringInfoString(context->buf, result);
	}
}


/**
 * convertExprRecursor()
 *
 * Convert node expression into Firebird-compatible SQL.
 *
 * This is a recursive function.
 */
static void
convertExprRecursor(Expr *node, convert_expr_cxt *context, char **result)
{
	elog(DEBUG2, "entering function %s", __func__);

	if (node == NULL)
		return;

	elog(DEBUG2, "Node tag %i", (int) nodeTag(node));
	switch (nodeTag(node))
	{
		case T_Var:
			convertVar((Var *) node, context, result);
			break;

		case T_OpExpr:
			convertOpExpr((OpExpr *) node, context, result);
			break;

		case T_Const:
			convertConst((Const *) node, context, result);
			break;

		case T_RelabelType:
			/* Need cast? */
			convertRelabelType((RelabelType *) node, context, result);
			break;

		case T_BoolExpr:
			convertBoolExpr((BoolExpr *) node, context, result);
			break;

		case T_NullTest:
			/* IS [NOT] NULL */
			convertNullTest((NullTest *) node, context, result);
			break;

		case T_ScalarArrayOpExpr:
			/* IS [NOT] IN (1,2,3) */
			convertScalarArrayOpExpr((ScalarArrayOpExpr *) node, context, result);
			break;

		case T_FuncExpr:
			/* selected functions which can be passed to Firebird */
			convertFunction((FuncExpr *)node, context, result);
			break;

		default:
			elog(ERROR, "unsupported expression type for convert: %d",
				 (int) nodeTag(node));
			break;
	}
}


/**
 * convertVar()
 *
 *
 */
static void
convertVar(Var *node, convert_expr_cxt *context, char **result)
{
	StringInfoData buf;
	initStringInfo(&buf);
	elog(DEBUG2, "entering function %s", __func__);

	if (node->varno == context->foreignrel->relid &&
		node->varlevelsup == 0)
		/* Var belongs to foreign table */
		convertColumnRef(&buf, node->varno, node->varattno, context->root);
	else
		elog(ERROR, "%s: var does not belong to foreign table", __func__);

	*result = pstrdup(buf.data);
}


/**
 * convertConst()
 *
 */
static void
convertConst(Const *node, convert_expr_cxt *context, char **result)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	StringInfoData	buf;

	initStringInfo(&buf);

	elog(DEBUG2, "entering function %s", __func__);

	if (node->constisnull)
	{
		appendStringInfoString(&buf, "NULL");
		*result = pstrdup(buf.data);
		return;
	}

   getTypeOutputInfo(node->consttype,
					 &typoutput, &typIsVarlena);
   extval = OidOutputFunctionCall(typoutput, node->constvalue);

   switch (node->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			appendStringInfoString(&buf, extval);
			break;

		/* BOOL supported from Firebird 3.0 */
		case BOOLOID:
			if (context->firebird_version >= 30000)
			{
				if (strcmp(extval, "t") == 0)
					appendStringInfoString(&buf, "true");
				else
					appendStringInfoString(&buf, "false");
				break;
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("BOOLEAN datatype supported from Firebird 3.0")));
			}
			break;

		/* Firebird does not support these types */
		case OIDOID:
		case BITOID:
		case VARBITOID:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("unsupported data type %i", node->consttype)));
			break;
		default:
			convertStringLiteral(&buf, extval);
			break;
	}

   *result = pstrdup(buf.data);
}


/**
 * convertBoolExpr()
 *
 * Convert a BoolExpr node.
 *
 * Note: by the time we get here, AND and OR expressions have been flattened
 * into N-argument form, so we'd better be prepared to deal with that.
 */
static void
convertBoolExpr(BoolExpr *node, convert_expr_cxt *context, char **result)
{
	const char *op = NULL;		/* keep compiler quiet */
	bool		first = true;
	ListCell   *lc;

	StringInfoData	buf;
	char *local_result;

	elog(DEBUG2, "entering function %s", __func__);

	initStringInfo(&buf);

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			convertExprRecursor(linitial(node->args), context, &local_result);
			appendStringInfo(&buf, "(NOT %s )", local_result);
			return;
	}

	appendStringInfoChar(&buf, '(');

	foreach (lc, node->args)
	{
		if (!first)
			appendStringInfo(&buf, " %s ", op);
		else
			first = false;

		convertExprRecursor((Expr *) lfirst(lc), context, &local_result);
		appendStringInfoString(&buf, local_result);
	}
	appendStringInfoChar(&buf, ')');
	*result = pstrdup(buf.data);
}


/**
 * convertNullTest()
 *
 * Convert IS [NOT] NULL expression.
 */
static void
convertNullTest(NullTest *node, convert_expr_cxt *context, char **result)
{
	StringInfoData	buf;
	char *local_result;

	elog(DEBUG2, "entering function %s", __func__);

	initStringInfo(&buf);

	appendStringInfoChar(&buf, '(');
	convertExprRecursor(node->arg, context, &local_result);
	appendStringInfoString(&buf, local_result);
	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(&buf, " IS NULL)");
	else
		appendStringInfoString(&buf, " IS NOT NULL)");

	*result = pstrdup(buf.data);
}


/**
 * convertOpExpr()
 *
 * Convert given operator expression into its Firebird equivalent, where
 * possible.
 *
 * Convertiblity is decided by canConvertOp().
 *
 * To operation priority issues, arguments are parenthesized.
 */
static void
convertOpExpr(OpExpr *node, convert_expr_cxt *context, char **result)
{
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;
	ListCell   *arg;
	char *left = NULL;
	char *right = NULL;
	StringInfoData	buf;
	initStringInfo(&buf);

	elog(DEBUG2, "entering function %s", __func__);

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	elog(DEBUG2, "oprname: %s; oprkind: %c", form->oprname.data, oprkind);

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
		   (oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Convert left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		convertExprRecursor(lfirst(arg), context, &left);
	}

	/* Convert right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		convertExprRecursor(lfirst(arg), context, &right);
	}

	/* Always parenthesize the expression. */
	appendStringInfoChar(&buf, '(');

	convertOperatorName(&buf, form, left, right);

	appendStringInfoChar(&buf, ')');

	ReleaseSysCache(tuple);

	*result = pstrdup(buf.data);
}


/**
 * convertOperatorName()
 *
 * Print the name of an operator.
 *
 * Synchronize with canConvertOp()
 */
static void
convertOperatorName(StringInfo buf, Form_pg_operator opform, char *left, char *right)
{
	char	   *oprname;
	elog(DEBUG2, "entering function %s", __func__);

	/* oprname is not a SQL identifier, so we should not quote it. */
	oprname = NameStr(opform->oprname);

	/* Raise an error if trying to convert a custom operator.
	 * This should have been caught by canConvertOp() and should therefore
	 * never happen.
	 */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);
		elog(ERROR, "Operator '%s.%s' not in pg_catalog!", opnspname, oprname);
		return;
	}

	/* These operators can be passed through as-is */
	/* ------------------------------------------- */
	if (  strcmp(oprname, "=")	== 0
	   || strcmp(oprname, "<>") == 0
	   || strcmp(oprname, ">")	== 0
	   || strcmp(oprname, "<")	== 0
	   || strcmp(oprname, ">=") == 0
	   || strcmp(oprname, "<=") == 0
		)
	{
		appendStringInfo(buf, "%s %s %s", left, oprname, right);
	}
	/* These operators require some conversion */
	/* --------------------------------------- */
	else if (strcmp(oprname, "~~") == 0)
	{
		/* LIKE */
		appendStringInfo(buf, "%s LIKE %s", left, right);
	}
	else if (strcmp(oprname, "!~~") == 0)
	{
		/* NOT LIKE */
		appendStringInfo(buf, "%s NOT LIKE %s", left, right);
	}
	else if (strcmp(oprname, "~~*") == 0)
	{
		/* ILIKE */
		appendStringInfo(buf, "LOWER(%s) LIKE LOWER(%s)", left, right);
	}
	else if (strcmp(oprname, "!~~*") == 0)
	{
		/* NOT ILIKE */
		appendStringInfo(buf, "LOWER(%s) NOT LIKE LOWER(%s)", left, right);
	}
	else if (strcmp(oprname, "<<") == 0)
	{
		appendStringInfo(buf, "BIN_SHL(%s, %s)", left, right);
	}
	else if (strcmp(oprname, ">>") == 0)
	{
		appendStringInfo(buf, "BIN_SHR(%s, %s)", left, right);
	}
	else
	{
		/* Should never happen, if it does blame canConvertOp() */
		elog(ERROR, "Unable to handle operator %s", oprname);
	}
}


/**
 * convertRelabelType()
 *
 * Convert a RelabelType (binary-compatible cast) node.
 *
 * XXX ensure correct FB casts; we will have to rewrite to
 * 'CAST (?? AS %S)'
 */
static void
convertRelabelType(RelabelType *node, convert_expr_cxt *context, char **result)
{
	elog(DEBUG2, "entering function %s", __func__);
	convertExprRecursor(node->arg, context, result);
	if (node->relabelformat != COERCE_IMPLICIT_CAST)
	{
		/* Fail with error for now */
		elog(ERROR, "convertRelabelType(): attempting to create cast");
/*		appendStringInfo(&buf, "::%s",
						 format_type_with_typemod(node->resulttype,
						 node->resulttypmod));*/
	}
}


/**
 * convertScalarArrayOpExpr()
 *
 * ... WHERE col [NOT] IN (1,2,3) ...
 */
static void
convertScalarArrayOpExpr(ScalarArrayOpExpr *node, convert_expr_cxt *context, char **result)
{
	HeapTuple	tuple;
	Datum datum;
	Const *constant;
	char *left = NULL;
	Expr	   *arg1;

	StringInfoData	buf;
	ArrayIterator iterator;
	bool first_arg, isNull;
	Oid leftargtype;
	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	elog(DEBUG2, "entering function %s", __func__);

	initStringInfo(&buf);
	arg1 = linitial(node->args);
	convertExprRecursor(arg1, context, &left);

	appendStringInfo(&buf, "(%s %s (", left, node->useOr ? "IN" : "NOT IN");

	/* the second (=last) argument must be a Const of ArrayType */
	constant = (Const *)llast(node->args);

	/* get operator name, left argument type and schema */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (! HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);

	leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;
	ReleaseSysCache(tuple);
	/* loop through the array elements */
#if (PG_VERSION_NUM >= 90500)
	iterator = array_create_iterator(DatumGetArrayTypeP(constant->constvalue), 0, NULL);
#else
	iterator = array_create_iterator(DatumGetArrayTypeP(constant->constvalue), 0);
#endif
	first_arg = true;
	while (array_iterate(iterator, &datum, &isNull))
	{
		char *c;

		if (isNull)
			c = "NULL";
		else
		{
			c = convertDatum(datum, leftargtype);
			if (c == NULL)
			{
				array_free_iterator(iterator);
				return;
			}
		}

		/* append the argument */
		appendStringInfo(&buf, "%s%s", first_arg ? "" : ", ", c);
		first_arg = false;
	}
	array_free_iterator(iterator);

	/* don't allow empty arrays */
	if (first_arg)
		return;

	appendStringInfoString(&buf, "))");
	*result = pstrdup(buf.data);
}


/**
 * convertFunction()
 *
 *
 * http://www.firebirdsql.org/refdocs/langrefupd20-functions.html
 * http://www.firebirdsql.org/refdocs/langrefupd21-intfunc.html
 * http://www.firebirdsql.org/refdocs/langrefupd25-new-in-25-intfunc.html
 */
static void
convertFunction(FuncExpr *node, convert_expr_cxt *context, char **result)
{
	HeapTuple tuple;
	char *oprname;

	StringInfoData	buf;
	bool first_arg = true;
	ListCell *lc;
	char *local_result;

	elog(DEBUG2, "entering function %s", __func__);
	/* get function name */
	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(node->funcid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", node->funcid);
	oprname = pstrdup(((Form_pg_proc)GETSTRUCT(tuple))->proname.data);
	ReleaseSysCache(tuple);

	elog(DEBUG2, " func name: %s; %i", oprname, node->funcid);

	/* Any implicit casts must be handled by Firebird */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		lc = list_head(node->args);
		convertExprRecursor(lfirst(lc), context, &local_result);

		*result = pstrdup(local_result);
		return;
	}

	/* Special conversion needed for some functions */

	if (strcmp(oprname, "concat") == 0)
	{
		*result = convertFunctionConcat(node, context);
		return;
	}

	if (strcmp(oprname, "position") == 0
	|| strcmp(oprname, "strpos") == 0)
	{
		*result = convertFunctionPosition(node, context);
		return;
	}

	if (strcmp(oprname, "substring") == 0)
	{
		*result = convertFunctionSubstring(node, context);
		return;
	}

	if (strcmp(oprname, "ltrim") == 0)
	{
		*result = convertFunctionTrim(node, context, "LEADING");
		return;
	}

	if (strcmp(oprname, "rtrim") == 0)
	{
		*result = convertFunctionTrim(node, context, "TRAILING");
		return;
	}

	initStringInfo(&buf);

	/* Name conversion needed for some functions */

	if (strcmp(oprname, "length") == 0)
	{
		appendStringInfoString(&buf, "CHAR_LENGTH");
	}
	/* FB's LOG() returns DOUBLE PRECISION
	 * and has bugs; see: http://www.firebirdsql.org/refdocs/langrefupd21-intfunc-log.html
	 * also LOG10(numeric) = LOG(dp or numeric)
	 */
	else if (strcmp(oprname, "log") == 0)
	{
		if (list_length(node->args) == 1)
			appendStringInfoString(&buf, "LOG10");
		else
			appendStringInfoString(&buf, "LOG");
	}
	/* FB's POWER() returns DOUBLE PRECISION
	 * http://www.firebirdsql.org/refdocs/langrefupd21-intfunc-power.html
	 *
	 * seems to handle implicit conversion OK
	 *	SELECT power(doubleval,decval) from datatypes
	 */
	else if (strcmp(oprname, "pow") == 0)
	{
		appendStringInfoString(&buf, "POWER");
	}
	else
	{
		appendStringInfoString(&buf, oprname);
	}

	appendStringInfoChar(&buf, '(');

	foreach (lc, node->args)
	{
		convertExprRecursor(lfirst(lc), context, &local_result);

		if (first_arg)
			first_arg = false;
		else
			appendStringInfoChar(&buf, ',');

		appendStringInfoString(&buf, local_result);
	}

	appendStringInfoChar(&buf, ')');

	*result = pstrdup(buf.data);
}


/**
 * convertFunctionConcat()
 *
 * Convert PostgreSQL's CONCAT() function (introduced in 8.4) to
 * || operator
 */
static char *
convertFunctionConcat(FuncExpr *node, convert_expr_cxt *context)
{
	StringInfoData	buf;
	ListCell *lc;
	char *local_result;
	bool first = true;

	elog(DEBUG2, "entering function %s", __func__);
	elog(DEBUG2, "arg length: %i", list_length(node->args));

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '(');

	foreach (lc, node->args)
	{
		if (first == true)
			first = false;
		else
		{
			appendStringInfoString(&buf, " || ");
		}

		convertExprRecursor((Expr *) lfirst(lc), context, &local_result);
		appendStringInfoString(&buf, local_result);
	}

	appendStringInfoChar(&buf, ')');

	return buf.data;
}



/**
 * convertFunctionPosition()
 *
 * Render POSITION() correctly. For some reason the arguments are in
 * the order for STRPOS(), so we have to switch the order. On the other
 * hand we can recycle this function to convert STRPOS().
 */
static char *
convertFunctionPosition(FuncExpr *node, convert_expr_cxt *context)
{
	StringInfoData	buf;
	ListCell *lc;
	char *string;
	char *substring;

	lc = list_head(node->args);
	convertExprRecursor(lfirst(lc), context, &string);

	lc = lnext(lc);
	convertExprRecursor(lfirst(lc), context, &substring);

	initStringInfo(&buf);
	appendStringInfo(&buf, "POSITION(%s IN %s)", substring, string);

	return buf.data;
}


/**
 * convertFunctionSubstring()
 *
 * Reconstitute SUBSTRING function arguments
 */
static char *
convertFunctionSubstring(FuncExpr *node, convert_expr_cxt *context)
{
	StringInfoData	buf;
	ListCell *lc;
	char *local_result;

	elog(DEBUG2, "entering function %s", __func__);
	elog(DEBUG2, "arg length: %i", list_length(node->args));

	initStringInfo(&buf);
	appendStringInfoString(&buf, "SUBSTRING(");

	lc = list_head(node->args);
	convertExprRecursor(lfirst(lc), context, &local_result);
	appendStringInfoString(&buf, local_result);

	lc = lnext(lc);
	convertExprRecursor(lfirst(lc), context, &local_result);
	appendStringInfo(&buf, " FROM %s", local_result);

	if (list_length(node->args) == 3)
	{
		lc = lnext(lc);
		convertExprRecursor(lfirst(lc), context, &local_result);
		appendStringInfo(&buf, " FOR %s", local_result);
	}
	appendStringInfoChar(&buf, ')');

	return buf.data;
}


/**
 * convertFunctionTrim()
 *
 * Convert Pg's LTRIM() and RTRIM() to Firebird's TRIM() syntax
 *
 * http://www.firebirdsql.org/refdocs/langrefupd21-intfunc-trim.html
 */

static char *
convertFunctionTrim(FuncExpr *node, convert_expr_cxt *context, char *where)
{
	StringInfoData	buf;
	ListCell *lc;
	char *from = NULL;
	char *what = NULL;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "TRIM(");

	appendStringInfoString(&buf, where);

	lc = list_head(node->args);
	convertExprRecursor(lfirst(lc), context, &from);

	if (list_length(node->args) == 2)
	{
		lc = lnext(lc);
		convertExprRecursor(lfirst(lc), context, &what);
		appendStringInfo(&buf, " %s", what);
	}

	appendStringInfo(&buf, " FROM %s)", from);

	return buf.data;
}


/**
 * convertReturningList()
 *
 * Generate RETURNING clause of a INSERT/UPDATE/DELETE ... RETURNING
 * statement.
 */
static void
convertReturningList(StringInfo buf, PlannerInfo *root,
					 Index rtindex, Relation rel,
					 List *returningList,
					 List **retrieved_attrs)
{
	Bitmapset  *attrs_used = NULL;
	bool db_key_used;

	elog(DEBUG2, "entering function %s", __func__);

#if (PG_VERSION_NUM >= 90400)
	if (rel->trigdesc && rel->trigdesc->trig_insert_after_row)
	{
		/* whole-row reference acquires all non-system columns */
		attrs_used =
			bms_make_singleton(0 - FirstLowInvalidHeapAttributeNumber);
	}
#endif

	if (returningList != NIL)
	{
		pull_varattnos((Node *) returningList, rtindex,
					   &attrs_used);
	}

	if (attrs_used != NULL)
	{
		/* Insert column names into the local query's RETURNING list */

		appendStringInfoString(buf, " RETURNING ");
		convertTargetList(buf, root, rtindex, rel, attrs_used,
						  retrieved_attrs, &db_key_used);
	}
	else
	{
		*retrieved_attrs = NIL;
	}
}


/**
 * convertTargetList()
 *
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is currently used for SELECT and RETURNING targetlists.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 */
static void
convertTargetList(StringInfo buf,
				  PlannerInfo *root,
				  Index rtindex,
				  Relation rel,
				  Bitmapset *attrs_used,
				  List **retrieved_attrs,
				  bool *db_key_used)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
#if (PG_VERSION_NUM >= 110000)
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
#else
		Form_pg_attribute attr = tupdesc->attrs[i - 1];
#endif

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			convertColumnRef(buf, rtindex, i, root);

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* Add rdb$db_key, if required */
	if (bms_is_member(SelfItemPointerAttributeNumber - FirstLowInvalidHeapAttributeNumber,
	attrs_used))
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		appendStringInfoString(buf, "rdb$db_key");

		*retrieved_attrs = lappend_int(*retrieved_attrs,
									   SelfItemPointerAttributeNumber);
		*db_key_used = true;
	}
	else
		*db_key_used = false;

	/* Avoid generating invalid syntax if no undropped columns exist */
	if (first)
		appendStringInfoString(buf, "NULL");
}


/**
 * identifyRemoteConditions()
 *
 * Examine each restriction clause in baserel's baserestrictinfo list,
 * and classify them into two groups, which are returned as two lists:
 *  - remote_conds contains expressions that can be evaluated remotely
 *  - local_conds contains expressions that can't be evaluated remotely
 *
 * Adapted from postgres_fdw
 */
void
identifyRemoteConditions(PlannerInfo *root,
						 RelOptInfo *baserel,
						 List **remote_conds,
						 List **local_conds,
						 bool disable_pushdowns,
						 int firebird_version)
{
	ListCell   *lc;
	elog(DEBUG2, "entering function %s", __func__);

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach (lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		if (!disable_pushdowns && isFirebirdExpr(root, baserel, ri->clause, firebird_version))
		{
			*remote_conds = lappend(*remote_conds, ri);
			elog(DEBUG2, " -> pushing down to remote");
		}
		else
		{
			*local_conds = lappend(*local_conds, ri);
			elog(DEBUG2, " -> keeping local");
		}
	}
}


/**
 * isFirebirdExpr()
 *
 * Returns true if given expr can be evaluate by Firebird.
 */
bool
isFirebirdExpr(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Expr *expr,
			   int firebird_version)
{
	foreign_glob_cxt glob_cxt;

	elog(DEBUG2, "entering function %s", __func__);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.firebird_version = firebird_version;

	if (!foreign_expr_walker((Node *) expr, &glob_cxt))
	{
		elog(DEBUG2, "%s: not FB expression", __func__);
		return false;
	}

	/* OK to evaluate on the remote server */
	return true;
}


/**
 * foreign_expr_walker()
 *
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * We must check that the expression contains only node types we can convert,
 * that all types/functions/operators are safe to send.
 *
 * Currently this only checks a subset of the more fundamental expressions,
 * and needs further testing to ensure we are only sending valid queries
 * to Firebird.
 *
 * Adapted from postgres_fdw
 */
static bool
foreign_expr_walker(Node *node,
                    foreign_glob_cxt *glob_cxt)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	elog(DEBUG2, "entering function %s", __func__);

	elog(DEBUG2, "Node is: %i", nodeTag(node));

	/* TODO: handle collation */

	switch (nodeTag(node))
	{
		case T_Var:
		{
			Var		   *var = (Var *) node;
			elog(DEBUG2, "%s: Node is var", __func__);
			/* Var belongs to foreign table */
			if (var->varno == glob_cxt->foreignrel->relid &&
				var->varlevelsup == 0)
			{
				elog(DEBUG2, "%s: Var is foreign", __func__);

				/* don't handle system columns */
				if (var->varattno < 1)
					return false;

				return true;
			}

			return false;
		}
		case T_Const:
			return true;

		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		{
			OpExpr	   *oe = (OpExpr *) node;
			elog(DEBUG2, "%s: Node is Op/Distinct", __func__);
			if (!is_builtin(oe->opno))
			{
				elog(DEBUG2, "%s: not builtin", __func__);
				return false;
			}

			if (!canConvertOp(oe, glob_cxt->firebird_version))
			{
				elog(DEBUG2, "%s: cannot translate op", __func__);
				return false;
			}

			/* Recurse to input subexpressions */
			if (!foreign_expr_walker((Node *) oe->args,
									 glob_cxt))
			{
				elog(DEBUG2, "%s: recurse to false", __func__);
				return false;
			}

			elog(DEBUG2, "%s: true", __func__);
			return true;
		}
		case T_BoolExpr:
		{
			BoolExpr   *b = (BoolExpr *) node;

			/* Recurse to input subexpressions */
			if (!foreign_expr_walker((Node *) b->args,
									 glob_cxt))
				return false;

			return true;
		}

		case T_NullTest:
		{
			NullTest   *nt = (NullTest *) node;

			/* Recurse to input subexpressions	*/
			if (!foreign_expr_walker((Node *) nt->arg,
									 glob_cxt))
				return false;

			return true;
		}

		case T_ScalarArrayOpExpr:
			/*	WHERE v1 NOT IN(1,2) */
			/* Note: FB can only handle up to 1,500 members; see FB book p396*/
		{
			HeapTuple tuple;
			char *oprname;
			Oid leftargtype;

			ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;
			elog(DEBUG2, "ScalarArrayOpExpr");

			/* We only have a chance of converting builtins */
			if (!is_builtin(oe->opno))
				return false;


			/* get operator name, left argument type and schema */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for operator %u", oe->opno);

			oprname = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);

			leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;


			ReleaseSysCache(tuple);

			/* get the type's output function */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(leftargtype));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for type %u", leftargtype);
				return false;
			}

			ReleaseSysCache(tuple);
			/* Only permit IN and NOT IN expressions for pushdown */
			if ((strcmp(oprname, "=") != 0 || ! oe->useOr)
				&& (strcmp(oprname, "<>") != 0 || oe->useOr))
				return false;

			elog(DEBUG2, "ScalarArrayOpExpr: leftargtype is %i", leftargtype);

			if (!canConvertPgType(leftargtype))
				return false;

			/* Recurse to input subexpressions */
			if (!foreign_expr_walker((Node *) oe->args,
									glob_cxt))
				return false;

			return true;
		}
		case T_FuncExpr:
		{
			FuncExpr *func = (FuncExpr *)node;
			HeapTuple tuple;
			char *oprname;
			Oid schema;

			elog(DEBUG2, "Func expr ------");
			if (!canConvertPgType(func->funcresulttype))
			{
				elog(DEBUG2, "Cannot convert return type");
				return false;
			}

			if (func->funcformat == COERCE_IMPLICIT_CAST)
			{
				if (!foreign_expr_walker((Node *) func->args,
										glob_cxt))
					return false;
				return true;
			}

			/* Recurse to input subexpressions */
			if (!foreign_expr_walker((Node *) func->args,
									glob_cxt))
				return false;

			/* get function name and schema */
			tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->funcid));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for function %u", func->funcid);
			oprname = pstrdup(((Form_pg_proc)GETSTRUCT(tuple))->proname.data);
			schema = ((Form_pg_proc)GETSTRUCT(tuple))->pronamespace;
			ReleaseSysCache(tuple);

			/* ignore functions not in pg_catalog */
			if (schema != PG_CATALOG_NAMESPACE)
				return false;

			/*
			 * Only permit certain functions (and depending on the function
			 * certain combination of parameters) to be passed
			 *
			 * NOTE: most of these functions were introduced in FB 2.1; some
			 *	 can be used to convert operators
			 *
			 * Not currently sending:
			 * BIN_AND()
			 * BIN_OR()
			 * BIN_XOR()
			 * EXTRACT()
			 * INITCAP()
			 * TO_CHAR()
			 * TO_DATE()
			 * TO_NUMBER()
			 * TO_TIMESTAMP()
			 * TRANSLATE()
			 *
			 * Not practical to push down these:
			 * IIF () - no equivalent in Pg, shorthand for a CASE construct
			 * LEFT() -> FB does not accept negative length
			 * RIGHT() -> FB does not accept negative length
			 *	 -> to handle these we'll need to examine the length value,
			 *		which is tricky
			 */
			elog(DEBUG2, "Func name is %s", oprname);

			/* Firebird 1.5 or later */
			if (glob_cxt->firebird_version >= 10500)
			{
				if (strcmp(oprname, "concat") == 0)
					return true;

				/* Firebird's COALESCE() requires at least two arguments */
				if ((strcmp(oprname, "coalesce") == 0 && list_length(func->args) >= 2))
					return true;
			}

			/* Firebird 2.0 or later */
			if (glob_cxt->firebird_version >= 20000)
			{
				if (strcmp(oprname, "bit_length") == 0
				 || strcmp(oprname, "char_length") == 0
				 || strcmp(oprname, "character_length") == 0
				 || strcmp(oprname, "lower") == 0
				 || strcmp(oprname, "octet_length") == 0
				 || strcmp(oprname, "upper") == 0)
				{
					return true;
				}

				/* SUBSTRING() is a special case: Firebird only accepts integers as the
				   2nd and 3rd params, Pg variants such as SUBSTRING(string FROM pattern FOR escape)
				   must not be pushed down.
				*/
				if (strcmp(oprname, "substring") == 0 && (list_length(func->args) == 2 || list_length(func->args) == 3))
				{
					ListCell *lc;
					Const *arg;
					bool can_handle = false;

					lc = list_head(func->args);
					lc = lnext(lc);
					arg = lfirst(lc);
					if (arg->consttype == INT4OID)
						can_handle = true;

					if (list_length(func->args) == 3)
					{
						lc = lnext(lc);
						arg = lfirst(lc);
						if (arg->consttype == INT4OID)
							can_handle = true;
					}

					return can_handle;
				}

			}

			/* Firebird 2.1 or later */
			if (glob_cxt->firebird_version >= 20100)
			{
				if (strcmp(oprname, "abs") == 0
				 || strcmp(oprname, "acos") == 0
				 || strcmp(oprname, "asin") == 0
				 || strcmp(oprname, "atan") == 0
				 || strcmp(oprname, "atan2") == 0
				 || strcmp(oprname, "ceil") == 0
				 || strcmp(oprname, "ceiling") == 0
				 || strcmp(oprname, "cos") == 0
				 || strcmp(oprname, "cot") == 0
				 || strcmp(oprname, "exp") == 0
				 || strcmp(oprname, "floor") == 0
				 || strcmp(oprname, "ltrim") == 0
				 || strcmp(oprname, "length") == 0
				 || strcmp(oprname, "log") == 0
					/* XXX: LPAD() behaviour is slightly different between 2.1 and 2.5 */
				 || strcmp(oprname, "lpad") == 0
				 || strcmp(oprname, "mod") == 0
				 || strcmp(oprname, "nullif") == 0
				 || strcmp(oprname, "overlay") == 0
				 || strcmp(oprname, "position") == 0
				 || strcmp(oprname, "pow") == 0
				 || strcmp(oprname, "power") == 0
				 || strcmp(oprname, "reverse") == 0
					/* XXX: RPAD() behaviour is slightly different between 2.1 and 2.5 */
				 || strcmp(oprname, "rpad") == 0
				 || strcmp(oprname, "rtrim") == 0
				 || strcmp(oprname, "sign") == 0
				 || strcmp(oprname, "sin") == 0
				 || strcmp(oprname, "sqrt") == 0
				 || strcmp(oprname, "strpos") == 0
				 || strcmp(oprname, "tan") == 0
				 || strcmp(oprname, "trunc") == 0)
				{
					return true;
				}
			}

			return false;
		}
		case T_List:
		{
			List	   *l = (List *) node;
			ListCell   *lc;
			foreach (lc, l)
			{
				if (!foreign_expr_walker((Node *) lfirst(lc),
										 glob_cxt))
					return false;
			}
			return true;
		}

		case T_RelabelType:
		{
			RelabelType *r = (RelabelType *) node;
			if (!foreign_expr_walker((Node *) r->arg,
									 glob_cxt))
				return false;
			return true;
		}

		/* Firebird 3 will support booleans; we may need to add an
		   exception here */

		default:

			/* Assume any other types are unsafe */
			elog(DEBUG1, "%s(): Unhandled node tag: %i", __func__, nodeTag(node));
			return false;
	}

	/* should never reach here */
	return false;
}


/**
 * is_builtin()
 *
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else format_type might incorrectly fail to schema-qualify their names.
 * (This could be fixed with some changes to format_type, but for now there's
 * no need.)  Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 *
 * Adapted from postgres_fdw
 */
static bool
is_builtin(Oid oid)
{
	return (oid < FirstBootstrapObjectId);
}


/**
 * canConvertOp()
 *
 * Indicate whether a Pg operator can be translated into a
 * Firebird equivalent
 *
 * See:
 *   http://ibexpert.net/ibe/index.php?n=Doc.ComparisonOperators
 *
 * Synchronize with convertOperatorName().
 */
static bool
canConvertOp(OpExpr *oe, int firebird_version)
{
	HeapTuple	tuple;
	Form_pg_operator form;
	char *oprname;
	Oid schema;

	/* Retrieve information the operator tuple */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", oe->opno);

	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprname = pstrdup(form->oprname.data);
	schema = form->oprnamespace;
	ReleaseSysCache(tuple);

	/* ignore operators in other than the pg_catalog schema */
	if (schema != PG_CATALOG_NAMESPACE)
		return false;

	elog(DEBUG2, "canConvertOp(): oprname is '%s'", oprname);
	if (  strcmp(oprname, "=") == 0
	   || strcmp(oprname, "<>") == 0
	   || strcmp(oprname, ">") == 0
	   || strcmp(oprname, "<") == 0
	   || strcmp(oprname, ">=") == 0
	   || strcmp(oprname, "<=") == 0
	   || strcmp(oprname, "~~") == 0
	   || strcmp(oprname, "!~~") == 0
	   || strcmp(oprname, "~~*") == 0
	   || strcmp(oprname, "!~~*") == 0
		)
	{
		pfree(oprname);
		return true;
	}

	/* Some Pg operators have equivalent functions in Firebird */
	if (firebird_version >= 20100)
	{
		if (strcmp(oprname, "<<") == 0
		|| strcmp(oprname, ">>") == 0)
		{
			pfree(oprname);
			return true;
		}
	}


	pfree(oprname);

	return false;
}


/**
 * getFirebirdColumnName()
 *
 * Return Firebird column name as defined by the column option,
 * otherwise NULL.
 */
char *
getFirebirdColumnName(Oid foreigntableid, int varattno)
{
	List	   *options;
	ListCell   *lc;
	char *colname = NULL;

	options = GetForeignColumnOptions(foreigntableid, varattno);
	foreach (lc, options)
	{
		DefElem	   *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	return colname;
}

#if (PG_VERSION_NUM >= 90500)
/**
 * _dataTypeSQL()
 *
 * Generate query to get column metadata for a table.
 *
 * This is used to generate a PostgreSQL table definition for
 * IMPORT FOREIGN SCHEMA.
 *
 * TODO:
 *  - verify all types can be converted to their PostgreSQL equivalents
 */
char *
_dataTypeSQL(char *table_name)
{
	StringInfoData data_type_sql;

	initStringInfo(&data_type_sql);
	appendStringInfo(&data_type_sql,
"   SELECT TRIM(LOWER(rf.rdb$field_name)) AS column_name,\n"
"          f.rdb$field_type, \n"
"          CASE f.rdb$field_type\n"
"            WHEN 261 THEN \n"
"              CASE f.rdb$field_sub_type \n"
"                WHEN 1 THEN 'TEXT' \n"
"                ELSE 'BYTEA' \n"
"              END \n"
"            WHEN 14  THEN 'CHAR(' || f.rdb$field_length|| ')'\n"
"            WHEN 40  THEN 'CSTRING'\n"
"            WHEN 11  THEN 'D_FLOAT'\n"
"            WHEN 27  THEN 'DOUBLE PRECISION'\n"
"            WHEN 10  THEN 'FLOAT'\n"
"            WHEN 16  THEN \n"
"              CASE f.rdb$field_sub_type \n"
"                WHEN 1 THEN 'NUMERIC(' || f.rdb$field_precision || ',' || (-f.rdb$field_scale) || ')' \n"
"                WHEN 2 THEN 'DECIMAL(' || f.rdb$field_precision || ',' || (-f.rdb$field_scale) || ')' \n"
"                ELSE 'BIGINT' \n"
"              END \n"
"            WHEN 8   THEN \n"
"              CASE f.rdb$field_sub_type \n"
"                WHEN 1 THEN 'NUMERIC(' || f.rdb$field_precision || ',' || (-f.rdb$field_scale) || ')' \n"
"                WHEN 2 THEN 'DECIMAL(' || f.rdb$field_precision || ',' || (-f.rdb$field_scale) || ')' \n"
"                ELSE 'INTEGER' \n"
"              END \n"
"            WHEN 9   THEN 'QUAD'\n"
"            WHEN 7   THEN \n"
"              CASE f.rdb$field_sub_type \n"
"                WHEN 1 THEN 'NUMERIC(' || f.rdb$field_precision || ',' || (-f.rdb$field_scale) || ')' \n"
"                WHEN 2 THEN 'DECIMAL(' || f.rdb$field_precision || ',' || (-f.rdb$field_scale) || ')' \n"
"                ELSE 'SMALLINT' \n"
"              END \n"
"            WHEN 12  THEN 'DATE'\n"
"            WHEN 13  THEN 'TIME'\n"
"            WHEN 35  THEN 'TIMESTAMP'\n"
"            WHEN 37  THEN 'VARCHAR(' || f.rdb$field_length|| ')'\n"
"            WHEN 23  THEN 'BOOLEAN' \n"
"            ELSE 'UNKNOWN'\n"
"          END AS data_type,\n"
"         COALESCE(CAST(rf.rdb$default_source AS VARCHAR(80)), '') \n"
"           AS \"Default value\", \n"
"         rf.rdb$null_flag AS null_flag, \n"
"         COALESCE(CAST(rf.rdb$description AS VARCHAR(80)), '') \n"
"           AS \"Description\" \n"
"      FROM rdb$relation_fields rf \n"
" LEFT JOIN rdb$fields f \n"
"        ON rf.rdb$field_source = f.rdb$field_name\n"
"     WHERE TRIM(LOWER(rf.rdb$relation_name)) = LOWER('%s')\n"
"  ORDER BY rf.rdb$field_position\n",
					 table_name
		);

	return data_type_sql.data;
}
#endif
