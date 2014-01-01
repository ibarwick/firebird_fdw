#include "postgres.h"


#include "access/heapam.h"
#include "access/htup_details.h"
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

#include "firebird_fdw.h"

static void deparseColumnRef(StringInfo buf, int varno, int varattno,
                 PlannerInfo *root);
static void deparseRelation(StringInfo buf, Relation rel);
static void deparseReturningList(StringInfo buf, PlannerInfo *root,
                     Index rtindex, Relation rel,
                     List *returningList,
                     List **retrieved_attrs);
static void deparseTargetList(StringInfo buf,
                  PlannerInfo *root,
                  Index rtindex,
                  Relation rel,
                  Bitmapset *attrs_used,
                  List **retrieved_attrs);

/**
 * buildInsertSql()
 *
 * Build remote INSERT statement
 */
void
buildInsertSql(StringInfo buf, PlannerInfo *root,
               Index rtindex, Relation rel,
               List *targetAttrs, List *returningList,
               List **retrieved_attrs)
{
    bool        first;
    ListCell   *lc;

    appendStringInfoString(buf, "INSERT INTO ");
    deparseRelation(buf, rel);
    appendStringInfoString(buf, " (");
    first = true;
    foreach(lc, targetAttrs)
    {
        int         attnum = lfirst_int(lc);

        if (!first)
            appendStringInfoString(buf, ", ");
        first = false;

        deparseColumnRef(buf, rtindex, attnum, root);
    }

    appendStringInfoString(buf, ")\n VALUES (");

    first = true;
    foreach(lc, targetAttrs)
    {
        if (!first)
            appendStringInfoString(buf, ", ");
        first = false;

        appendStringInfoString(buf, "?");
    }

    appendStringInfoString(buf, ")");

    if (returningList)
        deparseReturningList(buf, root, rtindex, rel, returningList,
                             retrieved_attrs);
    else
        *retrieved_attrs = NIL;
}


/**
 * buildUpdateSql()
 *
 * Build remote UPDATE statement
 */
void
buildUpdateSql(StringInfo buf, PlannerInfo *root,
               Index rtindex, Relation rel,
               List *targetAttrs, List *returningList,
               List **retrieved_attrs)
{
    bool        first;
    ListCell   *lc;

    appendStringInfoString(buf, "UPDATE ");
    deparseRelation(buf, rel);
    appendStringInfoString(buf, " SET ");

    first = true;
    foreach(lc, targetAttrs)
    {
        int attnum = lfirst_int(lc);

        if (!first)
            appendStringInfoString(buf, ", ");
        first = false;

        deparseColumnRef(buf, rtindex, attnum, root);
        appendStringInfo(buf, " = ?");
    }
    appendStringInfoString(buf, " WHERE rdb$db_key = ?");

    if (returningList)
        deparseReturningList(buf, root, rtindex, rel, returningList,
                             retrieved_attrs);
    else
        *retrieved_attrs = NIL;
}


/**
 * buildDeleteSql()
 *
 * build remote DELETE statement
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
    deparseRelation(buf, rel);
    appendStringInfoString(buf, " WHERE rdb$db_key = ?");

    if (returningList)
        deparseReturningList(buf, root, rtindex, rel, returningList,
                             retrieved_attrs);
    else
        *retrieved_attrs = NIL;
}



/**
 * deparseColumnRef()
 *
 * Construct name to use for given column, and emit it into 'buf'.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
deparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo *root)
{
    RangeTblEntry *rte;
    char       *colname = NULL;
    List       *options;
    ListCell   *lc;

    /* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
    Assert(!IS_SPECIAL_VARNO(varno));

    /* Get RangeTblEntry from array in PlannerInfo. */
    rte = planner_rt_fetch(varno, root);

    /*
     * If it's a column of a foreign table, and it has the column_name FDW
     * option, use that value
     * XXX not yet implemented
     */
    options = GetForeignColumnOptions(rte->relid, varattno);
    foreach(lc, options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "column_name") == 0)
        {
            colname = defGetString(def);
            break;
        }
    }

    /*
     * If it's a column of a regular table or it doesn't have column_name FDW
     * option, use attribute name.
     */
    if (colname == NULL)
        colname = get_relid_attribute_name(rte->relid, varattno);

    appendStringInfoString(buf, quote_identifier(colname));
}


/**
 * deparseRelation()
 *
 * Append remote name of specified foreign table to 'buf'.
 * Firebird does not have schemas, so we will only return the table
 * name itself.
 */
static void
deparseRelation(StringInfo buf, Relation rel)
{
    ForeignTable *table;
    const char *relname = NULL;
    ListCell   *lc;

    /* obtain additional catalog information. */
    table = GetForeignTable(RelationGetRelid(rel));

    /* If remote table name defined in the 'table' option provided, use this
       instead of the PostgreSQL name
     */
    foreach(lc, table->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, "table") == 0)
            relname = defGetString(def);
    }


    if (relname == NULL)
        relname = RelationGetRelationName(rel);

    appendStringInfo(buf, "%s",
                     quote_identifier(relname));
}


/**
 * deparseReturningList()
 *
 * Generate RETURNING clause of a INSERT/UPDATE/DELETE ... RETURNING
 * statement.
 */
static void
deparseReturningList(StringInfo buf, PlannerInfo *root,
                     Index rtindex, Relation rel,
                     List *returningList,
                     List **retrieved_attrs)
{
    Bitmapset  *attrs_used;

    /* Insert column names into the query's RETURNING list */
    attrs_used = NULL;
    pull_varattnos((Node *) returningList, rtindex,
                   &attrs_used);

    appendStringInfoString(buf, " RETURNING ");
    deparseTargetList(buf, root, rtindex, rel, attrs_used,
                      retrieved_attrs);
}


/**
 * deparseTargetList()
 *
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 */
static void
deparseTargetList(StringInfo buf,
                  PlannerInfo *root,
                  Index rtindex,
                  Relation rel,
                  Bitmapset *attrs_used,
                  List **retrieved_attrs)
{
    TupleDesc   tupdesc = RelationGetDescr(rel);
    bool        have_wholerow;
    bool        first;
    int         i;

    *retrieved_attrs = NIL;

    /* If there's a whole-row reference, we'll need all the columns. */
    have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
                                  attrs_used);

    first = true;
    for (i = 1; i <= tupdesc->natts; i++)
    {
        Form_pg_attribute attr = tupdesc->attrs[i - 1];

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

            deparseColumnRef(buf, rtindex, i, root);

            *retrieved_attrs = lappend_int(*retrieved_attrs, i);
        }
    }

    /* Add RDB$DB_KEY if needed */
    if (bms_is_member(SelfItemPointerAttributeNumber - FirstLowInvalidHeapAttributeNumber,
                      attrs_used))
    {
        if (!first)
            appendStringInfoString(buf, ", ");
        first = false;

        appendStringInfoString(buf, "RDB$DB_KEY");

        *retrieved_attrs = lappend_int(*retrieved_attrs,
                                       SelfItemPointerAttributeNumber);
    }

    /* Avoid generating invalid syntax if no undropped columns exist */
    if (first)
        appendStringInfoString(buf, "NULL");
}
