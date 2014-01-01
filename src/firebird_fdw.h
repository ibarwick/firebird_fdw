#ifndef FIREBIRD_FDW_H
#define FIREBIRD_FDW_H

#include "funcapi.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
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
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


#include "libfq.h"


#define FB_FDW_LOGPREFIX "[firebird_fdw] "
#define FB_FDW_LOGPREFIX_LEN strlen(FB_FDW_LOGPREFIX)

typedef struct fbTableColumn
{
    char *fbname;            /* Firebird column name */
    char *pgname;            /* PostgreSQL column name */
    int pgattnum;            /* PostgreSQL attribute number */
    Oid pgtype;              /* PostgreSQL data type */
    int pgtypmod;            /* PostgreSQL type modifier */
    bool isdropped;          /* indicate if PostgreSQL column is dropped */
} fbTableColumn;

typedef struct fbTable
{
    Oid foreigntableid;
    int pg_column_total;
    char *pg_table_name;
	fbTableColumn **columns;
} fbTable;


/*
 * Describes the valid options for objects that use this wrapper.
 */
struct FirebirdFdwOption
{
    const char *optname;
    Oid         optcontext;     /* Oid of catalog in which option may appear */
};


/*
 * FDW-specific information for RelOptInfo.fdw_private and ForeignScanState.fdw_state.
 *
 * This is what will be set and stashed away in fdw_private and fetched
 * for subsequent routines.
 */
typedef struct FirebirdFdwState
{
    /* Connection information */
    char    *svr_address;
    int      svr_port;           /* Server port (default: 3050) */
    char    *svr_database;

    char    *svr_username;       /* Firebird username */
    char    *svr_password;       /* Firebird password */

    char    *svr_query;
    char    *svr_table;
    char    *dbpath;
    FQconn  *conn;

    /* Foreign table information */
    fbTable *table;

    /* Query information */
    char *query;                   /* query to send to Firebird */

    Cost startup_cost;             /* cost estimate, only needed for planning */
    Cost total_cost;               /* cost estimate, only needed for planning */

    FQresult    *result;
    int         row;


} FirebirdFdwState;



/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct FirebirdFdwModifyState
{
    Relation    rel;            /* relcache entry for the foreign table */
    AttInMetadata *attinmeta;   /* attribute datatype conversion metadata */

    /* for remote query execution */
    FQconn     *conn;           /* connection for the scan */

    /* extracted fdw_private data */
    char       *query;          /* text of INSERT/UPDATE/DELETE command */
    List       *target_attrs;   /* list of target attribute numbers */
    bool        has_returning;  /* is there a RETURNING clause? */
    List       *retrieved_attrs;    /* attr numbers retrieved by RETURNING */

    /* info about parameters for prepared statement */
    AttrNumber  db_keyAttno_CtidPart;  /* attnum of input resjunk rdb$db_key column */
    AttrNumber  db_keyAttno_OidPart;   /* attnum of input resjunk rdb$db_key column (OID part)*/

    int         p_nums;         /* number of parameters to transmit */
    FmgrInfo   *p_flinfo;       /* output conversion functions for them */

    /* working memory context */
    MemoryContext temp_cxt;     /* context for per-tuple temporary data */
} FirebirdFdwModifyState;


/* query-building functions (in deparse.c) */

extern void buildInsertSql(StringInfo buf, PlannerInfo *root,
                 Index rtindex, Relation rel,
                 List *targetAttrs, List *returningList,
                 List **retrieved_attrs);

extern void buildUpdateSql(StringInfo buf, PlannerInfo *root,
                 Index rtindex, Relation rel,
                 List *targetAttrs, List *returningList,
                 List **retrieved_attrs);

extern void buildDeleteSql(StringInfo buf, PlannerInfo *root,
                           Index rtindex, Relation rel,
                           List *returningList,
                           List **retrieved_attrs);

#endif   /* FIREBIRD_FDW_H */
