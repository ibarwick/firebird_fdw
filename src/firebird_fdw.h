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
#if (PG_VERSION_NUM >= 120000)
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif
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

#define FIREBIRD_FDW_VERSION 10300
#define FIREBIRD_FDW_VERSION_STRING "1.3.0"

#define FB_FDW_LOGPREFIX "[firebird_fdw] "
#define FB_FDW_LOGPREFIX_LEN strlen(FB_FDW_LOGPREFIX)

/* http://www.firebirdfaq.org/faq259/ */
#define FIREBIRD_DEFAULT_PORT 3050

/*
 * In PostgreSQL 11 and earlier, "table_open|close()" were "heap_open|close()";
 * see core commits 4b21acf5 and f25968c4.
 */
#if PG_VERSION_NUM < 120000
#define table_open(x, y) heap_open(x, y)
#define table_close(x, y) heap_close(x, y)
#endif

#if (PG_VERSION_NUM >= 140000)
#define NO_BATCH_SIZE_SPECIFIED -1
#endif

/*
 * Macro to indicate if a given PostgreSQL datatype can be
 * converted to a Firebird type
 */
#define canConvertPgType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
							 || (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
							 || (x) == INT4OID ||  (x) == FLOAT4OID || (x) == FLOAT8OID \
							 || (x) == NUMERICOID || (x) == DATEOID || (x) == TIMESTAMPOID \
							 || (x) == TIMEOID)

typedef union opttype {
	char **strptr;
	int *intptr;
	bool *boolptr;
} opttype;

typedef struct fbServerOpt {
	union opttype opt;
	bool provided;
} fbServerOpt;

typedef struct fbServerOptions {
	fbServerOpt address;
	fbServerOpt port;
	fbServerOpt database;
	fbServerOpt disable_pushdowns;
	fbServerOpt updatable;
	fbServerOpt quote_identifiers;
	fbServerOpt implicit_bool_type;
#if (PG_VERSION_NUM >= 140000)
	fbServerOpt batch_size;
#endif
} fbServerOptions;

#if (PG_VERSION_NUM >= 140000)
#define fbServerOptions_init { \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false } \
}
#else
#define fbServerOptions_init { \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false }, \
	{ { NULL }, false } \
}
#endif


typedef struct fbTableOptions {
	char **query;
	char **table_name;
	bool *updatable;
	int *estimated_row_count;
	bool *quote_identifier;
#if (PG_VERSION_NUM >= 140000)
	int *batch_size;
#endif
} fbTableOptions;

#if (PG_VERSION_NUM >= 140000)
#define fbTableOptions_init { \
	NULL, \
	NULL, \
	NULL, \
	NULL, \
	NULL, \
	NULL \
}
#else
#define fbTableOptions_init { \
	NULL, \
	NULL, \
	NULL, \
	NULL, \
	NULL \
}
#endif

typedef struct fbColumnOptions {
	char **column_name;
	bool *quote_identifier;
	bool *implicit_bool_type;
} fbColumnOptions;

#define fbColumnOptions_init { \
	NULL, \
	NULL, \
	NULL \
}

typedef struct fbTableColumn
{
	bool isdropped;			 /* indicate if PostgreSQL column is dropped */
	bool used;				 /* indicate if column used in current query */
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
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};


/*
 * FDW-specific information for RelOptInfo.fdw_private and ForeignScanState.fdw_state.
 *
 * This is what will be set and stashed away in fdw_private and fetched
 * for subsequent routines.
 */
typedef struct FirebirdFdwState
{
	char	   *svr_query;
	char	   *svr_table;
	bool		disable_pushdowns;	 /* true if server option "disable_pushdowns" supplied */
	int			estimated_row_count; /* set if server option "estimated_row_count" provided */
	bool		quote_identifier;
	bool		implicit_bool_type;	 /* true if server option "implicit_bool_type" supplied */
#if (PG_VERSION_NUM >= 140000)
	int			batch_size;
#endif

	FBconn	   *conn;
	int			firebird_version; /* cache Firebird version from connection */

	List	   *remote_conds;
	List	   *local_conds;

	Bitmapset  *attrs_used;			/* Bitmap of attr numbers to be fetched from the remote server. */
	Cost		startup_cost;		/* cost estimate, only needed for planning */
	Cost		total_cost;			/* cost estimate, only needed for planning */
	int			row;
	char	   *query;				/* query to send to Firebird */
} FirebirdFdwState;

/*
 * Execution state of a foreign scan using firebird_fdw.
 */
typedef struct FirebirdFdwScanState
{
	FBconn	   *conn;
	/* Foreign table information */
	fbTable	   *table;
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */
	/* Query information */
	char	   *query;				/* query to send to Firebird */
	bool		db_key_used;		/* indicate whether RDB$DB_KEY was requested */

	FBresult   *result;
	int			row;

} FirebirdFdwScanState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct FirebirdFdwModifyState
{
	Relation	rel;			   /* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	   /* attribute datatype conversion metadata */

	/* for remote query execution */
	FBconn		 *conn;			   /* connection for the scan */
	int			  firebird_version; /* cache Firebird version from connection */
	/* extracted fdw_private data */
	char		 *query;		   /* text of INSERT/UPDATE/DELETE command */
	List		 *target_attrs;	   /* list of target attribute numbers */
	bool		  has_returning;   /* is there a RETURNING clause? */
	List		 *retrieved_attrs; /* attr numbers retrieved by RETURNING */

	/* info about parameters for prepared statement */
	AttrNumber	  db_keyAttno_CtidPart;	 /* attnum of input resjunk rdb$db_key column (CTID part) */
	AttrNumber	  db_keyAttno_XmaxPart;	 /* attnum of input resjunk rdb$db_key column (xmax part) */

	int			  p_nums;		  /* number of parameters to transmit */
	FmgrInfo	 *p_flinfo;		  /* output conversion functions for them */

	/* working memory context */
	MemoryContext temp_cxt;		  /* context for per-tuple temporary data */

#if (PG_VERSION_NUM >= 140000)
	int			batch_size;
#endif
} FirebirdFdwModifyState;


extern void fbSigInt(SIGNAL_ARGS);

/* connection functions (in connection.c) */


extern FBconn *firebirdInstantiateConnection(ForeignServer *server, UserMapping *user);
extern void firebirdCloseConnections(bool verbose);
extern int firebirdCachedConnectionsCount(void);
extern void fbfdw_report_error(int errlevel, int pg_errcode, FBresult *res, FBconn *conn, char *query);


/* option functions (in options.c) */

extern void firebirdGetServerOptions(ForeignServer *server,
									 fbServerOptions *options);

extern void firebirdGetTableOptions(ForeignTable *table,
									fbTableOptions *options);

extern void firebirdGetColumnOptions(Oid foreigntableid, int varattno,
									 fbColumnOptions *options);

/* query-building functions (in convert.c) */

extern void buildInsertSql(StringInfo buf,
						   RangeTblEntry *rte,
						   FirebirdFdwState *fdw_state,
						   Index rtindex, Relation rel,
						   List *targetAttrs, List *returningList,
						   List **retrieved_attrs);

extern void buildUpdateSql(StringInfo buf, RangeTblEntry *rte,
						   FirebirdFdwState *fdw_state,
						   Index rtindex, Relation rel,
						   List *targetAttrs, List *returningList,
						   List **retrieved_attrs);

extern void buildDeleteSql(StringInfo buf, RangeTblEntry *rte,
						   FirebirdFdwState *fdw_state,
						   Index rtindex, Relation rel,
						   List *returningList,
						   List **retrieved_attrs);

extern void buildSelectSql(StringInfo buf, RangeTblEntry *rte,
						   FirebirdFdwState *fdw_state,
						   RelOptInfo *baserel,
						   Bitmapset *attrs_used,
						   List **retrieved_attrs,
						   bool *db_key_used);

extern void buildWhereClause(StringInfo buf,
							 PlannerInfo *root,
							 RelOptInfo *baserel,
							 List *exprs,
							 bool is_first,
							 List **params);

extern void
identifyRemoteConditions(PlannerInfo *root,
						 RelOptInfo *baserel,
						 List **remote_conds,
						 List **local_conds,
						 bool disable_pushdowns,
						 int firebird_version);

extern bool
isFirebirdExpr(PlannerInfo *root,
			   RelOptInfo *baserel,
			   Expr *expr,
			   int firebird_version);

void convertColumnRef(StringInfo buf,
					  Oid relid,
					  int varattno,
					  bool quote_identifier);

const char *
quote_fb_identifier(const char *ident, bool quote_ident);

void
unquoted_ident_to_upper(char *ident);

#if (PG_VERSION_NUM >= 90500)
void
convertFirebirdObject(char *server_name, char *schema, char *object_name, char object_type, char *pg_name, bool import_not_null, bool updatable, FBresult *colres, StringInfoData *create_table);
extern char *
_dataTypeSQL(char *table_name);
#endif

#endif	 /* FIREBIRD_FDW_H */
