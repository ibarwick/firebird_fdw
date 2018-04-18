/*-------------------------------------------------------------------------
 *
 * connection.c
 *
 * Connection management functions for firebird_fdw
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "firebird_fdw.h"

#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"



typedef struct ConnCacheKey
{
	Oid			serverid;		/* OID of foreign server */
	Oid			userid;			/* OID of local user whose mapping we use */
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	FBconn	   *conn;			/* connection to foreign server, or NULL */
	int			xact_depth;		/* 0 = no xact open, 1 = main xact open, 2 =
								 * one level of subxact open, etc */
	bool		have_error;		/* have any subxacts aborted in this xact? */
} ConnCacheEntry;

/*
 * Global connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* tracks whether any work is needed in callback functions */
static bool xact_got_connection = false;


static char *firebirdDbPath(char **address, char **database, int *port);
static FBconn *firebirdGetConnection(char *dbpath, char *svr_username, char *svr_password);
static void fb_begin_remote_xact(ConnCacheEntry *entry);
static void fb_xact_callback(XactEvent event, void *arg);
static void fb_subxact_callback(SubXactEvent event,
					   SubTransactionId mySubid,
					   SubTransactionId parentSubid,
					   void *arg);

/**
 * firebirdGetConnection()
 *
 * Establish DB connection
 */
static FBconn *
firebirdGetConnection(char *dbpath, char *svr_username, char *svr_password)
{
	FBconn *volatile conn;
	const char *kw[5];
	const char *val[5];
	int i = 0;

	kw[i] = "db_path";
	val[i] = dbpath;
	i++;

	kw[i] = "user";
	val[i] = svr_username;
	i++;

	kw[i] = "password";
	val[i] = svr_password;
	i++;

	/* XXX I am taking a calculated risk and passing the PostgreSQL
	 * encoding name directly to Firebird. Firebird seems to be pretty
	 * good at parsing encoding names but I can't rule out errors
	 * with more obscure encoding combinations.
	 */
	kw[i] = "client_encoding";
	val[i] = GetDatabaseEncodingName();
	i++;

	kw[i] = NULL;
	val[i] = NULL;

	conn = FQconnectdbParams(kw, val);

	if (FQstatus(conn) != CONNECTION_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("Unable to to connect to foreign server"),
				 errdetail("%s", FQerrorMessage(conn))));

	FQsetAutocommit(conn, false);
	conn->client_min_messages = DEBUG2;

	elog(DEBUG2, "%s(): DB connection OK", __func__);

	return conn;
}


/**
 * firebirdInstantiateConnection()
 *
 * Connect to the foreign database using the foreign server parameters
 */
FBconn *
firebirdInstantiateConnection(ForeignServer *server, UserMapping *user)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	/* set up connection cache */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		elog(DEBUG2, "%s(): instantiating conn cache", __func__);

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hash = tag_hash;

		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("firebird_fdw connections", 8,
									 &ctl,
									 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		/* Set up transaction callbacks */
		RegisterXactCallback(fb_xact_callback, NULL);
		RegisterSubXactCallback(fb_subxact_callback, NULL);
	}

	/* Set flag that we did GetConnection during the current transaction */
	xact_got_connection = true;
	/* Create hash key for the entry.  Assume no pad bytes in key struct. */
	key.serverid = server->serverid;
	key.userid = user->userid;

	/* Find or create cached entry for requested connection */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/* initialize new hashtable entry */
		entry->conn = NULL;
		entry->xact_depth = 0;
		entry->have_error = false;
	}

	if (entry->conn == NULL)
	{
		char *svr_address  = NULL;
		char *svr_database = NULL;
		char *svr_username = NULL;
		char *svr_password = NULL;
		int	  svr_port	   = 0;
		char *dbpath;
		ListCell   *lc;

		elog(DEBUG2, "%s(): no cache entry found", __func__);

		entry->xact_depth = 0;	/* just to be sure */
		entry->have_error = false;

		foreach (lc, server->options)
		{
			DefElem	   *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "address") == 0)
				svr_address = defGetString(def);
			if (strcmp(def->defname, "database") == 0)
				svr_database = defGetString(def);
		}

		foreach (lc, user->options)
		{
			DefElem	   *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "username") == 0)
				svr_username = defGetString(def);
			if (strcmp(def->defname, "password") == 0)
				svr_password = defGetString(def);
		}

		dbpath = firebirdDbPath(&svr_address, &svr_database, &svr_port);

		entry->conn = firebirdGetConnection(
			dbpath,
			svr_username,
			svr_password
		);


		elog(DEBUG2, "%s(): new firebird_fdw connection %p for server \"%s\"",
			 __func__,entry->conn, server->servername);
	}
	else
	{
		elog(DEBUG2, "%s(): cache entry %p found",
			 __func__, entry->conn);

		/*
		 * Connection is not valid - reconnect.
		 *
		 * XXX if we're in a transaction we should roll back as the Firebird state will be lost
		 */
		if (FQstatus(entry->conn) == CONNECTION_BAD)
		{
			FBconn *new_conn = FQreconnect(entry->conn);

			elog(WARNING, "Firebird server connection has gone away");

			/* XXX do we need to reset entry->xact_depth? */
			elog(DEBUG2, "xact_depth: %i", entry->xact_depth);

			new_conn = firebirdGetConnection(
				FQdb_path(entry->conn),
				FQuname(entry->conn),
				FQupass(entry->conn));

			FQfinish(entry->conn);
			entry->conn = new_conn;
			ereport(NOTICE,
					(errmsg("reconnected to Firebird server")));
		}
	}


	pqsignal(SIGINT, fbSigInt);

	/* Start a new transaction or subtransaction if needed */
	fb_begin_remote_xact(entry);

	return entry->conn;
}



/**
 * fb_begin_remote_xact()
 *
 * Start remote transaction or subtransaction, if needed.
 *
 * Firebird's transaction levels are somewhat differen to PostgreSQL's.
 * Currently we are using "SET TRANSACTION SNAPSHOT", which is roughly
 * equivalent to SERIALIZABLE. We'll probably need to reexamine this at
 * some point.
 *
 * XXX need to improve error handling
 *
 * See also:
 *  - http://www.firebirdsql.org/manual/isql-transactions.html
 *  - http://www.firebirdsql.org/refdocs/langrefupd25-set-trans.html
 */
static void
fb_begin_remote_xact(ConnCacheEntry *entry)
{
	FBresult *res;
	int			curlevel = GetCurrentTransactionNestLevel();

	elog(DEBUG2, "fb_begin_remote_xact(): xact depth: %i", entry->xact_depth);

	/* Start main transaction if we haven't yet */
	if (entry->xact_depth <= 0)
	{
		elog(DEBUG2, "starting remote transaction on connection %p",
			 entry->conn);

		res = FQexec(entry->conn, "SET TRANSACTION SNAPSHOT");

		if (FQresultStatus(res) != FBRES_TRANSACTION_START)
		{
			/* XXX better error handling here */
			elog(ERROR, "unable to execute SET TRANSACTION SNAPSHOT: %s", FQresultErrorMessage(res));
		}

		FQclear(res);

		entry->xact_depth = 1;
	}
	else
	{
		if (FQisActiveTransaction(entry->conn))
			elog(DEBUG2, "%s(): xact_depth > 0, active transaction",
				 __func__);
		else
			elog(DEBUG2, "%s(): xact_depth > 0, no active transaction!",
				 __func__);
	}

	/*
	 * If we're in a subtransaction, stack up savepoints to match our level.
	 * This ensures we can rollback just the desired effects when a
	 * subtransaction aborts.
	 */
	while (entry->xact_depth < curlevel)
	{
		char		sql[64];

		snprintf(sql, sizeof(sql), "SAVEPOINT s%d", entry->xact_depth + 1);
		res = FQexec(entry->conn, sql);
		elog(DEBUG2, "savepoint:\n%s", sql);
		elog(DEBUG2, "res is %s", FQresStatus(FQresultStatus(res)));
		FQclear(res);

		entry->xact_depth++;
	}
}


/**
 * fb_xact_callback()
 */
static void
fb_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	elog(DEBUG3, "entering function %s", __func__);

	/* Connection has no transactions - do nothing */
	if (!xact_got_connection)
		return;

	/*
	 * Scan all connection cache entries and close any open remote transactions
	 */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{

		elog(DEBUG3, "closing remote transaction on connection %p",
			 entry->conn);

		/* We only care about connections with open remote transactions */
		if (entry->conn == NULL)
		{
			elog(DEBUG3, "%s(): no connection",
				 __func__);
			continue;
		}
		else if (entry->xact_depth == 0)
		{
			elog(DEBUG3, "%s(): no open transaction",
				 __func__);
			continue;
		}

		/* This shouldn't happen, but log just in case */
		if (!FQisActiveTransaction(entry->conn))
		{
			elog(DEBUG3, "%s(): no active transaction",
				 __func__);
			continue;
		}

		switch (event)
		{
			case XACT_EVENT_PRE_COMMIT:
				elog(DEBUG2, "COMMIT");
				if (FQcommitTransaction(entry->conn) != TRANS_OK)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_ERROR),
							 errmsg("COMMIT failed")));
				}
				break;
			case XACT_EVENT_PRE_PREPARE:
				/* XXX not sure how to handle this */
				elog(DEBUG2, "PREPARE");
				break;
			case XACT_EVENT_COMMIT:
			case XACT_EVENT_PREPARE:
				/* Should not get here -- pre-commit should have handled it */
				elog(ERROR, "missed cleaning up connection during pre-commit");
				break;
			case XACT_EVENT_ABORT:
				if (FQrollbackTransaction(entry->conn) != TRANS_OK)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_ERROR),
							 errmsg("ROLLBACK failed")));
				}
#if (PG_VERSION_NUM >= 90500)
			case XACT_EVENT_PARALLEL_COMMIT:
			case XACT_EVENT_PARALLEL_ABORT:
			case XACT_EVENT_PARALLEL_PRE_COMMIT:
				/* XXX Handle these */
				elog(DEBUG2, "Unhandled XACT_EVENT_PARALLEL_* event");
				break;
#endif
		}

		/* Reset state to show we're out of a transaction */
		entry->xact_depth = 0;
	}
}


/**
 * fb_subxact_callback()
 */
static void
fb_subxact_callback(SubXactEvent event,
					SubTransactionId mySubid,
					SubTransactionId parentSubid,
					void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	int			curlevel;

	elog(DEBUG3, "entering function %s", __func__);

	/* Nothing to do at subxact start, nor after commit. */
	if (!(event == SUBXACT_EVENT_PRE_COMMIT_SUB ||
		  event == SUBXACT_EVENT_ABORT_SUB))
		return;

	/* Quick exit if no connections were touched in this transaction. */
	if (!xact_got_connection)
		return;

	/*
	 * Scan all connection cache entries to find open remote subtransactions
	 * of the current level, and close them.
	 */

	curlevel = GetCurrentTransactionNestLevel();
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		FBresult   *res;
		char		sql[100];

		/*
		 * We only care about connections with open remote subtransactions of
		 * the current level.
		 */
		if (entry->conn == NULL || entry->xact_depth < curlevel)
			continue;

		if (entry->xact_depth > curlevel)
			elog(ERROR, "missed cleaning up remote subtransaction at level %d",
				 entry->xact_depth);

		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		{
			/* Commit all remote subtransactions during pre-commit */
			snprintf(sql, sizeof(sql), "RELEASE SAVEPOINT s%d", curlevel);
			elog(DEBUG2, "%s(): %s", __func__, sql);
			res = FQexec(entry->conn, sql);
			elog(DEBUG2, "%s(): res %i", __func__, FQresultStatus(res));
		}
		else
		{
			/* Assume we might have lost track of prepared statements */
			entry->have_error = true;
			/* Rollback all remote subtransactions during abort */
			snprintf(sql, sizeof(sql),
					 "ROLLBACK TO SAVEPOINT s%d",
					 curlevel);
			res = FQexec(entry->conn, sql);
			if (FQresultStatus(res) != FBRES_COMMAND_OK)
			{
				elog(WARNING, "%s(): unable to execute '%s'",
					 __func__, sql);
				FQclear(res);
			}
			else
			{
				snprintf(sql, sizeof(sql),
						 "RELEASE SAVEPOINT s%d",
						 curlevel);
				res = FQexec(entry->conn, sql);
				if (FQresultStatus(res) != FBRES_COMMAND_OK)
				{
					elog(WARNING, "%s(): unable to execute '%s'",
						 __func__, sql);
				}
				FQclear(res);
			}
		}

		/* Leaving current subtransaction level */
		entry->xact_depth--;
	}
}


/**
 * firebirdCloseConnections()
 *
 * Close any open connections before exiting
 */
void
firebirdCloseConnections(void)
{
	HASH_SEQ_STATUS fstat;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&fstat, ConnectionHash);
	while ((entry = (ConnCacheEntry *)hash_seq_search(&fstat)) != NULL)
	{
		if (entry->conn == NULL)
			continue;
		elog(DEBUG2, "%s(): closing cached connection %p", __func__, entry->conn);
		FQfinish(entry->conn);
		entry->conn = NULL;
		elog(DEBUG2, "%s(): cached connection closed", __func__);
	}
}


/**
 * firebirdDbPath()
 *
 * Utility function to generate a Firebird database path.
 *
 * XXX ignores the 'port' parameter
 */
static char *
firebirdDbPath(char **address, char **database, int *port)
{
	char *hostname;
	if (*address != NULL)
	{
		hostname = palloc0(strlen(*address) + strlen(*database) + 2);
		sprintf(hostname, "%s:%s", *address, *database);
	}
	else
	{
		hostname = palloc0(strlen(*database) + 1);
		sprintf(hostname, "%s", *database);
	}

	return hostname;
}
