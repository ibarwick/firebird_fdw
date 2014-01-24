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

#include "utils/hsearch.h"
#include "utils/memutils.h"



typedef struct ConnCacheKey
{
    Oid         serverid;       /* OID of foreign server */
    Oid         userid;         /* OID of local user whose mapping we use */
} ConnCacheKey;

typedef struct ConnCacheEntry
{
    ConnCacheKey key;           /* hash key (must be first) */
    FQconn     *conn;           /* connection to foreign server, or NULL */
} ConnCacheEntry;

/*
 * Global connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;


static char *firebirdDbPath(char **address, char **database, int *port);
static FQconn *firebirdGetConnection(char *dbpath, char *svr_username, char *svr_password);

/**
 * firebirdGetConnection()
 *
 * Establish DB connection
 */
static FQconn *
firebirdGetConnection(char *dbpath, char *svr_username, char *svr_password)
{
    FQconn *volatile conn;

    conn = FQconnect(
        dbpath,
        svr_username,
        svr_password
        );

    if(FQstatus(conn) != CONNECTION_OK)
        ereport(ERROR,
            (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
            errmsg("Unable to to connect to foreign server")
            ));

    /* Transaction support not yet implemented... */
    conn->autocommit = true;
    conn->client_min_messages = DEBUG2;

    elog(DEBUG2, "%s(): DB connection OK", __func__);

    return conn;
}


/**
 * firebirdInstantiateConnection()
 *
 * Connect to the foreign database using the foreign server parameters
 */
FQconn *
firebirdInstantiateConnection(ForeignServer *server, UserMapping *user)
{
    bool        found;
    ConnCacheEntry *entry;
    ConnCacheKey key;

    /* set up connection cache */
    if (ConnectionHash == NULL)
    {
        HASHCTL     ctl;

        elog(DEBUG2, "%s(): instantiating conn cache", __func__);

        MemSet(&ctl, 0, sizeof(ctl));
        ctl.keysize = sizeof(ConnCacheKey);
        ctl.entrysize = sizeof(ConnCacheEntry);
        ctl.hash = tag_hash;

        ctl.hcxt = CacheMemoryContext;
        ConnectionHash = hash_create("firebird_fdw connections", 8,
                                     &ctl,
                                     HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }

    /* Create hash key for the entry.  Assume no pad bytes in key struct */
    key.serverid = server->serverid;
    key.userid = user->userid;

    /*
     * Find or create cached entry for requested connection.
     */
    entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
    if (!found)
    {
       /* initialize new hashtable entry */
        entry->conn = NULL;
    }

    if (entry->conn == NULL)
    {
        char *svr_address  = NULL;
        char *svr_database = NULL;
        char *svr_username = NULL;
        char *svr_password = NULL;
        int   svr_port     = 0;
        char *dbpath;
        ListCell   *lc;

        elog(DEBUG2, "%s(): no cache entry found", __func__);

        foreach(lc, server->options)
        {
            DefElem    *def = (DefElem *) lfirst(lc);

            if (strcmp(def->defname, "address") == 0)
                svr_address = defGetString(def);
            if (strcmp(def->defname, "database") == 0)
                svr_database = defGetString(def);
        }

        foreach(lc, user->options)
        {
            DefElem    *def = (DefElem *) lfirst(lc);

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
    }

    return entry->conn;
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

    if(ConnectionHash == NULL)
        return;

    hash_seq_init(&fstat, ConnectionHash);
    while ((entry = (ConnCacheEntry *)hash_seq_search(&fstat)) != NULL)
    {
        if(entry->conn == NULL)
            continue;
        elog(DEBUG2, "%s(): closing cached connection %p", __func__, entry->conn);
        FQfinish(entry->conn);
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
    if(*address != NULL)
    {
        hostname = palloc(strlen(*address) + strlen(*database) + 1);
        sprintf(hostname, "%s:%s", *address, *database);
    }
    else
    {
        hostname = palloc(strlen(*database) + 1);
        sprintf(hostname, "%s", *database);
    }

    return hostname;
}
