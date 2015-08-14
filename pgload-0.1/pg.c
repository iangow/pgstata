// License blurb {{{

/*
    pgload - routines for connecting Postgres databases to Stata
    Copyright (C) 2007 Andrew Chadwick

    This program is free software: you can redistribute it and/or modify it
    under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or (at your
    option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.
    
    You should have received a copy of the GNU General Public License and the
    GNU Lesser General Public License along with this program.  If not, see
    <http://www.gnu.org/licenses/>.
*/

/*
    Code is included IN MODIFIED FORM from Apache 1.3, a work which is
    distributed under the terms of the Apache software license, version 2.0.
    Relevant attributions, in accordance with section 4(b) of that license:

     * Copyright 1999-2004 The Apache Software Foundation

     * util_date.c: date parsing utility routines
     *     These routines are (hopefully) platform-independent.
     * 
     * 27 Oct 1996  Roy Fielding
     *     Extracted (with many modifications) from mod_proxy.c and
     *     tested with over 50,000 randomly chosen valid date strings
     *     and several hundred variations of invalid date strings.

    Note that this program as a whole is licensed under the terms of the GNU
    Lesser General Public License 3.0 in accordance with section 4 final
    paragraph of the Apache License version 2.0.

 */

// }}}
// Settings and constants {{{

/* The speed at which this thing operates - over a network connection, at
 * least - seems to be remarkably independent of the SLURP_ROWS setting. This
 * setting does control memory usage, however, so set this to something not
 * too high. */

#define PGSTATA_CURSOR_SLURP_ROWS 10000

/* Database string format for dates: used by strptime(). */

#define PGSTATA_PG_DATE_FORMAT "%Y-%m-%d"

/* Return codes */

typedef enum _pgstata_rc {
    pgstata_ok = 0,
    pgstata_finished = 1,
    pgstata_usage_error = 198,
    pgstata_db_error = 200
} pgstata_rc;

// }}}
// Common blocks of code {{{

#define USAGE_CHECK(ARG_COUNT, MIN_ARGS, MAX_ARGS, USAGE)    \
    if ((ARG_COUNT < MIN_ARGS)                               \
        || (MAX_ARGS > -1 && ARG_COUNT > MAX_ARGS)) {        \
        SF_error("usage: " USAGE "\n");                      \
        return pgstata_usage_error;                          \
    }
#define PGCONN_CHECK(DEBUG_MODE)                            \
    if (PQstatus(pgstata_conn) != CONNECTION_OK) {          \
        SF_error("Database error: connection failed.\n");   \
        SF_error(PQerrorMessage(pgstata_conn));             \
        pgstata_teardown(DEBUG_MODE);                       \
        return pgstata_db_error;                            \
    }
#define PGRESULT_CHECK(RESULT, EXPECTED, DEBUG_MODE) \
    if (PQresultStatus(RESULT) != EXPECTED) {        \
        SF_error(PQresultErrorMessage(RESULT));      \
        PQclear(RESULT);                             \
        RESULT = NULL;                               \
        pgstata_cleanup(DEBUG_MODE);                 \
        return pgstata_db_error;                     \
    }

// }}}
// Library inclusions {{{

/* For struct tm and strptime() */
#define _XOPEN_SOURCE
#define __USE_XOPEN
#include <time.h>

/* Standard string ops and conversions */
#include <stdlib.h>
#include <string.h>

/* Postgres client operations */
#include <libpq-fe.h>

/* Server dev stuff: included for Postgres OID type definitions only. */
#include <postgres.h>
#include <catalog/pg_type.h>

/* Stata */
#define SD_FASTMODE
#include "stplugin.h"

// }}}
// Globals {{{

// Postgres connection, current query, and state
PGconn   *pgstata_conn = NULL;
PGresult *pgstata_res  = NULL;
int       pgstata_in_transaction = 0;
int       pgstata_num_obs_loaded = 0;
int       pgstata_num_obs = 0;

// Type information about columns
Oid *column_oids   = NULL;
int *column_widths = NULL;
int *column_mods   = NULL;

// }}}
// Helper funcs {{{


/*
 * Frees and reinitialises all globals except the one to do with the
 * connection, rolling back any open transaction. After this is called, the
 * db will still be connected and prepare() and friends can be called again.
 */

static inline void
pgstata_cleanup (const int debug_mode)
{
    if (column_oids != NULL) {
        free(column_oids);
        column_oids = NULL;
    }
    if (column_widths != NULL) {
        free(column_widths);
        column_widths = NULL;
    }
    if (column_mods != NULL) {
        free(column_mods);
        column_mods = NULL;
    }

    if (pgstata_in_transaction) {
        if (debug_mode) {
            SF_display("DEBUG: cleanup(): rolling back transaction\n");
        }
        char *rb_sql = "ROLLBACK TRANSACTION\n";
        if (debug_mode) {
            SF_display(rb_sql);
        }
        PGresult *rb_res = PQexec(pgstata_conn, rb_sql);
        if (PQresultStatus(rb_res) != PGRES_COMMAND_OK) {
            SF_error(PQresultErrorMessage(rb_res));
        }
        PQclear(rb_res);
        pgstata_in_transaction = 0;
    }
    if (pgstata_res != NULL) {
        if (debug_mode) {
            SF_display("DEBUG: cleanup(): freeing query result structs\n");
        }
        PQclear(pgstata_res);
        pgstata_res = NULL;
    }
}


/*
 * Like cleanup(), but also tears down the current database connection,
 * leaving the program in a state where connect() can be called.
 */

static inline void
pgstata_teardown (const int debug_mode) {
    pgstata_cleanup(debug_mode);
    if (pgstata_conn != NULL) {
        if (debug_mode) {
            SF_display("DEBUG: teardown(): ending connection\n");
        }
        PQfinish(pgstata_conn);
        pgstata_conn = NULL;
    }
}


/*
 * Converts a type OID to a human-readable type name.
 */

static inline void
pgstata_typoid2name (const Oid typoid, const size_t n, char *buf,
                     const int debug_mode)
{
    char tmpbuf[256];
    *tmpbuf = '\000';
    snprintf(tmpbuf, 255,
             "SELECT typname FROM pg_type WHERE oid=%d LIMIT 1\n",
             typoid);
    if (debug_mode) {
        SF_display(tmpbuf);
    }
    PGresult *res = PQexec(pgstata_conn, tmpbuf);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        SF_error("Internal error: type-name lookup failed.");
        SF_error(PQresultErrorMessage(res));
        *buf = '\000';
        strncpy(buf, "unknown", n);
    }
    else {
        char *tmpval = PQgetvalue(res, 0, 0);
        *buf = '\000';
        strncpy(buf, tmpval, n);
    }
    PQclear(res);
}


/*
  Converts a GMT struct tm into the number of days since 1 Jan 1960. tm_wday,
  tm_yday, and tm_dst are ignored.
 
  Code nicked from Apache 1.3.34's ap_tm2sec (util_date.c), with most of the
  checks relating to date validity removed.
*/
static inline long
pgstata_tm2statadate (const struct tm *t)
{
    int year;
    long days;

    static const int dayoffset[12] =
        {306, 337, 0, 31, 61, 92, 122, 153, 184, 214, 245, 275};

    year = t->tm_year + 100; // tm_year is YYYY-1900, we need YYYY-1800

    // if (year < 70 || ((sizeof(time_t) <= 4) && (year >= 138)))
    //    return BAD_DATE;

    /* shift new year to 1st March in order to make leap year calc easy */
    if (t->tm_mon < 2)
        year--;

    /* Find number of days since 1st March 1800 (in the Gregorian calendar). */
    days = year * 365 + year / 4 - year / 100 + (year / 100 + 2) / 4;
    days += dayoffset[t->tm_mon] + t->tm_mday - 1;
    //days -= 25508;            /* 1 jan 1970 is 25508 days since 1 mar 1900 */

    days -= 58379; // 1 Jan 1960 is 58379 days after 1 March 1800

    // No need to think about seconds, minutes, hours...
    //days = ((days * 24 + t->tm_hour) * 60 + t->tm_min) * 60 + t->tm_sec;

    return days;
}


// }}}
// Connect and disconnect {{{

pgstata_rc
pgstata_connect (int argc, char **argv) {
    USAGE_CHECK(argc, 1, 2, "connect CONNINFO [\"debug\"]");
    char *conninfo = argv[0];

    // Debugging mode
    int debug_mode = 0;
    if (argc >= 2) {
        if (strncasecmp(argv[1], "debug", 5) == 0) {
            debug_mode = 1;
        }
    }

    if (pgstata_conn != NULL) {
        SF_error("already connected: closing existing connection first\n");
        pgstata_teardown(debug_mode);
    }
    pgstata_conn = PQconnectdb(conninfo);
    PGCONN_CHECK(debug_mode);
    if (debug_mode) {
        SF_display("DEBUG: connected successfully\n");
    }
    return pgstata_ok;
}

pgstata_rc
pgstata_disconnect (int argc, char **argv) {
    USAGE_CHECK(argc, 0, 1, "disconnect [\"debug\"]");

    // Debugging mode
    int debug_mode = 0;
    if (argc >= 1) {
        if (strncasecmp(argv[0], "debug", 5) == 0) {
            debug_mode = 1;
        }
    }

    pgstata_teardown(debug_mode);
    return pgstata_ok;
}

// }}}
// Query prep {{{

// Prepare a workspace for pgstata_fetch() to populate: internals only.
// This needs to be executed by an ADO file which can read the macros
// it populates, and set up the appropriate variables and blank
// observations.

pgstata_rc
pgstata_prepare (int argc, char **argv) {
    USAGE_CHECK(argc, 1, 2, "prepare SQLQUERY [\"debug\"]");
    char *sql_query = argv[0];
    char msgtmp[256];
    *msgtmp = '\000';

    // Debugging mode
    int debug_mode = 0;
    if (argc >= 2) {
        if (strncasecmp(argv[1], "debug", 5) == 0) {
            debug_mode = 1;
        }
    }

    PGCONN_CHECK(debug_mode);
    if (SF_nobs() != 0) {
        SF_error("no; data in memory would be lost\n");
        return 4;   /* FIXME: de-hardcode, investigate if error message can
                       be sent automatically. */
    }

    PGresult *tmpres;
    const size_t tmpsql_buf_len = 1024;
    char tmpsql_buf[tmpsql_buf_len + 1];
    bzero(tmpsql_buf, tmpsql_buf_len + 1);

    static char *sql_begin_trans = "BEGIN TRANSACTION\n";
    if (debug_mode) {
        SF_display(sql_begin_trans);
    }
    tmpres = PQexec(pgstata_conn, sql_begin_trans);
    PGRESULT_CHECK(tmpres, PGRES_COMMAND_OK, debug_mode);
    PQclear(tmpres);
    pgstata_in_transaction = 1;

    snprintf(tmpsql_buf, tmpsql_buf_len,
             "DECLARE pgstata_cursor CURSOR FOR %s\n",
             sql_query);
    if (debug_mode) {
        SF_display(tmpsql_buf);
    }
    tmpres = PQexec(pgstata_conn, tmpsql_buf);
    PGRESULT_CHECK(tmpres, PGRES_COMMAND_OK, debug_mode);
    bzero(tmpsql_buf, tmpsql_buf_len);
    PQclear(tmpres);

    // Step the cursor forward so that we have type information available,
    // and so that populate_workspace() has something to chew on.
    snprintf(tmpsql_buf, tmpsql_buf_len,
             "FETCH FORWARD %d FROM pgstata_cursor\n",
             PGSTATA_CURSOR_SLURP_ROWS);
    if (debug_mode) {
        SF_display(tmpsql_buf);
    }
    pgstata_res = PQexec(pgstata_conn, tmpsql_buf);
    PGRESULT_CHECK(pgstata_res, PGRES_TUPLES_OK, debug_mode);
    pgstata_num_obs_loaded = 0;
    pgstata_num_obs = PQntuples(pgstata_res);

    // Workspace size
    int num_vars = PQnfields(pgstata_res);

    // Column type information
    column_widths = malloc(sizeof(int) * num_vars);
    column_oids = malloc(sizeof(Oid) * num_vars);
    column_mods = malloc(sizeof(int) * num_vars);
    char *stata_mac_vars  = malloc(num_vars * 33 * sizeof(char));
    char *stata_mac_types = malloc(num_vars * 12);
    char *stata_mac_fmts  = malloc(num_vars * 12);
    *stata_mac_vars = '\000';
    *stata_mac_types = '\000';
    *stata_mac_fmts = '\000';

    char typetmp[256];
    bzero(typetmp, 256);
    int i;
    for (i=0; i<num_vars; ++i) {
        char *fname = PQfname(pgstata_res, i);
        Oid ftype = PQftype(pgstata_res, i);   // postgres's internal type
        int fsize = PQfsize(pgstata_res, i);
        int fmod = PQfmod(pgstata_res, i);
        
        strcat(stata_mac_vars, fname);
        strcat(stata_mac_vars, " ");

        char statatype_tmp[33];
        char statafmt_tmp[13];
        bzero(statafmt_tmp, 13);
        switch (ftype) {
            
            // pg bool -> stata byte
            case BOOLOID:
                strcat(stata_mac_types, "byte ");
                break;

            // pg smallint -> stata long
            // sadly a stata int is just a little too narrow
            case INT2OID:
                strcat(stata_mac_types, "long ");
                break;

            // other pg numeric types -> stata double
            case INT4OID:
            case INT8OID:
            case FLOAT4OID:
            case FLOAT8OID:
            case NUMERICOID:
                strcat(stata_mac_types, "double ");
                break;

            // fixed-width character strings <= 244 chars in length
            case BPCHAROID:
            case VARCHAROID:
                *statatype_tmp = '\000';
                if (fmod-VARHDRSZ < 245 && fmod-VARHDRSZ > 0) {
                    snprintf(statatype_tmp, 32, "str%d ", fmod-VARHDRSZ);
                    strcat(stata_mac_types, statatype_tmp);
                    break;
                    // typemod for a char(N) or a varchar(N) is VARHDRSZ+N
                    // assume character == ASCII for now
                }
                // FALLTHRU - treat it as TEXT and truncate...
                ftype = TEXTOID;

            // variable-width character strings
            case TEXTOID:
                strcat(stata_mac_types, "str244 ");
                break;
                // Could issue more SQL commands to find the maximum length
                // here. It's likely a view is being used, however.

            // Dates and times
            case DATEOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
                strcat(stata_mac_types, "long ");
                strcat(statafmt_tmp, "%d");
                break;

            /*
             * Unknown types. The explicitly-named ones might get some proper
             * conversion code at some point.
             */

            case CASHOID: // money
            case INTERVALOID: // interval
            case TIMEOID: // time without time zone
            case TIMETZOID: //time with time zone
            default:
                *typetmp = '\000';
                pgstata_typoid2name(ftype, 255, typetmp, debug_mode);
                snprintf(msgtmp, 255,
                         "Type \"%s\" (column %s) is only partially "
                         "supported: treating it as str244\n",
                         typetmp, fname);
                SF_error(msgtmp);
                *msgtmp = '\000';
                strcat(stata_mac_types, "str244 ");
                break;
        }

        // Remember what we discovered for per-row tests later on
        column_widths[i] = fsize;
        column_oids[i] = ftype;
        column_mods[i] = fmod;

        if (*statafmt_tmp != '\000') {
            strcat(stata_mac_fmts, statafmt_tmp);
        }
        else {
            strcat(stata_mac_fmts, "default");
        }
        strcat(stata_mac_fmts, " ");
        if (debug_mode) {
            char message[256];
            snprintf(message, 255,
                     "DEBUG: name=%s size=%d oid=%d mod=%d\n",
                     fname, fsize, ftype, fmod);
            SF_display(message);
        }
    }

    // save type info
    char tmpbuf[256];
    bzero(tmpbuf, 256);
    sprintf(tmpbuf, "%i", pgstata_num_obs);
    if (debug_mode) {
        SF_display("DEBUG: _vars: ");
        SF_display(stata_mac_vars);
        SF_display("\nDEBUG: _types: ");
        SF_display(stata_mac_types);
        SF_display("\nDEBUG: _fmts: ");
        SF_display(stata_mac_fmts);
        SF_display("\nDEBUG: _obs: ");
        SF_display(tmpbuf);
        SF_display("\n");
    }
    SF_macro_save("_obs", tmpbuf);
    SF_macro_save("_vars", stata_mac_vars);
    SF_macro_save("_types", stata_mac_types);
    SF_macro_save("_fmts", stata_mac_fmts);

    free(stata_mac_vars);
    free(stata_mac_types);
    free(stata_mac_fmts);

    return pgstata_ok;
}

// }}}
// Query execution, and population of Stata workspace {{{

pgstata_rc
pgstata_populate_next (int argc, char **argv) {
    USAGE_CHECK(argc, 0, 1, "populate_next [debug]");

    // Debugging mode
    int debug_mode = 0;
    if (argc >= 1) {
        if (strncasecmp(argv[0], "debug", 5) == 0) {
            debug_mode = 1;
        }
    }

    PGCONN_CHECK(debug_mode);

    ST_retcode rc = 0;
    if (! pgstata_res) {
        SF_error("Must call \"prepare\" before calling \"populate_next\"\n");
        pgstata_cleanup(debug_mode);
        return pgstata_usage_error;
    }

    const int  nfields = PQnfields(pgstata_res); // number of columns
    char      *valuetmp = NULL;  // ptr to string value, owned by libpq
    char       msg[256];    // message buffer for errors
    bzero(msg, 256);
    // Individual values
    struct tm  tvalue;
    char       svalue[245];
    bzero(svalue, 245);
    bzero(&tvalue, sizeof(struct tm));

    int ntups = PQntuples(pgstata_res);  // #rows in this slurp
    if (ntups == 0) {
        return pgstata_finished;
    }

    int i, j;
    for (i=0; i<ntups; ++i) {
        int stata_obs = 1 + i + pgstata_num_obs_loaded;
        for (j=0; j<nfields; ++j) {
            if (PQgetisnull(pgstata_res, i, j)) {
                continue;
            }
            valuetmp = PQgetvalue(pgstata_res, i, j);
            int stata_var = 1 + j;
            switch (column_oids[j]) {
                case INT4OID:
                case INT2OID: // maybe atoi?
                case INT8OID: // maybe use atoll for this?
                    rc = SF_vstore(stata_var, stata_obs, atol(valuetmp));
                    break;

                case FLOAT4OID:
                case FLOAT8OID:
                case NUMERICOID:
                    rc = SF_vstore(stata_var, stata_obs, 
                                   strtold(valuetmp, NULL));
                    break;

                case BPCHAROID:
                case VARCHAROID:
                    // Assume these are always <= 244 in length.
                    // See earlier setup.
                    rc = SF_sstore(stata_var, stata_obs, valuetmp);
                    break;

                case DATEOID:
                case TIMESTAMPOID:
                case TIMESTAMPTZOID:
                    if (strptime(valuetmp, PGSTATA_PG_DATE_FORMAT,
                                 &tvalue) == NULL) {
                        snprintf(msg, 255,
                                 "failed to parse date at (%d, %d)",
                                 stata_obs, stata_var);
                        SF_error(msg);
                        goto CLEANUP;
                    }
                    rc = SF_vstore(stata_var, stata_obs, 
                                   pgstata_tm2statadate(&tvalue));
                    bzero(&tvalue, sizeof(struct tm));
                    break;

                case BOOLOID:
                    rc = SF_vstore(stata_var, stata_obs,
                                   strncasecmp(valuetmp, "t", 1) == 0);
                    break;

                /*
                 * The following types will be imported as strings and
                 * truncated to 244 characters if their string representation
                 * is longer than that.
                 */
                case TEXTOID:
                case CASHOID: // money
                case INTERVALOID: // interval
                case TIMEOID: // time without time zone
                case TIMETZOID: //time with time zone
                default:
                    strncpy(svalue, valuetmp, 244);
                    rc = SF_sstore(stata_var, stata_obs, svalue);
                    bzero(svalue, 245);
                    break;
            }
            if (rc) {
                snprintf(msg, 255,
                         "failed to store oid:%d at (%d,%d)\n",
                         column_oids[j], stata_obs, stata_var);
                SF_error(msg);
                goto CLEANUP;
            }
        }
    }
    pgstata_num_obs_loaded += i;
    PQclear(pgstata_res);

    /* Advance the cursor */
    char advance_sql[256];
    *advance_sql = '\000';
    snprintf(advance_sql, 255, "FETCH FORWARD %d FROM pgstata_cursor\n",
             PGSTATA_CURSOR_SLURP_ROWS);
    if (debug_mode) {
        SF_display(advance_sql);
    }
    pgstata_res = PQexec(pgstata_conn, advance_sql);
    if (PQresultStatus(pgstata_res) != PGRES_TUPLES_OK) {
        SF_error("error: ");
        SF_error(PQresultErrorMessage(pgstata_res));
        rc = pgstata_db_error;
        goto CLEANUP;
    }
    int pending = PQntuples(pgstata_res);
    pgstata_num_obs += pending;

    *msg = '\000';
    snprintf(msg, 32, "%i", pgstata_num_obs);
    if (debug_mode) {
        SF_display("DEBUG: _obs: ");
        SF_display(msg);
        SF_display("\n");
    }
    SF_macro_save("_obs", msg);

  CLEANUP:
    if (rc) {
        SF_error("*error* cleaning up\n");
        pgstata_cleanup(debug_mode);
    }

    if (!rc) {
        rc = pending ? pgstata_ok : pgstata_finished;
        if (debug_mode) {
            if (rc == pgstata_ok) {
                SF_display("DEBUG: more data.\n");
            }
            else if (rc == pgstata_finished) {
                SF_display("DEBUG: no more data. Show's over. Go home.\n");
            }
        }
    }
    return rc;
}

// }}}
// Entry point {{{

STDLL
stata_call (int argc, char **argv) {
    USAGE_CHECK(argc, 1, -1, "pg COMMAND [OPTS...]");

    if (strcmp(argv[0], "connect") == 0) {
        return pgstata_connect(argc-1, argv+1);
    }
    else if (strcmp(argv[0], "disconnect") == 0) {
        return pgstata_disconnect(argc-1, argv+1);
    }
    else if (strcmp(argv[0], "prepare") == 0) {
        return pgstata_prepare(argc-1, argv+1);
    }
    else if (strcmp(argv[0], "populate_next") == 0) {
        return pgstata_populate_next(argc-1, argv+1);
    }

    SF_error("unrecognised command option\n");
    return pgstata_usage_error;
}


// }}}
