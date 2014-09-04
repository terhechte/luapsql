/* {=================================================================
 *
 * psql.c
 * Main file for luapsql (Lua interface to PostgreSQL's libpq)
 * Luis Carvalho (lexcarvalho at gmail dot com)
 * See Copyright Notice at the bottom of this file
 * $Id: $
 *
 * ==================================================================} */

#include <lua.h>
#include <lauxlib.h>

#include <stdlib.h> /* atoi */
#include <string.h> /* memcpy */
#include "lpqtype.h"
#include <libpq-fe.h>
#include <libpqtypes.h>

#define PSQL_NAME       "psql"
#define LPQ_CONN_NAME   "connection"
#define LPQ_PLAN_NAME   "plan"
#define LPQ_RSET_NAME   "result set"
#define LPQ_TUPLE_NAME  "tuple"
#define LPQ_RSET_FIELDS "fields" /* in result set userdata environment */


typedef struct lpq_Conn_struct {
  PGconn *conn;
  int done;
} lpq_Conn;

typedef struct lpq_Plan_struct {
  lpq_Conn *conn;
  const char *name;
  int n; /* #params */
  Oid *type;
  const char **value;
  int *length;
  int *format;
  int valid; /* referenced conn valid? */
} lpq_Plan;

typedef struct lpq_Rset_struct {
  PGresult *result;
} lpq_Rset;

typedef struct lpq_Tuple_struct {
  lpq_Rset *rset;
  int row; /* row reference in rset */
  int valid; /* referenced rset valid? */
} lpq_Tuple;


/* =======   Auxiliar   ======= */

#if LUA_VERSION_NUM <= 501
#define lua_rawlen lua_objlen
#define lpq_registerlib(L,l,n) luaL_openlib(L,NULL,l,n)
#define luaL_newlibtable(L,l) \
  lua_createtable(L, 0, sizeof(l)/sizeof((l)[0]) - 1)
#define lua_setuservalue lua_setfenv
#define lua_getuservalue lua_getfenv
#else
#define lpq_registerlib luaL_setfuncs
#endif

static int lpq_typeerror (lua_State *L, int narg, const char *tname) {
  const char *msg = lua_pushfstring(L, "%s expected, got %s", tname,
      luaL_typename(L, narg));
  return luaL_argerror(L, narg, msg);
}

/* from include/catalog/pg_type.h */
#define BOOLOID    16
#define BYTEAOID   17
#define CHAROID    18
#define NAMEOID    19
#define INT8OID    20
#define INT4OID    23
#define TEXTOID    25 /* ignore encoding for now */
#define OIDOID     26
#define FLOAT4OID  700
#define FLOAT8OID  701
#define BPCHAROID  1042
#define VARCHAROID 1043
#define REGCLASSOID 2205
#define TIMESTAMPOID 1114
// oops these are the same
#define INTERVALOID 1184
#define TIMESTAMPTZOID 1184
#define JSONOID 114
// array oid types
#define VARCHARARRAYOID 1015
#define INTEGERARRAYOID 1007
#define BIGINTEGERARRAYOID 1016
#define TIMESTAMPARRAYOID 1115
#define TIMESTAMPTZARRAYOID 1185
#define FLOAT4ARRAYOID 1021
#define FLOAT8ARRAYOID 1022


static int lpq_type_mt_ = 0;
#define LPQ_TYPE_MT ((void *) &lpq_type_mt_)

static int lpq_gettypemt (lua_State *L, Oid type) {
  int found;
  lua_pushlightuserdata(L, LPQ_TYPE_MT);
  lua_rawget(L, LUA_REGISTRYINDEX);
  lua_rawgeti(L, -1, (int) type);
  found = !lua_isnil(L, -1);
  if (found) lua_replace(L, -2);
  else lua_pop(L, 2);
  return found;
}

static void lpq_pushvalue (lua_State *L, Oid type, int mod, const char *value,
                           int length, PGresult *result, int field_number, int rowindex) {

  // FIXME: Some of the blocks below are all so similar, that they can be abstracted further
  switch (type) {
    case BOOLOID:
      lua_pushboolean(L, *value);
      break;
    case CHAROID:
      lua_pushlstring(L, value, 1);
      break;
    case INT4OID:
    case REGCLASSOID:
    case OIDOID:
      lua_pushinteger(L, (int) lpq_getuint32(value));
      break;
    case INT8OID:
      lua_pushinteger(L, (int64) lpq_getint64(value));
      break;
    case FLOAT4OID:
      lua_pushnumber(L, (lua_Number) lpq_getfloat4(value));
      break;
    case FLOAT8OID:
      lua_pushnumber(L, (lua_Number) lpq_getfloat8(value));
      break;
    case TIMESTAMPOID:
    case TIMESTAMPTZOID: {
      // I'm re-reading the value as casting the *value did not work correctly
      char *typeflag = "%timestamptz";
      if (type == TIMESTAMPOID)typeflag = "%timestamp";
      PGtimestamp tvalue;
      PQgetf(result, rowindex, typeflag, field_number, &tvalue);
      lua_pushnumber(L, tvalue.epoch);
      break;
    }
    case BYTEAOID:
    case TEXTOID:
    case VARCHAROID:
    case JSONOID:
    case NAMEOID:
      lua_pushlstring(L, value, length);
      break;
  case INTEGERARRAYOID: {
      PGarray arr;
      int i = 0;
      PQgetf(result, rowindex, "%int4[]", field_number, &arr);
      int ntups = PQntuples(arr.res);
      lua_newtable(L);
      for(i=0; i < ntups; i++) {
        // We get the value for each row in the dictionary
        PGint4 val;
        PQgetf(arr.res, i, "%int4", 0, &val);
        lua_pushnumber(L, i + 1); //index
        lua_pushnumber(L, val); //value
        lua_settable(L, -3);
      }
    break;
  }
  case BIGINTEGERARRAYOID: {
      PGarray arr;
      int i = 0;
      PQgetf(result, rowindex, "%int8[]", field_number, &arr);
      int ntups = PQntuples(arr.res);
      lua_newtable(L);
      for(i=0; i < ntups; i++) {
        // We get the value for each row in the dictionary
        PGint8 val;
        PQgetf(arr.res, i, "%int8", 0, &val);
        lua_pushnumber(L, i + 1); //index
        lua_pushnumber(L, val); //value
        lua_settable(L, -3);
      }
    break;
  }
  case VARCHARARRAYOID: {
      PGarray arr;
      int i = 0;
      PQgetf(result, rowindex, "%varchar[]", field_number, &arr);
      int ntups = PQntuples(arr.res);
      lua_newtable(L);
      for(i=0; i < ntups; i++) {
        // We get the value for each row in the dictionary
        PGvarchar val;
        PQgetf(arr.res, i, "%varchar", 0, &val);
        lua_pushnumber(L, i + 1); //index
        // I'm not sure if strlen is correct usage here, but I couldn't
        // figure out if there is an official function for determining
        // PGvarchar's length. Besides, strlen seems to work fine.
        lua_pushlstring(L, val, strlen(val));
        lua_settable(L, -3);
      }
      break;
    }
  case TIMESTAMPARRAYOID: {
    PGarray arr;
    int i = 0;
    PQgetf(result, rowindex, "%timestamp[]", field_number, &arr);
    int ntups = PQntuples(arr.res);
    lua_newtable(L);
    for(i=0; i < ntups; i++) {
      // We get the val for each row in the dictionary
      PGtimestamp val;
      PQgetf(arr.res, i, "%timestamp", 0, &val);
      lua_pushnumber(L, i + 1); //index
      // we're pushing the time converted to time stamp, lua doesn't support much more anyway
      lua_pushnumber(L, val.epoch);
      lua_settable(L, -3);
    }
    break;
  }
  case TIMESTAMPTZARRAYOID: {
    PGarray arr;
    int i = 0;
    PQgetf(result, rowindex, "%timestamptz[]", field_number, &arr);
    int ntups = PQntuples(arr.res);
    lua_newtable(L);
    for(i=0; i < ntups; i++) {
      // We get the value for each row in the dictionary
      PGtimestamp val;
      PQgetf(arr.res, i, "%timestamptz", 0, &val);
      lua_pushnumber(L, i + 1); //index
      // we're pushing the time converted to time stamp, lua doesn't support much more anyway
      lua_pushnumber(L, val.epoch);
      lua_settable(L, -3);
    }
    break;
  }
  case FLOAT8ARRAYOID:
  case FLOAT4ARRAYOID: {
    int byteSize = 4;
    if (type == FLOAT8ARRAYOID) {
      byteSize = 8;
    }
    char template1[8];
    sprintf(template1, "%%float%i", byteSize);
    char template2[10];
    sprintf(template2, "%s[]", template1);

    PGarray arr;
    int i = 0;
    PQgetf(result, rowindex, template2, field_number, &arr);
    int ntups = PQntuples(arr.res);
    lua_newtable(L);
    for(i=0; i < ntups; i++) {
      // We get the val for each row in the dictionary
      lua_pushnumber(L, i + 1); //index
      // we're pushing the time converted to time stamp, lua doesn't support much more anyway
      if (byteSize == 4) {
        PGfloat4 val;
        PQgetf(arr.res, i, template1, 0, &val);
        lua_pushnumber(L, (lua_Number)val);
      }
      if (byteSize == 8) {
        PGfloat8 val;
        PQgetf(arr.res, i, template1, 0, &val);
        lua_pushnumber(L, (lua_Number)val);
      }
      lua_settable(L, -3);
    }
    break;
  }
  case BPCHAROID: {
      int l = mod - VARHDRSZ;
      if (l < 0) l = length;
      lua_pushlstring(L, value, l);
      if (length > l) { /* pad? */
        luaL_Buffer b;
        length -= l;
        luaL_buffinit(L, &b);
        while (l-- > 0) luaL_addchar(&b, ' ');
        luaL_pushresult(&b);
        lua_concat(L, 2);
      }
      break;
    }
    default:
      if (lpq_gettypemt(L, type)) { /* registered type? */
        lua_getfield(L, -1, LPQ_REGMT_RECV);
        if (lua_type(L, -1) == LUA_TFUNCTION) {
          int consistent = 0;
          lua_pushlstring(L, value, length);
          lua_pushinteger(L, mod);
          lua_call(L, 2, 1);
          /* check returned value */
          if (lua_getmetatable(L, -1)) {
            if (lua_rawequal(L, -1, -3)) consistent = 1;
            lua_pop(L, 1); /* MT */
          }
          if (consistent) {
            lua_replace(L, -2);
            return;
          }
        }
        lua_pop(L, 2);
      }
      memcpy((char *) lua_newuserdata(L, length), value, length);
  }
}

static int lpq_tovalue (lua_State *L, int narg, Oid type, luaL_Buffer *b) {
  switch (type) {
    case INTERVALOID:
      lua_getfield(L, narg, "time");
      /* XXX: if timestamps are int64... or float8... */
      lpq_sendfloat8(b, (float8) lua_tonumber(L, -1));
      lua_getfield(L, narg, "day");
      lpq_senduint32(b, (uint32) lua_tonumber(L, -1));
      lua_getfield(L, narg, "month");
      lpq_senduint32(b, (uint32) lua_tonumber(L, -1));
      return 0x10;
    case BOOLOID:
      luaL_addchar(b, (char) lua_toboolean(L, narg));
      return sizeof(char);
    case CHAROID:
      luaL_addchar(b, *lua_tostring(L, narg));
      return sizeof(char);
    case INT4OID:
    case OIDOID:
      lpq_senduint32(b, (uint32) lua_tointeger(L, narg));
      return sizeof(uint32);
    case FLOAT4OID:
      lpq_sendfloat4(b, (float4) lua_tonumber(L, narg));
      return sizeof(float4);
    case FLOAT8OID:
      lpq_sendfloat8(b, (float8) lua_tonumber(L, narg));
      return sizeof(float8);
    case BYTEAOID:
    case TEXTOID:
    case BPCHAROID:
    case NAMEOID:
    case VARCHAROID: {
      size_t l;
      const char *s = lua_tolstring(L, narg, &l);
      luaL_addlstring(b, s, l);
      return l;
    }
    default: {
      size_t l;
      const char *s;
      if (lpq_gettypemt(L, type)) { /* registered type? */
        lua_getfield(L, -1, LPQ_REGMT_SEND);
        if (lua_type(L, -1) == LUA_TFUNCTION) {
          int consistent = 0;
          /* check input */
          if (lua_getmetatable(L, narg)) {
            if (lua_rawequal(L, -1, -3)) consistent = 1;
            lua_pop(L, 1); /* MT */
          }
          if (consistent) {
            lua_pushvalue(L, narg);
            lua_call(L, 1, 1);
            s = lua_tolstring(L, -1, &l);
            if (s != NULL) luaL_addlstring(b, s, l);
            else l = 0;
            lua_pop(L, 2);
            return l;
          }
        }
        lua_pop(L, 2);
      }
      s = (const char *) lua_touserdata(L, narg);
      if (s != NULL) {
        l = lua_rawlen(L, narg);
        luaL_addlstring(b, s, l);
      }
      else l = 0;
      return l;
    }
  }
}

static int lpq_pushstatus (lua_State *L, int status, PGconn *conn) {
  lua_pushboolean(L, status);
  if (status == 0) {
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
  }
  return 1;
}

/* =======   PSQL   ======= */

static int lpq_pushconnection (lua_State *L, PGconn *conn) {
  lpq_Conn *C;
  if (conn == NULL) luaL_error(L, "libpq unable to alloc connection");
  C = (lpq_Conn *) lua_newuserdata(L, sizeof(lpq_Conn));
  // Have to call this on any connection to be used with libpqtypes: http://libpqtypes.esilo.com/
  PQinitTypes(conn);
  C->conn = conn;
  C->done = 0;
  lua_newtable(L);
  lua_setuservalue(L, -2);
  lua_pushvalue(L, lua_upvalueindex(1)); /* MT */
  lua_setmetatable(L, -2);
  return 1;
}

static int lpq_connect (lua_State *L) {
  const char *conninfo = luaL_checkstring(L, 1);
  return lpq_pushconnection(L, PQconnectdb(conninfo));
}

static int lpq_start (lua_State *L) {
  const char *conninfo = luaL_checkstring(L, 1);
  return lpq_pushconnection(L, PQconnectStart(conninfo));
}

/* register(oid [, metatable]) */
static int lpq_register (lua_State *L) {
  Oid type = (Oid) luaL_checkinteger(L, 1);
  lua_settop(L, 2);
  lua_pushlightuserdata(L, LPQ_TYPE_MT);
  lua_rawget(L, LUA_REGISTRYINDEX);
  if (lua_type(L, 2) != LUA_TTABLE) 
    lua_createtable(L, 0, 1);
  else
    lua_pushvalue(L, 2);
  lua_pushinteger(L, (int) type);
  lua_setfield(L, -2, LPQ_REGMT_OID);
  lua_rawseti(L, -2, (int) type);
  return 0;
}


/* =======   lpq_Conn   ======= */

static lpq_Conn *lpq_checkconn (lua_State *L, int narg) {
  lpq_Conn *C = NULL;
  if (lua_getmetatable(L, narg)) { /* has metatable? */
    if (lua_rawequal(L, -1, lua_upvalueindex(1))) /* MT == upvalue? */
      C = (lpq_Conn *) lua_touserdata(L, narg);
    lua_pop(L, 1); /* MT */
  }
  if (C == NULL) lpq_typeerror(L, narg, LPQ_CONN_NAME);
  if (C->done) luaL_error(L, LPQ_CONN_NAME " is finished");
  return C;
}

static void lpq_finishconn (lua_State *L, lpq_Conn *C) {
  if (!C->done) {
    PQfinish(C->conn);
    /* mark plans that reference conn as invalid */
    lua_getuservalue(L, 1);
    lua_pushnil(L);
    while (lua_next(L, 2)) {
      lpq_Plan *P = (lpq_Plan *) lua_touserdata(L, -1);
      if (P != NULL) P->valid = 0;
      lua_pop(L, 1);
    }
    C->done = 1;
  }
}

static int lpq_conn__tostring (lua_State *L) {
  lua_pushfstring(L, LPQ_CONN_NAME ": %p", (void *) lua_touserdata(L, 1));
  return 1;
}

static int lpq_conn__gc (lua_State *L) {
  lpq_finishconn(L, (lpq_Conn *) lua_touserdata(L, 1));
  return 0;
}


static int lpq_conn_notifies (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  PGnotify *p = PQnotifies(C->conn);
  if (p == NULL) { /* queue is empty? */
    lua_pushnil(L);
    return 1;
  }
  lua_pushstring(L, p->relname);
  lua_pushinteger(L, p->be_pid);
  lua_pushstring(L, p->extra);
  PQfreemem(p);
  return 3;
}

static int lpq_conn_poll (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  PostgresPollingStatusType status = PQconnectPoll(C->conn);
  lua_pushboolean(L, status == PGRES_POLLING_OK);
  switch (status) {
    case PGRES_POLLING_OK:
      lua_pushstring(L, "PGRES_POLLING_OK"); break;
    case PGRES_POLLING_READING:
      lua_pushstring(L, "PGRES_POLLING_READING"); break;
    case PGRES_POLLING_WRITING:
      lua_pushstring(L, "PGRES_POLLING_WRITING"); break;
    default:
      lua_pushstring(L, "PGRES_POLLING_FAILED");
  }
  return 2;
}

static int lpq_conn_status (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  ConnStatusType status = PQstatus(C->conn);
  lua_pushboolean(L, status == CONNECTION_OK);
  switch (status) {
    case CONNECTION_OK:
      lua_pushstring(L, "CONNECTION_OK"); break;
    case CONNECTION_STARTED:
      lua_pushstring(L, "CONNECTION_STARTED"); break;
    case CONNECTION_MADE:
      lua_pushstring(L, "CONNECTION_MADE"); break;
    case CONNECTION_AWAITING_RESPONSE:
      lua_pushstring(L, "CONNECTION_AWAITING_RESPONSE"); break;
    case CONNECTION_AUTH_OK:
      lua_pushstring(L, "CONNECTION_AUTH_OK"); break;
    case CONNECTION_SSL_STARTUP:
      lua_pushstring(L, "CONNECTION_SSL_STARTUP"); break;
    case CONNECTION_SETENV:
      lua_pushstring(L, "CONNECTION_SETENV"); break;
    default:
      lua_pushstring(L, "CONNECTION_BAD");
  }
  return 2;
}

static int lpq_conn_finish (lua_State *L) {
  lpq_finishconn(L, lpq_checkconn(L, 1));
  return 0;
}

static int lpq_conn_reset (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  PQreset(C->conn);
  return 0;
}

static int lpq_conn_resetstart (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  lua_pushboolean(L, PQresetStart(C->conn));
  return 1;
}

static int lpq_conn_resetpoll (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  PostgresPollingStatusType status = PQresetPoll(C->conn);
  lua_pushboolean(L, status == PGRES_POLLING_OK);
  switch (status) {
    case PGRES_POLLING_OK: lua_pushstring(L, "OK"); break;
    case PGRES_POLLING_READING: lua_pushstring(L, "READING"); break;
    case PGRES_POLLING_WRITING: lua_pushstring(L, "WRITING"); break;
    default: lua_pushstring(L, "FAILED");
  }
  return 2;
}

static int lpq_conn_socket (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  lua_pushinteger(L, PQsocket(C->conn));
  return 1;
}

static int lpq_conn_error (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  lua_pushstring(L, PQerrorMessage(C->conn));
  return 1;
}

#define lpq_conn_string(name) \
  static int lpq_conn_ ## name (lua_State *L) { \
    lpq_Conn *C = lpq_checkconn(L, 1); \
    lua_pushstring(L, PQ ## name (C->conn)); \
    return 1; \
  }

lpq_conn_string(db)
lpq_conn_string(user)
lpq_conn_string(pass)
lpq_conn_string(host)
lpq_conn_string(port)
lpq_conn_string(tty)
lpq_conn_string(options)

static int lpq_conn_escape (lua_State *L) {
  int error;
  size_t length;
  lpq_Conn *C = lpq_checkconn(L, 1);
  const char *from = luaL_checklstring(L, 2, &length);
  char *to = (char *) lua_newuserdata(L, 2 * length + 1);
  length = PQescapeStringConn(C->conn, to, from, length, &error);
  if (!error) lua_pushlstring(L, to, length);
  else lua_pushnil(L);
  return 1;
}

static int lpq_conn_isbusy (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  lua_pushboolean(L, PQisBusy(C->conn));
  return 1;
}

static int lpq_conn_consume (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  return lpq_pushstatus(L, PQconsumeInput(C->conn), C->conn);
}

static int lpq_conn_query (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  const char *cmd = luaL_checkstring(L, 2);
  return lpq_pushstatus(L, PQsendQueryParams(C->conn, cmd,
      0, NULL, NULL, NULL, NULL, 1), /* binary, no params */
      C->conn);
}

/* related to lpq_Rset */
/* lpq_Rset MT as second upvalue */
static int lpq_pushresult (lua_State *L, PGresult *result) {
  if (result == NULL) lua_pushnil(L);
  else {
    lpq_Rset *R = (lpq_Rset *) lua_newuserdata(L, sizeof(lpq_Rset));
    ExecStatusType status = PQresultStatus(result);
    R->result = result;
    lua_pushvalue(L, lua_upvalueindex(2)); /* lpq_Rset MT */
    lua_setmetatable(L, -2);
    if (status == PGRES_TUPLES_OK) { /* from SELECT? */
      /* store field name table in udata environment */
      int i, n = PQnfields(R->result);
      lua_newtable(L);
      lua_createtable(L, 0, n);
      for (i = 0; i < n; i++) {
        lua_pushstring(L, PQfname(R->result, i));
        lua_pushinteger(L, i);
        lua_rawset(L, -3);
      }
      lua_setfield(L, -2, LPQ_RSET_FIELDS);
      lua_setuservalue(L, -2);
    }
  }
  return 1;
}

static int lpq_conn_getresult (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  return lpq_pushresult(L, PQgetResult(C->conn));
}

static int lpq_conn_exec (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  const char *cmd = luaL_checkstring(L, 2);
  return lpq_pushresult(L, PQexecParams(C->conn, cmd,
      0, NULL, NULL, NULL, NULL, 1)); /* binary, no params */
}

/* related to lpq_Plan */
/* lpq_Plan MT as second upvalue */
static lpq_Plan *lpq_getplan (lua_State *L, lpq_Conn *C, const char *name) {
  lpq_Plan *P = NULL;
  PGresult *result = PQdescribePrepared(C->conn, name);
  ExecStatusType status = PQresultStatus(result);
  if (status == PGRES_COMMAND_OK) {
    int i, n = PQnparams(result);
    P = (lpq_Plan *) lua_newuserdata(L, sizeof(lpq_Plan)
        + n * (sizeof(Oid) + sizeof(char *) + 2 * sizeof(int)));
    P->conn = C;
    /* keep reference in conn env and set P->name */
    lua_getuservalue(L, 1); /* note: conn at stack pos 1 */
    lua_pushlightuserdata(L, P);
    lua_pushstring(L, name);
    P->name = lua_tostring(L, -1);
    lua_rawset(L, -3); /* env(conn)[light(P)] = name */
    lua_pop(L, 1); /* env */
    /* set param pointers */
    P->n = n;
    P->type = (Oid *) (P + 1);
    P->value = (const char **) (P->type + n);
    P->length = (int *) (P->value + n);
    P->format = (int *) (P->length + n);
    for (i = 0; i < n; i++) {
      P->type[i] = PQparamtype(result, i);
      P->format[i] = 1; /* binary */
    }
    P->valid = 1;
    /* set lpq_Plan MT */
    lua_pushvalue(L, lua_upvalueindex(2));
    lua_setmetatable(L, -2);
  }
  else lua_pushnil(L);
  PQclear(result);
  return P;
}

/* plan = conn:prepare(stmt [, name]) */
/* lpq_Plan MT as upvalue */
static int lpq_conn_prepare (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  const char *query = luaL_checkstring(L, 2);
  const char *name = luaL_optstring(L, 3, "");
  lpq_Plan *P = NULL;
  /* types are inferred by the server */
  PGresult *result = PQprepare(C->conn, name, query, 0, NULL);
  ExecStatusType status = PQresultStatus(result);
  PQclear(result);
  if (status == PGRES_COMMAND_OK) P = lpq_getplan(L, C, name);
  else lua_pushnil(L);
  if (P == NULL) {
    lua_pushstring(L, PQerrorMessage(C->conn));
    return 2;
  }
  return 1;
}

/* plan = conn:getplan([name]) */
/* lpq_Plan MT as upvalue */
static int lpq_conn_getplan (lua_State *L) {
  lpq_Conn *C = lpq_checkconn(L, 1);
  const char *name = luaL_optstring(L, 2, "");
  lpq_Plan *P = lpq_getplan(L, C, name);
  if (P == NULL) {
    lua_pushstring(L, PQerrorMessage(C->conn));
    return 2;
  }
  return 1;
}


/* =======   lpq_Plan   ======= */

static lpq_Plan *lpq_checkplan (lua_State *L, int narg) {
  lpq_Plan *P = NULL;
  if (lua_getmetatable(L, narg)) { /* has metatable? */
    if (lua_rawequal(L, -1, lua_upvalueindex(1))) /* MT == upvalue? */
      P = (lpq_Plan *) lua_touserdata(L, narg);
    lua_pop(L, 1); /* MT */
  }
  if (P == NULL) lpq_typeerror(L, narg, LPQ_PLAN_NAME);
  if (!P->valid)
    luaL_error(L, "referenced " LPQ_CONN_NAME " is finished");
  return P;
}


static int lpq_plan__len (lua_State *L) {
  lpq_Plan *P = (lpq_Plan *) lua_touserdata(L, 1);
  lua_pushinteger(L, P->n);
  return 1;
}

static int lpq_plan__tostring (lua_State *L) {
  lua_pushfstring(L, LPQ_PLAN_NAME ": %p", (void *) lua_touserdata(L, 1));
  return 1;
}

static void lpq_setparams (lua_State *L, lpq_Plan *P) {
  int i;
  luaL_Buffer buf;
  lua_settop(L, P->n + 1);
  luaL_buffinit(L, &buf);
  for (i = 0; i < P->n; i++)
    P->length[i] = lpq_tovalue(L, i + 2, P->type[i], &buf);
  luaL_pushresult(&buf);
  P->value[0] = lua_tostring(L, -1);
  for (i = 0; i < P->n - 1; i++)
    P->value[i + 1] = P->value[i] + P->length[i];
}

static int lpq_plan_query (lua_State *L) {
  lpq_Plan *P = lpq_checkplan(L, 1);
  lpq_setparams(L, P);
  return lpq_pushstatus(L, PQsendQueryPrepared(P->conn->conn, P->name, P->n,
      P->value, P->length, P->format, 1), /* binary */
      P->conn->conn);
}

static int lpq_plan_exec (lua_State *L) {
  lpq_Plan *P = lpq_checkplan(L, 1);
  lpq_setparams(L, P);
  lpq_pushresult(L, PQexecPrepared(P->conn->conn, P->name, P->n,
      P->value, P->length, P->format, 1)); /* binary */
  return 1;
}


/* =======   lpq_Rset   ======= */

static lpq_Rset *lpq_checkrset (lua_State *L, int narg) {
  lpq_Rset *R = NULL;
  if (lua_getmetatable(L, narg)) { /* has metatable? */
    if (lua_rawequal(L, -1, lua_upvalueindex(1))) /* MT == upvalue? */
      R = (lpq_Rset *) lua_touserdata(L, narg);
    lua_pop(L, 1); /* MT */
  }
  if (R == NULL) lpq_typeerror(L, narg, LPQ_RSET_NAME);
  return R;
}

static int lpq_rset__tostring (lua_State *L) {
  lua_pushfstring(L, LPQ_RSET_NAME ": %p", (void *) lua_touserdata(L, 1));
  return 1;
}

static int lpq_rset__gc (lua_State *L) {
  lpq_Rset *R = (lpq_Rset *) lua_touserdata(L, 1);
  ExecStatusType status = PQresultStatus(R->result);
  if (status == PGRES_TUPLES_OK) {
    /* mark tuples that reference rset as invalid */
    lua_getuservalue(L, 1);
    lua_pushnil(L);
    while (lua_next(L, 2)) {
      if (lua_tointeger(L, -2) > 0) { /* tuple? */
        lpq_Tuple *T = (lpq_Tuple *) lua_touserdata(L, -1);
        T->valid = 0;
      }
      lua_pop(L, 1);
    }
  }
  PQclear(R->result);
  return 0;
}

static int lpq_rset__len (lua_State *L) {
  lpq_Rset *R = (lpq_Rset *) lua_touserdata(L, 1);
  ExecStatusType status = PQresultStatus(R->result);
  if (status == PGRES_TUPLES_OK)
    lua_pushinteger(L, PQntuples(R->result));
  else if (status == PGRES_COMMAND_OK)
    lua_pushinteger(L, atoi(PQcmdTuples(R->result)));
  else lua_pushnil(L);
  return 1;
}

/* lpq_Tuple MT as second upvalue */
static int lpq_rset__index (lua_State *L) {
  lpq_Rset *R = (lpq_Rset *) lua_touserdata(L, 1);
  if (lua_isnumber(L, 2)) {
    int n = lua_tointeger(L, 2);
    if (PQresultStatus(R->result) != PGRES_TUPLES_OK
        || n < 1 || n > PQntuples(R->result))
      lua_pushnil(L);
    else {
      lua_getuservalue(L, 1);
      lua_rawgeti(L, -1, n);
      if (lua_isnil(L, -1)) {
        lpq_Tuple *T = (lpq_Tuple *) lua_newuserdata(L, sizeof(lpq_Tuple));
        T->rset = R;
        T->row = n - 1; /* zero-based */
        T->valid = 1;
        lua_pushvalue(L, lua_upvalueindex(2)); /* lpq_Tuple MT */
        lua_setmetatable(L, -2);
        lua_getfield(L, 3, LPQ_RSET_FIELDS); /* rset_env.fields */
        lua_setuservalue(L, -2); /* tuple env is rset env fields */
        lua_pushvalue(L, -1); /* tuple */
        lua_rawseti(L, 3, n); /* rset_env[n] = tuple */
      }
    }
  }
  else lua_rawget(L, lua_upvalueindex(1)); /* lpq_Rset class */
  return 1;
}

static int lpq_rset_fields (lua_State *L) {
  lpq_Rset *R = lpq_checkrset(L, 1);
  if (PQresultStatus(R->result) != PGRES_TUPLES_OK)
    lua_pushnil(L);
  else {
    lua_getuservalue(L, 1);
    lua_getfield(L, -1, LPQ_RSET_FIELDS);
  }
  return 1;
}

static int lpq_rset_status (lua_State *L) {
  lpq_Rset *R = lpq_checkrset(L, 1);
  lua_pushstring(L, PQresStatus(PQresultStatus(R->result)));
  return 1;
}

static int lpq_rset_error (lua_State *L) {
  lpq_Rset *R = lpq_checkrset(L, 1);
  lua_pushstring(L, PQresultErrorMessage(R->result));
  return 1;
}

static int lpq_rset_cmdstatus (lua_State *L) {
  lpq_Rset *R = lpq_checkrset(L, 1);
  if (PQresultStatus(R->result) == PGRES_COMMAND_OK)
    lua_pushstring(L, PQcmdStatus(R->result));
  else lua_pushnil(L);
  return 1;
}

static int lpq_rset_fetchaux (lua_State *L) {
  lpq_Rset *R = (lpq_Rset *) lua_touserdata(L, lua_upvalueindex(1));
  int rowindex = lua_toboolean(L, lua_upvalueindex(2));
  int i = lua_tointeger(L, lua_upvalueindex(3)); /* current row */
  if (i < PQntuples(R->result)) {
    int f, n = PQnfields(R->result);
    if (rowindex) lua_pushinteger(L, i + 1);
    for (f = 0; f < n; f++) {
      if (PQgetisnull(R->result, i, f)) lua_pushnil(L);
      else lpq_pushvalue(L, PQftype(R->result, f), PQfmod(R->result, f),
                         PQgetvalue(R->result, i, f), PQgetlength(R->result, i, f), R->result, f, rowindex);
    }
    if (rowindex) n++;
    lua_pushinteger(L, i + 1);
    lua_replace(L, lua_upvalueindex(3));
    return n;
  }
  return 0;
}

/* rset:fetch(rowindex?) */
static int lpq_rset_fetch (lua_State *L) {
  lpq_checkrset(L, 1);
  lua_settop(L, 2);
  lua_pushinteger(L, 0); /* current row */
  lua_pushcclosure(L, lpq_rset_fetchaux, 3);
  return 1;
}


/* auxiliar closure (on tuple) to lpq_rset_rows */
static int lpq_rset_rowsaux (lua_State *L) {
  lpq_Rset *R = (lpq_Rset *) lua_touserdata(L, 1);
  int n = lua_tointeger(L, 2);
  lpq_Tuple *T = (lpq_Tuple *) lua_touserdata(L, lua_upvalueindex(1));
  if (PQresultStatus(R->result) != PGRES_TUPLES_OK
      || n == PQntuples(R->result)) {
    T->valid = 0;
    lua_pushnil(L);
    return 1;
  }
  T->row = n;
  lua_pushinteger(L, n + 1);
  lua_pushvalue(L, lua_upvalueindex(1));
  return 2;
}

/* lpq_Tuple MT as upvalue */
static int lpq_rset_rows (lua_State *L) {
  lpq_Rset *R = lpq_checkrset(L, 1);
  lpq_Tuple *T;
  lua_getuservalue(L, 1);
  T = (lpq_Tuple *) lua_newuserdata(L, sizeof(lpq_Tuple));
  T->rset = R;
  T->valid = 1;
  lua_pushvalue(L, lua_upvalueindex(2)); /* lpq_Tuple MT */
  lua_setmetatable(L, -2);
  lua_getfield(L, 2, LPQ_RSET_FIELDS); /* rset_env.fields */
  lua_setuservalue(L, -2); /* tuple env is rset env fields */
  lua_pushcclosure(L, lpq_rset_rowsaux, 1);
  lua_pushvalue(L, 1);
  lua_pushinteger(L, 0);
  return 3;
}


/* =======   lpq_Tuple   ======= */

static int lpq_tuple__tostring (lua_State *L) {
  lua_pushfstring(L, LPQ_TUPLE_NAME ": %p", lua_touserdata(L, 1));
  return 1;
}

static int lpq_tuple__len (lua_State *L) {
  lpq_Tuple *T = (lpq_Tuple *) lua_touserdata(L, 1);
  if (!T->valid) lua_pushnil(L);
  else lua_pushinteger(L, T->row);
  return 1;
}

static int lpq_tuple__index (lua_State *L) {
  lpq_Tuple *T = (lpq_Tuple *) lua_touserdata(L, 1);
  PGresult *result = T->rset->result;
  if (!T->valid) lua_pushnil(L);
  else {
    lua_getuservalue(L, 1);
    lua_pushvalue(L, 2);
    lua_rawget(L, -2);
    if (lua_isnumber(L, -1)) { /* field name match? */
      int f = lua_tointeger(L, -1); /* field number */
      if (PQgetisnull(result, T->row, f)) lua_pushnil(L);
      else {
        lpq_pushvalue(L, PQftype(result, f), PQfmod(result, f),
          PQgetvalue(result, T->row, f), PQgetlength(result, T->row, f), result, f, T->row);
      }
    }
  }
  return 1;
}



/* =======   Interface   ======= */

static const luaL_Reg lpq_conn_mt[] = {
  {"__gc", lpq_conn__gc},
  {"__tostring", lpq_conn__tostring},
  {NULL, NULL}
};

static const luaL_Reg lpq_conn_func[] = {
  {"notifies", lpq_conn_notifies},
  {"poll", lpq_conn_poll},
  {"status", lpq_conn_status},
  {"finish", lpq_conn_finish},
  {"reset", lpq_conn_reset},
  {"resetstart", lpq_conn_resetstart},
  {"resetpoll", lpq_conn_resetpoll},
  {"socket", lpq_conn_socket},
  {"error", lpq_conn_error},
  {"db", lpq_conn_db},
  {"user", lpq_conn_user},
  {"pass", lpq_conn_pass},
  {"host", lpq_conn_host},
  {"port", lpq_conn_port},
  {"tty", lpq_conn_tty},
  {"options", lpq_conn_options},
  {"escape", lpq_conn_escape},
  {"isbusy", lpq_conn_isbusy},
  {"consume", lpq_conn_consume},
  {"query", lpq_conn_query},
  {NULL, NULL}
};

static const luaL_Reg lpq_plan_mt[] = {
  {"__tostring", lpq_plan__tostring},
  {"__len", lpq_plan__len},
  {NULL, NULL}
};

static const luaL_Reg lpq_plan_func[] = {
  {"query", lpq_plan_query},
  {NULL, NULL}
};

static const luaL_Reg lpq_rset_mt[] = {
  {"__gc", lpq_rset__gc},
  {"__tostring", lpq_rset__tostring},
  {"__len", lpq_rset__len},
  {NULL, NULL}
};

static const luaL_Reg lpq_rset_func[] = {
  {"fields", lpq_rset_fields},
  {"status", lpq_rset_status},
  {"error", lpq_rset_error},
  {"cmdstatus", lpq_rset_cmdstatus},
  {"fetch", lpq_rset_fetch},
  {NULL, NULL}
};

static const luaL_Reg lpq_tuple_mt[] = {
  {"__tostring", lpq_tuple__tostring},
  {"__len", lpq_tuple__len},
  {"__index", lpq_tuple__index},
  {NULL, NULL}
};


static const luaL_Reg psql_func[] = {
  {"connect", lpq_connect},
  {"start", lpq_start},
  {"register", lpq_register},
  {NULL, NULL}
};

int luaopen_psql (lua_State *L) {
  lua_pushlightuserdata(L, LPQ_TYPE_MT);
  lua_newtable(L); /* type MT table */
  lua_rawset(L, LUA_REGISTRYINDEX);
  /* === lpq_Conn === */
  luaL_newlibtable(L, lpq_conn_mt); /* lpq_Conn MT */
  lpq_registerlib(L, lpq_conn_mt, 0); /* push metamethods */
  luaL_newlibtable(L, psql_func); /* lib */
  lua_pushvalue(L, -2);
  lpq_registerlib(L, psql_func, 1); /* lib methods */
  lua_insert(L, -2);
  luaL_newlibtable(L, lpq_conn_func); /* lpq_Conn class */
  lua_pushvalue(L, -2);
  lpq_registerlib(L, lpq_conn_func, 1); /* push methods */
  /* store lpq_conn_prepare and lpq_conn_getplan */
  luaL_newlibtable(L, lpq_plan_mt); /* lpq_Plan MT */
  lua_pushvalue(L, -3); lua_pushvalue(L, -2); /* lpq_Conn and lpq_Plan MT */
  lua_pushcclosure(L, lpq_conn_prepare, 2);
  lua_setfield(L, -3, "prepare");
  lua_pushvalue(L, -3); lua_pushvalue(L, -2); /* lpq_Conn and lpq_Plan MT */
  lua_pushcclosure(L, lpq_conn_getplan, 2);
  lua_setfield(L, -3, "getplan");
  lua_insert(L, -3); /* lpq_Plan MT below lpq_Conn MT and class tables */
  /* store lpq_conn_getresult */
  luaL_newlibtable(L, lpq_rset_mt); /* lpq_Rset MT */
  lua_pushvalue(L, -3); lua_pushvalue(L, -2); /* lpq_Conn and lpq_Rset MT */
  lua_pushcclosure(L, lpq_conn_getresult, 2);
  lua_setfield(L, -3, "getresult");
  lua_pushvalue(L, -3); lua_pushvalue(L, -2); /* lpq_Conn and lpq_Rset MT */
  lua_pushcclosure(L, lpq_conn_exec, 2);
  lua_setfield(L, -3, "exec");
  lua_insert(L, -4); /* lpq_Rset MT below lpq_Conn MT, class, and lpq_Plan */
  /* set lpq_Conn MT */
  lua_setfield(L, -2, "__index"); /* MT(conn).__index = class(conn) */
  lua_pop(L, 1); /* lpq_Conn MT */
  /* === lpq_Plan === */
  lpq_registerlib(L, lpq_plan_mt, 0); /* push metamethods */
  luaL_newlibtable(L, lpq_plan_func); /* lpq_Plan class */
  lua_pushvalue(L, -2);
  lpq_registerlib(L, lpq_plan_func, 1); /* push methods */
  lua_pushvalue(L, -2); lua_pushvalue(L, -4); /* lpq_Plan and lpq_Rset MT */
  lua_pushcclosure(L, lpq_plan_exec, 2);
  lua_setfield(L, -2, "exec");
  lua_setfield(L, -2, "__index"); /* MT(plan).__index = class(plan) */
  lua_pop(L, 1); /* lpq_Plan MT */
  /* === lpq_Rset === */
  lpq_registerlib(L, lpq_rset_mt, 0); /* push metamethods */
  luaL_newlibtable(L, lpq_rset_func); /* lpq_Rset class */
  lua_pushvalue(L, -2);
  lpq_registerlib(L, lpq_rset_func, 1); /* push methods */
  luaL_newlibtable(L, lpq_tuple_mt); /* lpq_Tuple MT */
  lpq_registerlib(L, lpq_tuple_mt, 0); /* push metamethods */
  lua_pushvalue(L, -3); lua_pushvalue(L, -2); /* lpq_Rset and lpq_Tuple MT */
  lua_pushcclosure(L, lpq_rset_rows, 2);
  lua_setfield(L, -3, "rows");
  lua_pushcclosure(L, lpq_rset__index, 2); /* lpq_Rset class, lpq_Tuple MT */
  lua_setfield(L, -2, "__index");
  lua_pop(L, 1); /* lpq_Rset MT */
  return 1; /* lib */
}


/* {=================================================================
 * 
 *  Copyright (c) 2010 Luis Carvalho
 * 
 *  Permission is hereby granted, free of charge, to any person
 *  obtaining a copy of this software and associated documentation files
 *  (the "Software"), to deal in the Software without restriction,
 *  including without limitation the rights to use, copy, modify,
 *  merge, publish, distribute, sublicense, and/or sell copies of the
 *  Software, and to permit persons to whom the Software is furnished
 *  to do so, subject to the following conditions:
 * 
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 * 
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 *  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 *  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 *  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 * 
 *  ==================================================================} */

