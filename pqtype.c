/* {=================================================================
 *
 * pqtype.c
 * Examples of registered types for luapsql
 * Luis Carvalho (lexcarvalho at gmail dot com)
 * See Copyright Notice at the bottom of psql.c
 * $Id: $
 *
 * ==================================================================} */

#include <lua.h>
#include <lauxlib.h>
#include "lpqtype.h"

#if LUA_VERSION_NUM <= 501
#define registerlib(L,l,n) luaL_openlib(L,NULL,l,n)
#define luaL_newlibtable(L,l) \
  lua_createtable(L, 0, sizeof(l)/sizeof((l)[0]) - 1)
#else
#define registerlib luaL_setfuncs
#endif


/* =======   int2   ======= */

typedef short int16;
typedef unsigned short uint16;

static int16 *newint2 (lua_State *L, int16 v) {
  int16 *i = (int16 *) lua_newuserdata(L, sizeof(int16));
  lua_pushvalue(L, lua_upvalueindex(1));
  lua_setmetatable(L, -2);
  *i = v;
  return i;
}

static int int2_call (lua_State *L) {
  newint2(L, (int16) luaL_optinteger(L, 1, 0));
  return 1;
}

static int int2__tostring (lua_State *L) {
  int16 *i = (int16 *) lua_touserdata(L, 1);
  lua_pushfstring(L, "%d", (int) *i);
  return 1;
}


/* PQ type interface */
static int16 getint16 (const char *s) {
  return (int16) ntohs(*(uint16 *) s);
}

static int int2__recv (lua_State *L) {
  newint2(L, getint16(luaL_checkstring(L, 1)));
  return 1;
}

static void sendint16 (luaL_Buffer *b, int16 i) {
  uint16 u = htons((uint16) i);
  luaL_addlstring(b, (const char *) &u, sizeof(uint16));
}

static int int2__send (lua_State *L) {
  int16 *i = (int16 *) lua_touserdata(L, 1);
  luaL_Buffer buf;
  luaL_buffinit(L, &buf);
  sendint16(&buf, *i);
  luaL_pushresult(&buf);
  return 1;
}


static const luaL_Reg pqtype_int2_mt[] = {
  {"__tostring", int2__tostring},
  {LPQ_REGMT_SEND, int2__send},
  {LPQ_REGMT_RECV, int2__recv},
  {NULL, NULL}
};

int luaopen_pqtype_int2 (lua_State *L) {
  luaL_newlibtable(L, pqtype_int2_mt);
  lua_pushvalue(L, -1);
  registerlib(L, pqtype_int2_mt, 1);
  lua_pushcclosure(L, int2_call, 1);
  return 1;
}

/* =======   point   ======= */

typedef struct {
  double x;
  double y;
} point;

static point *newpoint (lua_State *L, double x, double y) {
  point *p = (point *) lua_newuserdata(L, sizeof(point));
  lua_pushvalue(L, lua_upvalueindex(1));
  lua_setmetatable(L, -2);
  p->x = x;
  p->y = y;
  return p;
}

static int point_call (lua_State *L) {
  newpoint(L, luaL_optnumber(L, 1, 0), luaL_optnumber(L, 2, 0));
  return 1;
}

static int point__tostring (lua_State *L) {
  point *p = (point *) lua_touserdata(L, 1);
  lua_pushfstring(L, "(%f, %f)", p->x, p->y);
  return 1;
}


static int point__recv (lua_State *L) {
  const char *s = luaL_checkstring(L, 1);
  newpoint(L, (double) lpq_getfloat8(&s[0]), /* x */
      (double) lpq_getfloat8(&s[8])); /* y */
  return 1;
}

static int point__send (lua_State *L) {
  point *p = (point *) lua_touserdata(L, 1);
  luaL_Buffer buf;
  luaL_buffinit(L, &buf);
  lpq_sendfloat8(&buf, p->x);
  lpq_sendfloat8(&buf, p->y);
  luaL_pushresult(&buf);
  return 1;
}


static const luaL_Reg pqtype_point_mt[] = {
  {"__tostring", point__tostring},
  {LPQ_REGMT_SEND, point__send},
  {LPQ_REGMT_RECV, point__recv},
  {NULL, NULL}
};

int luaopen_pqtype_point (lua_State *L) {
  luaL_newlibtable(L, pqtype_point_mt);
  lua_pushvalue(L, -1);
  registerlib(L, pqtype_point_mt, 1);
  lua_pushcclosure(L, point_call, 1);
  return 1;
}

