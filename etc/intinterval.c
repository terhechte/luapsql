#include <lua.h>
#include <lauxlib.h>
#include <postgres.h>

#define LPQ_REGMT_RECV  "__recv"
#define LPQ_REGMT_SEND  "__send"

#if LUA_VERSION_NUM <= 501
#define lregister(L,l,n) luaL_openlib(L,NULL,l,n)
#else
#define lregister luaL_setfuncs
#endif

typedef struct {
  int32 low;
  int32 high;
} int_interval;

static int_interval *newintinterval (lua_State *L, int32 low, int32 high) {
  int_interval *ii = (int_interval *) lua_newuserdata(L,
      sizeof(int_interval));
  lua_pushvalue(L, lua_upvalueindex(1));
  lua_setmetatable(L, -2);
  ii->low = low;
  ii->high = high;
  return ii;
}

static int intinterval_call (lua_State *L) {
  newintinterval(L, luaL_optinteger(L, 1, 0), luaL_optinteger(L, 2, 0));
  return 1;
}

static int intinterval__tostring (lua_State *L) {
  int_interval *ii = (int_interval *) lua_touserdata(L, 1);
  lua_pushfstring(L, "(%d, %d)", ii->low, ii->high);
  return 1;
}

/* PQ type interface */
static uint32 getuint32 (const char *v) {
  return ntohl(*(uint32 *)v);
}

static int intinterval__recv (lua_State *L) {
  const char *s = luaL_checkstring(L, 1);
  newintinterval(L, (int32) getuint32(&s[0]), /* low */
      (int32) getuint32(&s[4])); /* high */
  return 1;
}

static void senduint32 (luaL_Buffer *b, uint32 n32) {
  n32 = htonl(n32);
  luaL_addlstring(b, (const char *) &n32, 4);
}

static int intinterval__send (lua_State *L) {
  int_interval *ii = (int_interval *) lua_touserdata(L, 1);
  luaL_Buffer buf;
  luaL_buffinit(L, &buf);
  senduint32(&buf, (uint32) ii->low);
  senduint32(&buf, (uint32) ii->high);
  luaL_pushresult(&buf);
  return 1;
}


static const luaL_reg pqtype_intinterval_mt[] = {
  {"__tostring", intinterval__tostring},
  {LPQ_REGMT_SEND, intinterval__send},
  {LPQ_REGMT_RECV, intinterval__recv},
  {NULL, NULL}
};

int luaopen_pqtype_intinterval (lua_State *L) {
  lua_newtable(L);
  lua_pushvalue(L, -1);
  lregister(L, pqtype_intinterval_mt, 1);
  lua_pushcfunction(L, intinterval_call);
  return 1;
}

