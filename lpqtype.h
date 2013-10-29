/* {=================================================================
 *
 * lpqtype.h
 * Useful routines for registering types
 * Luis Carvalho (lexcarvalho at gmail dot com)
 * See Copyright Notice at the bottom of psql.c
 * $Id: $
 *
 * ==================================================================} */

#include <lua.h>
#include <lauxlib.h>

/* for ntoh{s,l}, hton{s,l} */
#if defined(_MSC_VER) || defined(__MINGW32__)
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

typedef unsigned int uint32;
typedef long long int int64;
typedef float float4;
typedef double float8;

#define VARHDRSZ ((int) sizeof(int))

/* in registered metatable: */
#define LPQ_REGMT_OID   "__oid"
#define LPQ_REGMT_RECV  "__recv"
#define LPQ_REGMT_SEND  "__send"

/* recv */
uint32 lpq_getuint32 (const char *v);
int64 lpq_getint64 (const char *v);
float4 lpq_getfloat4 (const char *v);
float8 lpq_getfloat8 (const char *v);
/* send */
void lpq_senduint32 (luaL_Buffer *b, uint32 n32);
void lpq_sendint64 (luaL_Buffer *b, int64 i);
void lpq_sendfloat4 (luaL_Buffer *b, float4 f);
void lpq_sendfloat8 (luaL_Buffer *b, float8 f);

