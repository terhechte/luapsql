/* {=================================================================
 *
 * lpqtype.c
 * Useful routines for registering types
 * Luis Carvalho (lexcarvalho at gmail dot com)
 * See Copyright Notice at the bottom of psql.c
 * $Id: $
 *
 * ==================================================================} */

#include <lauxlib.h>
#include "lpqtype.h"

/* according to recv routines in pqformat */
uint32 lpq_getuint32 (const char *v) {
  return ntohl(*(uint32 *)v);
}

int64 lpq_getint64 (const char *v) {
  int64 result;
  result = lpq_getuint32(&v[0]);
  result <<= 32;
  result |= lpq_getuint32(&v[4]);
  return result;
}

float4 lpq_getfloat4 (const char *v) {
  union { float4 f; uint32 i; } swap;
  swap.i = lpq_getuint32(v);
  return swap.f;
}

float8 lpq_getfloat8 (const char *v) {
  union { float8 f; int64 i; } swap;
  swap.i = lpq_getint64(v);
  return swap.f;
}


/* according to send routines in pqformat */
void lpq_senduint32 (luaL_Buffer *b, uint32 n32) {
  n32 = htonl(n32);
  luaL_addlstring(b, (const char *) &n32, 4);
}

void lpq_sendint64 (luaL_Buffer *b, int64 i) {
  lpq_senduint32(b, (uint32) (i >> 32)); /* higher order first */
  lpq_senduint32(b, (uint32) i); /* lower order next */
}

void lpq_sendfloat4 (luaL_Buffer *b, float4 f) {
  union { float4 f; uint32 i; } swap;
  swap.f = f;
  lpq_senduint32(b, swap.i);
}

void lpq_sendfloat8 (luaL_Buffer *b, float8 f) {
  union { float8 f; int64 i; } swap;
  swap.f = f;
  lpq_sendint64(b, swap.i);
}

