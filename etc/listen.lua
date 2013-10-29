-- =================================================================
-- 
-- listen.lua
-- Listen notifications using luapsql 
-- Luis Carvalho (lexcarvalho at gmail dot com)
-- See Copyright Notice at the bottom of psql.c
--
-- ==================================================================

local psql_connect = require"psql".connect
local assert, type = assert, type
local setmetatable, tostring = setmetatable, tostring

module(...)
local mt = {__index = _M}

local function notify (o)
  local conn = o.conn
  while true do
    local c, _, payload = conn:notifies()
    if c == nil then break end
    local cb = o.channel[c]
    if cb ~= nil then cb(payload, c) end
  end
end

function new (info)
  local c = assert(psql_connect(info))
  return setmetatable({conn = c, channel = {}}, mt), c:status()
end

function listen (o, c, cb)
  local conn, c = o.conn, tostring(c)
  assert(type(cb) == "function", "function expected")
  local rset = conn:exec("LISTEN " .. c)
  assert(rset:status() == "PGRES_COMMAND_OK", conn:error())
  o.channel[c] = cb
end

function unlisten (o, c)
  local conn = o.conn
  if o.channel[c] ~= nil then
    local rset = conn:exec("UNLISTEN " .. c)
    assert(rset:status() == "PGRES_COMMAND_OK", conn:error())
    o.channel[c] = nil
  end
end

function exec (o, cmd)
  local conn = o.conn
  local rset = conn:exec(cmd)
  notify(o) -- check notifications
  return rset
end

function check (o)
  local conn = o.conn
  assert(conn:consume())
  notify(o)
end

