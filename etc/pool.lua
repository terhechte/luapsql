-- =================================================================
-- 
-- pool.lua
-- Connection pool using luapsql 
-- Luis Carvalho (lexcarvalho at gmail dot com)
-- See Copyright Notice at the bottom of psql.c
--
-- ==================================================================

local psql = require "psql"
local socket = require "socket"

local assert, ipairs, setmetatable = assert, ipairs, setmetatable
local psql_connect, socket_select = psql.connect, socket.select

module(...)
local mt = {__index = _M}

function new ()
  return setmetatable({}, mt)
end

function add (pool, info)
  local conn = assert(psql_connect(info))
  local fd = conn:socket()
  local n = #pool + 1
  local c = { conn=conn, pos=n, getfd=function() return fd end }
  pool[n] = c
  return c
end

function exec (pool, cmdlist)
  assert(#pool >= #cmdlist, "not enough connections in pool")
  local active = {}
  for i, cmd in ipairs(cmdlist) do
    assert(pool[i].conn:query(cmd))
    active[i] = pool[i]
  end
  local result = {}
  while active[1] ~= nil do -- not empty?
    local read = socket_select(active)
    for _, s in ipairs(read) do
      assert(s.conn:consume())
    end
    local shift = 0 
    for i, s in ipairs(active) do
      if not s.conn:isbusy() then -- ready to fetch?
        result[s.pos] = s.conn:getresult()
        active[i] = nil
        shift = shift + 1
      elseif shift > 0 then -- shift elements down?
        active[i - shift] = s
      end
    end
  end
  return result
end

