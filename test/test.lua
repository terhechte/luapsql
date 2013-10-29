-- =================================================================
-- 
-- test.lua
-- Testing luapsql (Lua interface to PostgreSQL's libpq)
-- Luis Carvalho (lexcarvalho at gmail dot com)
-- See Copyright Notice at the bottom of psql.c
-- $Id: $
--
-- ==================================================================

local psql = require "psql"

-- ==========   non-blocking queries   =======
require "socket"
local socket_select = socket.select
local function select_timeout (read, write, timeout)
  local _, _, e = socket_select(read, write, timeout)
  return e == "timeout"
end

local function connsocket (conn)
  local fd = conn:socket() -- file descriptor
  return { getfd = function () return fd end } -- luasocket socket object
end

-- === connect ===
function connect (info, timeout)
  local conn = psql.start(info)
  local list = { connsocket(conn) }
  local baderror = string.format("bad connection [%s]", info)
  local ok, status = conn:status()
  assert(status ~= "CONNECTION_BAD", baderror)
  -- start polling
  ok, status = false, "PGRES_POLLING_WRITING"
  repeat
    if status == "PGRES_POLLING_WRITING" then
      assert(not select_timeout(nil, list, timeout), baderror) -- timed out?
    elseif status == "PGRES_POLLING_READING" then
      assert(not select_timeout(list, nil, timeout), baderror) -- timed out?
    else
      error(baderror)
    end
    ok, status = conn:poll()
  until ok
  return conn
end

-- === query ===
local function fetcher (conn)
  local readlist = { connsocket(conn) }
  return function (timeout)
    if not conn:isbusy() then -- ready to fetch?
      return true, conn:getresult()
    end
    if select_timeout(readlist, nil, timeout) then -- not ready?
      return false
    end
    assert(conn:consume()) -- consume some input
    if conn:isbusy() then -- still busy?
      return false
    end
    return true, conn:getresult()
  end
end

-- assume conn:query() was successful
local function getlastresult (conn, timeout)
  local rset, reported
  local fetchresult = fetcher(conn)
  repeat
    local ready, s = fetchresult(timeout)
    if ready then
      if s ~= nil then -- not finished?
        rset = s -- update last result
        local status = s:status()
        if status == "PGRES_COPY_IN" or status == "PGRES_COPY_OUT"
          or conn:status() == "CONNECTION_BAD" then break end
      else -- finalize
        if reported then io.stderr:write("\n") end
      end
    else -- do something while conn is busy
      io.stderr:write(".")
      reported = true
    end
  until ready and s == nil -- finished?
  return rset
end

function exec (conn, stmt, timeout)
  assert(conn:query(stmt))
  return getlastresult(conn, timeout)
end

function execparams (conn, stmt, ...)
  local plan = assert(conn:prepare(stmt)) -- unnamed
  assert(plan:query(...))
  return getlastresult(conn)
end

function execprepared (conn, name, ...)
  local plan = assert(conn:getplan(name))
  assert(plan:query(...))
  return getlastresult(conn)
end


-- ==========   pretty printer   =======
local function pad (s, l)
  return s .. string.rep(" ", l - #s)
end

local function center (s, l)
  local n = l - #s
  local l1 = (n - n % 2) / 2
  local l2 = n - l1
  return string.rep(" ", l1) .. s .. string.rep(" ", l2)
end

function list (rset)
  if rset == nil then return end
  local status = rset:status()
  if status == "PGRES_TUPLES_OK" then
    local n = #rset
    local fields = rset:fields()
    -- find field lengths
    local length = {}
    for f, k in pairs(fields) do length[k] = #f end
    for i = 1, n do
      local tup = rset[i]
      for f, k in pairs(fields) do
        length[k] = math.max(#tostring(tup[f]), length[k])
      end
    end
    -- print header
    local line = {}
    for f, k in pairs(fields) do
      line[k + 1] = center(f, length[k])
    end
    print(" " .. table.concat(line, " | ") .. " ") -- header
    for _, k in pairs(fields) do
      line[k + 1] = string.rep("-", length[k])
    end
    print("-" .. table.concat(line, "-+-") .. "-") -- underline
    -- print tuples
    for i = 1, n do
      local tup = rset[i]
      for f, k in pairs(fields) do
        line[k + 1] = pad(tostring(tup[f]), length[k])
      end
      print(" " .. table.concat(line, " | ") .. " ")
    end
    -- print footer
    if n == 1 then
      print("(1 row)")
    else
      print(string.format("(%d rows)", n))
    end
  elseif status == "PGRES_COMMAND_OK" then
    print(rset:cmdstatus())
  end
end


-- ==========   test connection   =======
assert(arg[1] ~= nil, "connection info expected")
c = connect(arg[1])
assert(c:status())

local function checkset (conn, rset)
  assert(rset:status() == "PGRES_COMMAND_OK", conn:error())
  print(rset:cmdstatus())
end

local function checktest (test, conn, ...)
  checkset(conn, conn:exec"BEGIN")
  local status, e = pcall(test, conn, ...)
  if status then
    checkset(conn, conn:exec"COMMIT")
  else
    checkset(conn, conn:exec"ROLLBACK")
    error(e)
  end
end

local function register (typename)
  local class = require("pqtype." .. typename)
  local o = c:exec("select '" .. typename .. "'::regtype::oid as o")[1].o
  psql.register(o, getmetatable(class()))
  print("TYPE `" .. typename .. "' REGISTERED")
end

-- === first test ===
local function test1 (conn)
  -- register custom types
  local int2 = require"pqtype.int2"
  register"int2"
  local point = require"pqtype.point"
  register"point"
  -- create table
  checkset(conn, conn:exec("CREATE TABLE test " ..
    "(i1 smallint, i2 int, f1 real, f2 double precision, p point," ..
    " c char(5), t text, v varchar)"))
  -- insert values
  local cmd = "INSERT INTO test VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
  checkset(conn, execparams(conn, cmd,
      int2(1), 1, math.pi, math.pi, point(1, math.pi),
      "abcde", "hello", "Lua"))
  assert(conn:prepare(cmd))
  checkset(conn, execprepared(conn, "",
      int2(2), 2, math.exp(1), math.exp(1), point(2, math.exp(1)),
      "abc", "hi", "PostgreSQL"))
  -- list and drop table
  list(conn:exec"SELECT * FROM test")
  checkset(conn, conn:exec"DROP TABLE test")
end
print("TEST 1")
print(string.rep("-", 40))
checktest(test1, c)
print(string.rep("=", 40))

-- === second test ===
local function test2 (conn, n, s)
  -- create table
  checkset(conn, conn:exec"CREATE TABLE bigtest (i int, f double precision)")
  -- insert values
  local plan = assert(c:prepare"INSERT INTO bigtest VALUES ($1, $2)")
  for i = 1, n do
    assert(#plan:exec(i, math.sin(i)) == 1, conn:error())
  end
  -- check table size and drop table
  local r = exec(c, "SELECT * FROM bigtest")
  assert(#r == n, conn:error())
  list(execparams(c, "SELECT * FROM bigtest WHERE i < $1 AND f > 0", s))
  checkset(conn, conn:exec"DROP TABLE bigtest")
end
print("TEST 2")
print(string.rep("-", 40))
checktest(test2, c, 1e4, 20)
print(string.rep("=", 40))

-- === third test ===
local function test3 (conn)
  -- create table
  checkset(conn, conn:exec"CREATE TABLE bytetest (b bytea)")
  -- insert values
  checkset(conn, execparams(conn, "INSERT INTO bytetest VALUES ($1)",
    string.dump(function() print"OK" end)))
  -- test and drop table
  r = conn:exec("SELECT * FROM bytetest")
  loadstring(r[1].b)()
  checkset(conn, conn:exec"DROP TABLE bytetest")
end
print("TEST 3")
print(string.rep("-", 40))
checktest(test3, c)
print(string.rep("=", 40))

