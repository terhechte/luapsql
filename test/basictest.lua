local psql = require "psql"

-- check result set
local function checkset (conn, rset)
  local status = rset:status()
  assert(status == "PGRES_COMMAND_OK" or status == "PGRES_TUPLES_OK",
    conn:error())
  return rset
end

-- connect
local conn = psql.connect "dbname=test"
assert(conn:status(), conn:error())

-- create and populate table
checkset(conn, conn:exec"CREATE TABLE x (i int, f double precision)")
local plan = assert(conn:prepare("INSERT INTO x VALUES ($1, $2)"))
for i = 1, 10 do
  checkset(conn, plan:exec(i, math.sin(math.pi / i)))
end

-- list table
local rset = checkset(conn, conn:exec"SELECT * FROM x")
local f = rset:fields()
for i, t in rset:rows() do
  for k in pairs(f) do
    print(i, k, t[k])
  end
end

-- wrap up
checkset(conn, conn:exec"DROP TABLE x")
conn:finish()

