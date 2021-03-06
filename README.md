FORK
=======
This is a small fork of LuaPSQL to add some small additional features.
I also moved it from Google Code to Github. The original author is Luis Carvalho.
It can be found here:
https://code.google.com/p/luapsql/

Most notably support for array datatypes. These are currently not supported
in any of the other Postgresql drivers for lua.

I've added *read* support for the following additional datatypes:

* Integer[]
* Character Varying[]
* Character[]
* Timestamp[] (with or without timezone) converted to int (timezones are converted into the local machine's timezone) 
* Timestamp/tz
* BigInt
* BigInt[]
* JSON
* Real[]
* Double Precision[]

The data for these fields is being returned as a Lua table.

- Please note that writing this data is currently not supported. (i.e. insert/update)

- Also, currently only one-dimenstional types (i.e. type[], and not type[][]) are supported

Adding support for more array types should not be too difficult and I may
add additional types in the future.

In order to support types, I had to add another dependency to
the project. The project now also requires libpqtypes, a library
that greatly simplifies the handling of Postgres array columns.

The library can be found here:
http://libpqtypes.esilo.com/

The code could need a bit of a refactoring since there're
currently many code duplications.

At the bottom of this document is a short introduction into hacking
for people interested in adding more types support to this project.

LuaPSQL
=======

This is LuaPSQL, a binding of PostgreSQL's client library, libpq, for Lua.
This module is specific to PostgreSQL and offers more functionality when
compared to the generic LuaSQL bindings:

  * Binary types as equivalent as possible to Lua types (moreover, check
    "registered types" below)
  * Asynchronous connections and query executions
  * Parametrizable statements
  * Prepared statements

The routines are, however, *low*-level methods, most of them simply wrapping
their C counterparts. For this reason, a more detailed documentation is not
included and the user can refer to PostgreSQL's documentation on libpq for
details -- check the chapter entitled "libpq - C library". This binding is
fairly complete, with the sole exception of function associated with the COPY
command.

Here's a simple example:

``` Lua
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
```

More usage examples can be found in the "test" and "etc" folders.


Registered types
----------------

It is possible to register specific types in LuaPSQL using:

``` Lua
    metatable = psql.register(oid [, metatable])
```

If `metatable` is not provided a new table will be created. psql.register sets
field `__oid` to the provided `oid` parameter (to get the oid of a certain
type, issue "select 'typename'::regtype::oid" in psql.)

The metatable for your registered type should contain two fields, `__send` and
`__recv` that specify how values are translated from Lua to PostgreSQL and
vice-versa, respectively:

``` Lua
    bytea_str = objmt.__send(obj)
    obj = objmt.__recv(bytea_str, fmod)
```

For examples, check `pqtype.c`.


Installation
------------

The installation should be straightforward as LuaPSQL uses Luarocks. However,
if you want to run the tests in the `test` folder you will also need
`pqtype.so`; to compile it, modify the rockspec file according to the comments
in it.

You may need to install libpqtypes by hand. It can be found here:
http://libpqtypes.esilo.com/


Extending:
=========

Is your favorite array type missing? Or another type for that matter?

As explained above, some types can easily be implemented via metatables,
though this doesn't work for array types.

Additional Documentation on postgresql types / array types can be found here:
(this is especially useful if you're interested in adding support
for writing / updating such values)

- http://libpqtypes.esilo.com/man3/pqt-specs.html
- http://libpqtypes.esilo.com/man3/PQgetf.html
- http://libpqtypes.esilo.com/man3/PQputf.html

Some info on what to feed into lua afterwards:

- http://pgl.yoyo.org/luai/i/lua_pushnumber

If you want to add support for reading a new type, go into psql.c
and have a look at 'lpq_pushvalue'. This function has a rather
long switch statement (which really needs to be refactored).
In that statement, we're performing a comparison for all the
different PostgreSQL datatypes. If the correct one is found,
we're getting the data from the Postgres query result, converting
it to Lua data objects, and pushing it onto the Lua stack.

Simple Example (convert string):

``` c
#define VARCHAROID 1043
...
    case VARCHAROID:
      lua_pushlstring(L, value, length);
      break;
```

The value of the type (1043 in this case) can be retrieved
from doing a quick query in psql: "select 'varchar'::regtype::oid;".

If you want to add support for a new datatype, just research the
datatype, and build conversion utilities for the type in question,
so it can be handed of to Lua. If you happen to stumble upon a more
difficult type, also have a look at the libpqtypes specs (http://libpqtypes.esilo.com/man3/pqt-specs.html)
as it has a wealth of documentation on this matter. The implementation of
TIMESTAMPOID serves as an example of this.

Finally, adding support for array operators is a bit more tricky.
The gist is that you're retrieving a second query result from the
first query result. The second query result is just a list of items of
the specified array datatype:

``` C
case VARCHARARRAYOID: {
      PGarray arr;
      int i = 0;

      // Get a new result from the result into a PGArray Struct
      PQgetf(result, rowindex, "%varchar[]", field_number, &arr);

      // Count the number of 'rows'
      int ntups = PQntuples(arr.res);

      // Create a new Lua Table and push it onto the stack
      lua_newtable(L);

      // Iterate over the rows
      for(i=0; i < ntups; i++) {
        // Retrieve a Varchar Value from each row
        PGvarchar val;
        PQgetf(arr.res, i, "%varchar", 0, &val); // we always take field 0

        // Add an index to the table
        lua_pushnumber(L, i + 1);

        // Add the string to the table
        lua_pushlstring(L, val, strlen(val));

        // Set the table
        lua_settable(L, -3);
      }
      break;
    }
```

If you want to add support for writing new data types, again,
have a look at http://libpqtypes.esilo.com/man3/pqt-specs.html as it contains
a plethora of information on this matter.

In that case you want to add functions to lpq_tovalue in psql.c

If you quickly want to setup a test table with various array types,
here's some SQL:

``` SQL
-- Creating
CREATE TABLE lala
(
  intarray integer[],
  stringarray character varying[],
  floatarray real[],
  bigintarray bigint[],
  jsonfield json,
  jsonarray json[]
)
```

``` SQL
-- Inserting
INSERT INTO test_table(
            intarray, stringarray, floatarray, bigintarray, jsonfield, jsonarray)
    VALUES (ARRAY[1, 2, 3], ARRAY['kalr', 'jochen'], ARRAY[1.1, 1.7], ARRAY[10000, 480248], '{"a":1}'::json, ARRAY['{"b":2}'::json, '[1, 3, 7]'::json]);
```