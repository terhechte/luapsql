# Makefile for LuaPSQL
# (in case you don't like Luarocks :)

PGINC = -I/usr/local/Cellar/postgresql/9.2.4/include/
PGLIB = -L/usr/local/Cellar/postgresql/9.2.4/lib/ -lpq -L/usr/local/lib -lpqtypes
LUAINC = -I/usr/local/Cellar/lua/5.1.5/include/
LUALIB = -L/usr/local/Cellar/lua/5.1.5/lib/ -llua

#PGINC = -I/usr/include/postgresql
#PGLIB = -lpq -lpqtypes
#LUAINC = -I/usr/include/lua5.1

# Lua for Windows / PostgreSQL installer
#PGDIR = c:\Program Files\PostgreSQL\8.4
#PGINC = -I"$(PGDIR)\include"
#PGLIB = -L"$(PGDIR)\lib" -lpq
#LUADIR = c:\Program Files\Lua\5.1
#LUAINC = -I"$(LUADIR)\include"
#LUALIB = -L"$(LUADIR)\lib" -llua5.1
#RTLIB = -lws2_32 -lgcc -lmsvcr80

CC = gcc
CFLAGS = -W -Wall -g -fPIC $(LUAINC) $(PGINC)
RM = rm -f

OBJ = psql.o lpqtype.o
LIB = psql.so
#LIB = psql.dll

POBJ = pqtype.o lpqtype.o
PLIB = pqtype.so
#PLIB = pqtype.dll

all : $(LIB) $(PLIB)

$(LIB) : $(OBJ)
	$(CC) $(CFLAGS) -shared -o $@ $(OBJ) $(PGLIB) $(LUALIB) $(RTLIB)

$(PLIB) : $(POBJ)
	$(CC) $(CFLAGS) -shared -o $@ $(POBJ) $(LUALIB) $(RTLIB)

clean :
	$(RM) $(OBJ) $(POBJ)

klean : clean
	$(RM) $(LIB) $(PLIB)

refresh : clean all

