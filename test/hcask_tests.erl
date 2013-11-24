%%
%%   Copyright (c) 2012, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
-module(hcask_tests).
-include_lib("eunit/include/eunit.hrl").

hcask_test_() ->
   {
      setup,
      fun init/0,
      fun free/1,
      [
          {"create", fun create/0}
         ,{"lookup", fun lookup/0}
         ,{"update", fun update/0}
         ,{"delete", fun delete/0}
         ,{"lookup init", fun lookup_init/0}
         ,{"lookup eq",   fun lookup_eq/0}
         ,{"lookup gt",   fun lookup_gt/0}
         ,{"lookup lt",   fun lookup_lt/0}
         ,{"lookup ge",   fun lookup_ge/0}
         ,{"lookup le",   fun lookup_le/0}
         ,{"lookup free", fun lookup_free/0}

         ,{"create mkv",  fun create_mkv/0}
         ,{"lookup mkv",  fun lookup_mkv/0}
         ,{"update mkv",  fun update_mkv/0}
         ,{"delete mkv",  fun delete_mkv/0}

         % ,{"create pkv",  fun create_pkv/0}
         % ,{"lookup pkv",  fun lookup_pkv/0}
         % ,{"update pkv",  fun update_pkv/0}
         % ,{"delete pkv",  fun delete_pkv/0}
      ]
   }.
-record(kv, {key,   val}).
-record(mkv,{a,b,c, val}).

%%
%% cask 
-define(KV, [
   {peer,     mysqld}
  ,{struct,   kv}
  ,{property, [key,val]}
  ,{db,       hcask}
]).
-define(MKV,[
   {peer,     mysqld}
  ,{struct,   mkv}
  ,{keylen,     3}
  ,{property, [a, b, c, val]}
  ,{db,       hcask}
]).

%%%------------------------------------------------------------------
%%%
%%% setup
%%%
%%%------------------------------------------------------------------   


init() ->
   hcask:start(),
   {ok, _} = hcask:join(mysqld, [{host, mysqld}, {reader, 9998}, {writer, 9999}, {pool, 10}]).

free(Pids) ->
   _ = hcask:leave(mysqld).

pkv_resolver(#kv{key=Key}) 
 when Key rem 2 =:= 0 ->
   {ns, kv0, 'PRIMARY'};
pkv_resolver(Key) 
 when is_integer(Key), Key rem 2 =:= 0 ->
   {ns, kv0, 'PRIMARY'};
pkv_resolver(_) ->
   {ns, kv1, 'PRIMARY'}.

%%%------------------------------------------------------------------
%%%
%%% 
%%%
%%%------------------------------------------------------------------   

%%
%%
create() ->
   Cask    = hcask:new(?KV),
   {ok, _} = hcask:create(Cask, #kv{key=1, val="abc"}).

%%
%%
lookup() ->
   Cask    = hcask:new(?KV),
   {ok, [#kv{key = 1, val = <<"abc">>}]} = hcask:lookup(Cask, 1).

%%
%%
update() ->
   Cask    = hcask:new(?KV),
   {ok, _} = hcask:update(Cask, #kv{key=1, val="abc++"}),
   {ok, [#kv{key = 1, val = <<"abc++">>}]} = hcask:lookup(Cask, 1).

%%
%%
delete() ->
   Cask     = hcask:new(?KV),
   {ok, _}  = hcask:delete(Cask, 1),
   {ok, []} = hcask:lookup(Cask, 1).

%%
%%
-define(T1, #kv{key=1, val = <<"qaz">>}).
-define(T2, #kv{key=2, val = <<"wsx">>}).
-define(T3, #kv{key=3, val = <<"edc">>}). 
-define(T4, #kv{key=4, val = <<"rfv">>}).
-define(T5, #kv{key=5, val = <<"tgb">>}).
-define(T6, #kv{key=6, val = <<"yhn">>}). 
-define(T7, #kv{key=7, val = <<"ujm">>}).
-define(T8, #kv{key=8, val = <<"ik,">>}).
-define(T9, #kv{key=9, val = <<"ol.">>}).

lookup_init() ->
   Cask = hcask:new(?KV),
   lists:foreach(
      fun(X) -> {ok, _} = hcask:create(Cask, X) end,
      [?T1, ?T2, ?T3, ?T4, ?T5, ?T6, ?T7, ?T8, ?T9]
   ).

lookup_eq() ->
   Cask = hcask:new(?KV),
   {ok, [?T1]} = hcask:lookup(Cask, 
      hcask:q(hcask:eq(key, 1))
   ).

lookup_gt() ->
   Cask = hcask:new(?KV),
   {ok, [?T2, ?T3]} = hcask:lookup(Cask, 
      hcask:q(hcask:gt(key, 1), 2)
   ),
   {ok, [?T2, ?T3, ?T4, ?T5]} = hcask:lookup(Cask, 
      hcask:q(hcask:gt(key, 1), 4)
   ).

lookup_lt() ->
   Cask = hcask:new(?KV),
   {ok, [?T8, ?T7]} = hcask:lookup(Cask, 
      hcask:q(hcask:lt(key, 9), 2)
   ),
   {ok, [?T8, ?T7, ?T6, ?T5]} = hcask:lookup(Cask, 
      hcask:q(hcask:lt(key, 9), 4)
   ).

lookup_ge() ->
   Cask = hcask:new(?KV),
   {ok, [?T1, ?T2]} = hcask:lookup(Cask, 
      hcask:q(hcask:ge(key, 1), 2)
   ),
   {ok, [?T1, ?T2, ?T3, ?T4]} = hcask:lookup(Cask, 
      hcask:q(hcask:ge(key, 1), 4)
   ).

lookup_le() ->
   Cask = hcask:new(?KV),
   {ok, [?T9, ?T8]} = hcask:lookup(Cask,
      hcask:q(hcask:le(key, 9), 2)
   ),
   {ok, [?T9, ?T8, ?T7, ?T6]} = hcask:lookup(Cask, 
      hcask:q(hcask:le(key, 9), 4)
   ). 

lookup_free() ->
   Cask = hcask:new(?KV),
   lists:foreach(
      fun(X) -> {ok, _} = hcask:delete(Cask, X#kv.key) end,
      [?T1, ?T2, ?T3, ?T4, ?T5, ?T6, ?T7, ?T8, ?T9]
   ).


%%
%%
create_mkv() ->
   Cask = hcask:new(?MKV),
   {ok, _} = hcask:create(Cask, #mkv{a=1, b=1, c=1, val="abc"}).

lookup_mkv() ->
   Cask = hcask:new(?MKV),
   {ok, [#mkv{a=1, b=1, c=1, val = <<"abc">>}]} = hcask:lookup(Cask, {1, 1, 1}).

update_mkv() ->
   Cask = hcask:new(?MKV),
   {ok, _} = hcask:update(Cask, #mkv{a=1, b=1, c=1, val="abc++"}),
   {ok, [#mkv{a=1, b=1, c=1, val = <<"abc++">>}]} = hcask:lookup(Cask, {1, 1, 1}).

delete_mkv() ->
   Cask = hcask:new(?MKV),
   {ok, _}  = hcask:delete(Cask, {1, 1, 1}),
   {ok, []} = hcask:lookup(Cask, {1, 1, 1}).


% %%
% %%
% create_pkv() ->
%    ok = mets:create(pkv, #kv{key=1, val="abc"}),
%    ok = mets:create(pkv, #kv{key=2, val="abc"}),

%    ok = '<<'(mets:create_(pkv, #kv{key=3, val="abc"})),
%    ok = '<<'(mets:create_(pkv, #kv{key=4, val="abc"})),

%    ok = mets:create_(pkv, #kv{key=5, val="abc"}, false),
%    ok = mets:create_(pkv, #kv{key=6, val="abc"}, false).

% lookup_pkv() ->
%    {ok, [#kv{key=1, val = <<"abc">>}]} = mets:lookup(pkv, 1),
%    {ok, [#kv{key=2, val = <<"abc">>}]} = mets:lookup(pkv, 2),

%    {ok, [#kv{key=3, val = <<"abc">>}]} = '<<'(mets:lookup_(pkv, 3)),
%    {ok, [#kv{key=4, val = <<"abc">>}]} = '<<'(mets:lookup_(pkv, 4)),

%    ok = mets:lookup_(pkv, 5, false),
%    ok = mets:lookup_(pkv, 6, false).


% update_pkv() ->
%    ok = mets:update(pkv, #kv{key=1, val="abc++"}),
%    ok = mets:update(pkv, #kv{key=2, val="abc++"}),

%    ok = '<<'(mets:update_(pkv, #kv{key=3, val="abc++"})),
%    ok = '<<'(mets:update_(pkv, #kv{key=4, val="abc++"})),

%    ok = mets:update_(pkv, #kv{key=5, val="abc++"}, false),
%    ok = mets:update_(pkv, #kv{key=6, val="abc++"}, false),

%    {ok, [#kv{key=1, val = <<"abc++">>}]} = mets:lookup(pkv, 1),
%    {ok, [#kv{key=2, val = <<"abc++">>}]} = mets:lookup(pkv, 2),
%    {ok, [#kv{key=3, val = <<"abc++">>}]} = mets:lookup(pkv, 3),
%    {ok, [#kv{key=4, val = <<"abc++">>}]} = mets:lookup(pkv, 4),
%    {ok, [#kv{key=5, val = <<"abc++">>}]} = mets:lookup(pkv, 5),
%    {ok, [#kv{key=6, val = <<"abc++">>}]} = mets:lookup(pkv, 6).

% delete_pkv() ->
%    ok = mets:delete(pkv, 1),
%    ok = mets:delete(pkv, 2),

%    ok = '<<'(mets:delete_(pkv, 3)),
%    ok = '<<'(mets:delete_(pkv, 4)),

%    ok = mets:delete_(pkv, 5, false),
%    ok = mets:delete_(pkv, 6, false),

%    {ok, []} = mets:lookup(pkv, 1),
%    {ok, []} = mets:lookup(pkv, 2),
%    {ok, []} = mets:lookup(pkv, 3),
%    {ok, []} = mets:lookup(pkv, 4),
%    {ok, []} = mets:lookup(pkv, 5),
%    {ok, []} = mets:lookup(pkv, 6).






% -define(HOST,   "192.168.10.10").
% -define(USER,   "admin").
% -define(PASS,   "admin").
% -define(MYSQL, {?HOST, 3306, ?USER, ?PASS}).

% -define(NODE, [{host, ?HOST}, {user, ?USER}, {pass, ?PASS}]).
% -define(KEYVAL, "
%    CREATE TABLE IF NOT EXISTS ~s.keyval(
%       uid  int NOT NULL,
%       val  varbinary(20),
%       PRIMARY KEY(uid)
%    ) ROW_FORMAT=DYNAMIC, ENGINE=MYISAM
% ").

% -define(NKEYVAL, "
%    CREATE TABLE IF NOT EXISTS ~s.nkeyval(
%       uid1  int NOT NULL,
%       uid2  int NOT NULL,
%       uid3  int NOT NULL,
%       val  varbinary(20),
%       PRIMARY KEY(uid1, uid2, uid3)
%    ) ROW_FORMAT=DYNAMIC, ENGINE=MYISAM
% ").

% -define(TYPES, "
%    CREATE TABLE IF NOT EXISTS ~s.types(
%       uid        int NOT NULL,
%       \\`bit\\`  bit(8),
%       \\`bool\\` boolean,
%       u8         tinyint unsigned,
%       u16        smallint unsigned,
%       u24        mediumint unsigned,
%       u32        int unsigned,
%       u64        bigint unsigned,
%       flt        float,
%       dbl        double,
%       chr        char(30),
%       vchr      varchar(30),
%       bin        binary(22),
%       vbin       varbinary(30),   
%       blb        blob,

%       PRIMARY KEY(uid)
%    ) ROW_FORMAT=DYNAMIC, ENGINE=MYISAM
% ").


% %%%------------------------------------------------------------------
% %%%
% %%% setup
% %%%
% %%%------------------------------------------------------------------

% mets_test_() ->
%    {
%       setup,
%       fun init/0,
%       fun stop/1,
%       [
%           {"CRUD: sync, key=1",  fun crud_sync_key_1/0}
%          ,{"CRUD: async, key=1",  fun crud_async_key_1/0}
%          ,{"CRUD: tuple uid, key=1",  fun crud_tuid_key_1/0} 
%          ,{"CRUD: sync, key=n",  fun crud_sync_key_n/0} 

%          ,{"TYPE: bit",   fun type_bit/0} 
%          ,{"TYPE: bool",  fun type_bool/0}  
%          ,{"TYPE: u8", fun type_u8/0}
%          ,{"TYPE: u16", fun type_u16/0}
%          ,{"TYPE: u24", fun type_u24/0}
%          ,{"TYPE: u32", fun type_u32/0}
%          ,{"TYPE: u64", fun type_u64/0}
%          ,{"TYPE: float", fun type_float/0}
%          ,{"TYPE: double", fun type_double/0}
%          ,{"TYPE: char", fun type_char/0}              
%          ,{"TYPE: varchar", fun type_vchar/0}
%          ,{"TYPE: binary", fun type_bin/0}
%          ,{"TYPE: varbinary", fun type_vbin/0}
%          ,{"TYPE: blob", fun type_blob/0}

%          ,{"FIND: find by pkey", fun find_pkey/0}
%          ,{"FIND: find query", fun find_query/0} 

%          ,{"E: elements",  fun e_elements/0}
%          ,{"E: range",  fun e_range/0}
%          ,{"E: fold",   fun e_fold/0}
%          ,{"E: transform",  fun e_transform/0} 

%          ,{"CRUD: ring, key=1",  fun crud_ring_key_1/0}
%       ]
%    }.

% %%
% %%
% init() ->
%    mets:start(),
%    mets:attach({node, mdb00}, [{addr,  <<0,0,0>>}|?NODE]),
%    mets:attach({node, mdb01}, [{addr,  <<7,0,0>>}|?NODE]),
%    mets:attach({node, mdb02}, [{addr, <<15,0,0>>}|?NODE]),
%    mets:attach({ring, mring}, []),
%    % create nodes
%    ok = mets:sql({node, mdb00}, "CREATE DATABASE IF NOT EXISTS mdb00"),
%    ok = mets:sql({node, mdb01}, "CREATE DATABASE IF NOT EXISTS mdb01"),
%    ok = mets:sql({node, mdb02}, "CREATE DATABASE IF NOT EXISTS mdb02"),
%    % create buckets
%    ok = mets:sql({node, mdb00}, io_lib:format(?NKEYVAL, [mdb00])),
%    ok = mets:sql({node, mdb00}, io_lib:format(?TYPES, [mdb00])),
%    ok = mets:sql({node, mdb00}, io_lib:format(?KEYVAL, [mdb00])),
%    ok = mets:sql({node, mdb01}, io_lib:format(?KEYVAL, [mdb01])),
%    ok = mets:sql({node, mdb02}, io_lib:format(?KEYVAL, [mdb02])),
%    % 
%    mets:join({ring, mring}, {node, mdb00}),
%    mets:join({ring, mring}, {node, mdb01}),
%    mets:join({ring, mring}, {node, mdb02}),
%    mets:mount({ring, mring}).


% %%
% %%
% stop(_) ->
%    mets:umount({ring, mring}),
%    ok = mets:sql({node, mdb00}, "DROP DATABASE mdb00"),
%    ok = mets:sql({node, mdb01}, "DROP DATABASE mdb01"),
%    ok = mets:sql({node, mdb02}, "DROP DATABASE mdb02"),
%    ok.

% %%
% %%
% crud_sync_key_1() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    ok = mets:create(keyval, {1, <<"abc">>}),
%    {error, duplicate} = mets:create(keyval, {1, <<"abc">>}),
%    {ok, {1, <<"abc">>}} = mets:read(keyval, 1),
%    {ok, {1, <<"abc">>}} = mets:read(keyval, {1, 2, 3}),

%    {ok, 1} = mets:update(keyval, {1, <<"abc++">>}),
%    {ok, {1, <<"abc++">>}} = mets:read(keyval, 1),

%    {ok, 1} = mets:delete(keyval, 1),
%    {error, not_found} = mets:read(keyval, 1),

%    mets:close({node, mdb00}, keyval).

% %%
% %%
% crud_async_key_1() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    ok = mets:create(keyval, {1, <<"abc">>}, [async]),
%    ok = mets:create(keyval, {1, <<"abc">>}, [async]),
%    timer:sleep(100),
%    {ok, {1, <<"abc">>}} = mets:read(keyval, 1),
%    {ok, {1, <<"abc">>}} = mets:read(keyval, {1, 2, 3}),

%    ok = mets:update(keyval, {1, <<"abc++">>}, [async]),
%    timer:sleep(100),
%    {ok, {1, <<"abc++">>}} = mets:read(keyval, 1),

%    ok = mets:delete(keyval, 1, [async]),
%    timer:sleep(100),
%    {error, not_found} = mets:read(keyval, 1),

%    mets:close({node, mdb00}, keyval).

% %%
% %%
% crud_tuid_key_1() ->
%    {ok, _} = mets:open({node, mdb00}, {keyval, 1}, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    ok = mets:create({keyval, 1}, {1, <<"abc">>}),
%    {error, duplicate} = mets:create({keyval, 1}, {1, <<"abc">>}),
%    {ok, {1, <<"abc">>}} = mets:read({keyval, 1}, 1),
%    {ok, {1, <<"abc">>}} = mets:read({keyval, 1}, {1, 2, 3}),

%    {ok, 1} = mets:update({keyval, 1}, {1, <<"abc++">>}),
%    {ok, {1, <<"abc++">>}} = mets:read({keyval, 1}, 1),

%    {ok, 1} = mets:delete({keyval, 1}, 1),
%    {error, not_found} = mets:read({keyval, 1}, 1),

%    mets:close({node, mdb00}, {keyval, 1}).

% %%
% %%
% crud_sync_key_n() ->
%    {ok, _} = mets:open({node, mdb00}, nkeyval, [
%       {db, [nkeyval, 'PRIMARY']},
%       {schema, [uid1, uid2, uid3, val]},
%       {keylen, 3}
%    ]),
%    ok = mets:create(nkeyval, {1, 2, 3, <<"abc">>}),
%    {error, duplicate} = mets:create(nkeyval, {1, 2, 3, <<"abc">>}),
%    {ok, {1, 2, 3, <<"abc">>}} = mets:read(nkeyval, {1, 2, 3, 4}),
%    {ok, {1, 2, 3, <<"abc">>}} = mets:read(nkeyval, {1, 2, 3}),
%    {ok, {1, 2, 3, <<"abc">>}} = mets:read(nkeyval, {1, 2}),
%    {ok, {1, 2, 3, <<"abc">>}} = mets:read(nkeyval, 1),

%    {ok, 1} = mets:update(nkeyval, {1, 2, 3, <<"abc++">>}),
%    {ok, {1, 2, 3, <<"abc++">>}} = mets:read(nkeyval, {1, 2, 3}),

%    {ok, 1} = mets:delete(nkeyval, {1, 2, 3}),
%    {error, not_found} = mets:read(nkeyval, {1, 2, 3}),

%    mets:close({node, mdb00}, nkeyval).

% %%
% %%
% ttype(Type, Val) ->
%    {ok, _} = mets:open({node, mdb00}, Type, [
%       {db, [types, 'PRIMARY']},
%       {schema, [uid, Type]}
%    ]),
%    ok = mets:create(Type, {1, Val}),
%    {ok, {1, Val}} = mets:read(Type, 1),
%    {ok, 1} = mets:delete(Type, 1),
%    mets:close({node, mdb00}, Type). 

% type_bit()  -> ttype(bit,  <<16#20>>).
% type_bool() -> ttype(bool, 1).
% type_u8() -> ttype(u8, 16#ff).
% type_u16() -> ttype(u16, 16#ffff).
% type_u24() -> ttype(u24, 16#ffffff).
% type_u32() -> ttype(u32, 16#ffffffff).
% type_u64() -> ttype(u64, 16#ffffffffffffffff).
% type_float() -> ttype(flt, 123.45).
% type_double() -> ttype(dbl, 123.45).
% type_char() -> ttype(chr, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
% type_vchar() -> ttype(vchr, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
% type_bin() -> ttype(bin, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
% type_vbin() -> ttype(vbin, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
% type_blob() -> ttype(blb, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).


% %%
% %%
% find_pkey() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    lists:foreach(
%       fun(X) -> ok = mets:create(keyval, {X, <<"abc">>}) end,
%       lists:seq(1,10)
%    ),

%    {ok, [{5, <<"abc">>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:eq(key, 5)]),

%    {ok, [{6, <<"abc">>}, {7, <<"abc">>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:gt(key, 5)]),

%    {ok, [{4, <<"abc">>}, {3, <<"abc">>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:lt(key, 5)]),

%    {ok, [{5, <<"abc">>}, {6, <<"abc">>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:ge(key, 5)]),

%    {ok, [{5, <<"abc">>}, {4, <<"abc">>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:le(key, 5)]),

%    lists:foreach(
%       fun(X) -> {ok, 1} = mets:delete(keyval, X) end,
%       lists:seq(1,10)
%    ),
%    mets:close({node, mdb00}, keyval).

% %%
% %%
% find_query() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    lists:foreach(
%       fun(X) -> ok = mets:create(keyval, {X, <<($a + X)>>}) end,
%       lists:seq(0,9)
%    ),

%    {ok, [{5, <<$f>>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:eq(key, 5), mets:eq(val, <<$f>>)]),

%    {ok, [{6, <<$g>>}, {7, <<$h>>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:gt(key, 5), mets:gt(val, <<$f>>)]),

%    {ok, [{4, <<$e>>}, {3, <<$d>>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:lt(key, 5), mets:lt(val, <<$f>>)]),

%    {ok, [{5, <<$f>>}, {6, <<$g>>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:ge(key, 5), mets:ge(val, <<$f>>)]),

%    {ok, [{5, <<$f>>}, {4, <<$e>>}]} = 
%    mets:find(keyval, [mets:lm(0,2), mets:le(key, 5), mets:le(val, <<$f>>)]),

%    lists:foreach(
%       fun(X) -> {ok, 1} = mets:delete(keyval, X) end,
%       lists:seq(0,9)
%    ),
%    mets:close({node, mdb00}, keyval).

% %%
% %%
% e_elements() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    lists:foreach(
%       fun(X) -> ok = mets:create(keyval, {X, <<"abc">>}) end,
%       lists:seq(1,9)
%    ),

%    I = lists:foldl(
%       fun(X, Acc0) -> 
%          {{X, _}, Acc} = mets:next(Acc0),
%          Acc
%       end,
%       mets:elements(keyval),
%       lists:seq(1, 9)
%    ),
%    none = mets:next(I),

%    lists:foreach(
%       fun(X) -> {ok, 1} = mets:delete(keyval, X) end,
%       lists:seq(1,9)
%    ),
%    mets:close({node, mdb00}, keyval).


% %%
% %%
% e_range() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    lists:foreach(
%       fun(X) -> ok = mets:create(keyval, {X, <<"abc">>}) end,
%       lists:seq(1,9)
%    ),

%    I = lists:foldl(
%       fun(X, Acc0) -> 
%          {{X, _}, Acc} = mets:next(Acc0),
%          Acc
%       end,
%       mets:elements(keyval, 3, 7),
%       lists:seq(4, 7)
%    ),
%    none = mets:next(I),

%    lists:foreach(
%       fun(X) -> {ok, 1} = mets:delete(keyval, X) end,
%       lists:seq(1,9)
%    ),
%    mets:close({node, mdb00}, keyval).


% %%
% %%
% e_fold() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    lists:foreach(
%       fun(X) -> ok = mets:create(keyval, {X, <<"abc">>}) end,
%       lists:seq(1,9)
%    ),

%    [7, 6, 5, 4] = mets:fold(
%       keyval, {3, 7}, [],
%       fun({X, _}, Acc) -> [X | Acc] end
%    ),

%    lists:foreach(
%       fun(X) -> {ok, 1} = mets:delete(keyval, X) end,
%       lists:seq(1,9)
%    ),
%    mets:close({node, mdb00}, keyval).


% %%
% %%
% e_transform() ->
%    {ok, _} = mets:open({node, mdb00}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    lists:foreach(
%       fun(X) -> ok = mets:create(keyval, {X, <<"abc">>}) end,
%       lists:seq(1,9)
%    ),
   
%    ok = mets:transform(keyval, undefined, 
%       fun
%       ({X, _}) when X < 2 -> drop;
%       ({X, _}) when X > 7 -> next;
%       ({X, _}) -> {X, <<"def">>}
%       end
%    ),
   
%    {error, not_found}   = mets:read(keyval, 1),
%    {ok, {5, <<"def">>}} = mets:read(keyval, 5),
%    {ok, {8, <<"abc">>}} = mets:read(keyval, 8),

%    lists:foreach(
%       fun(X) -> {ok, 1} = mets:delete(keyval, X) end,
%       lists:seq(2,9)
%    ),
%    mets:close({node, mdb00}, keyval).


% %%
% %%
% crud_ring_key_1() ->
%    {ok, _} = mets:open({ring, mring}, keyval, [
%       {db, [keyval, 'PRIMARY']},
%       {schema, [uid, val]}
%    ]),
%    %lager:set_loglevel(lager_console_backend, debug),

%    ok = mets:create(keyval, {1, <<"abc">>}),
%    ok = mets:create(keyval, {2, <<"abc">>}),
%    ok = mets:create(keyval, {3, <<"abc">>}),

%    {error, duplicate} = mets:create(keyval, {1, <<"abc">>}),

%    {ok, {1, <<"abc">>}} = mets:read(keyval, 1),
%    %{error,   not_found} = mets:read(keyval, 1),

%    {ok, {2, <<"abc">>}} = mets:read(keyval, 2),
%    %{error,   not_found} = mets:read(keyval, 2),

%    {ok, {3, <<"abc">>}} = mets:read(keyval, 3),
%    %{error,   not_found} = mets:read(keyval, 3),

%    %{ok, {1, <<"abc">>}} = mets:read(keyval, {1, 2, 3}),

%    {ok, 1} = mets:update(keyval, {1, <<"abc++">>}),
%    {ok, {1, <<"abc++">>}} = mets:read(keyval, 1),

%    {ok, 1} = mets:delete(keyval, 1),
%    {error, not_found} = mets:read(keyval, 1),

%    mets:close({ring, mring}, keyval).



