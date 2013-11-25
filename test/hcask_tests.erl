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

         ,{"create pkv",  fun create_pkv/0}
         ,{"lookup pkv",  fun lookup_pkv/0}
         ,{"update pkv",  fun update_pkv/0}
         ,{"delete pkv",  fun delete_pkv/0}

         ,{"TYPE: bit",    fun type_bit/0} 
         ,{"TYPE: bool",   fun type_bool/0}  
         ,{"TYPE: u8",     fun type_u8/0}
         ,{"TYPE: u16",    fun type_u16/0}
         ,{"TYPE: u24",    fun type_u24/0}
         ,{"TYPE: u32",    fun type_u32/0}
         ,{"TYPE: u64",    fun type_u64/0}
         ,{"TYPE: float",  fun type_float/0}
         ,{"TYPE: double", fun type_double/0}
         ,{"TYPE: char",   fun type_char/0}              
         ,{"TYPE: varchar",fun type_vchar/0}
         ,{"TYPE: binary", fun type_bin/0}
         ,{"TYPE: varbinary", fun type_vbin/0}
         ,{"TYPE: blob",   fun type_blob/0}
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
-define(PKV, [
   {peer,     mysqld}
  ,{struct,   kv}
  ,{property, [key,val]}
  ,{db,       hcask}
  ,{identity, fun pkv_resolver/1}
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
   {hcask, kv0, 'PRIMARY'};
pkv_resolver(Key) 
 when is_integer(Key), Key rem 2 =:= 0 ->
   {hcask, kv0, 'PRIMARY'};
pkv_resolver(_) ->
   {hcask, kv1, 'PRIMARY'}.

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


%%
%%
create_pkv() ->
   Cask = hcask:new(?PKV),
   {ok, _} = hcask:create(Cask, #kv{key=1, val="abc"}),
   {ok, _} = hcask:create(Cask, #kv{key=2, val="abc"}).

lookup_pkv() ->
   Cask = hcask:new(?PKV),
   {ok, [#kv{key=1, val = <<"abc">>}]} = hcask:lookup(Cask, 1),
   {ok, [#kv{key=2, val = <<"abc">>}]} = hcask:lookup(Cask, 2).


update_pkv() ->
   Cask = hcask:new(?PKV),
   {ok, _} = hcask:update(Cask, #kv{key=1, val="abc++"}),
   {ok, _} = hcask:update(Cask, #kv{key=2, val="abc++"}),

   {ok, [#kv{key=1, val = <<"abc++">>}]} = hcask:lookup(Cask, 1),
   {ok, [#kv{key=2, val = <<"abc++">>}]} = hcask:lookup(Cask, 2).

delete_pkv() ->
   Cask = hcask:new(?PKV),
   {ok, _} = hcask:delete(Cask, 1),
   {ok, _} = hcask:delete(Cask, 2),

   {ok, []} = hcask:lookup(Cask, 1),
   {ok, []} = hcask:lookup(Cask, 2).

%%
%%
ttype(Type, Val) ->
   Cask = hcask:new([
      {peer,     mysqld}
     ,{struct,   types}
     ,{property, [uid, Type]}
     ,{db,       hcask}
   ]),
   {ok, _} = hcask:create(Cask, {types, 1, Val}),
   {ok, [{types, 1, Val}]} = hcask:lookup(Cask, 1),
   {ok, _} = hcask:delete(Cask, 1). 

type_bit()  -> ttype(bit,  <<16#20>>).
type_bool() -> ttype(bool, 1).
type_u8()   -> ttype(u8, 16#ff).
type_u16()  -> ttype(u16, 16#ffff).
type_u24()  -> ttype(u24, 16#ffffff).
type_u32()  -> ttype(u32, 16#ffffffff).
type_u64()  -> ttype(u64, 16#ffffffffffffffff).
type_float() -> ttype(flt, 123.45).
type_double()-> ttype(dbl, 123.45).
type_char()  -> ttype(chr, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
type_vchar() -> ttype(vchr, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
type_bin()   -> ttype(bin, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
type_vbin()  -> ttype(vbin, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).
type_blob()  -> ttype(blb, <<$a, $b, $c, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, $d, $e, $f>>).




