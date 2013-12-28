%%
%%   Copyright (c) 2012 Dmitry Kolesnikov
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
%% @description
-module(hcask).

-include("hcask.hrl").
-include("include/hcask.hrl").

-export([start/0, start/1]).
-export([
   % peer interface
   join/2
  ,leave/1
  ,peers/0
  	% i/o interface
  ,new/1
  ,uid/1
  ,create/2
  ,update/2
  ,delete/2
  ,lookup/2
  % query
  ,q/1
  ,q/2
  ,q/3
  ,eq/2
  ,gt/2
  ,lt/2
  ,ge/2
  ,le/2
]).


%%
%% start application
start()    -> 
   applib:boot(?MODULE, []).

start(Cfg) -> 
	applib:boot(?MODULE, Cfg).


%%%------------------------------------------------------------------
%%%
%%% peer interface
%%%
%%%------------------------------------------------------------------   

%%
%% join storage peer
%%  Options:
%%    {host,  hostname()} - peer hostname or ip address
%%    {reader, integer()} - peer reader i/o port (read-only request)
%%    {writer, integer()} - peer writer i/o port (mixed request)
%%    {pool,   integer()} - capacity of i/o queues
-spec(join/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

join(Peer, Opts) ->
   hcask_peer_root_sup:join(Peer, Opts).

%%
%% leave storage peer
-spec(leave/1 :: (atom()) -> ok).

leave(Peer) ->
   hcask_peer_root_sup:leave(Peer).

%%
%% list of peers
-spec(peers/0 :: () -> [atom()]).

peers() ->
   hcask_peer_root_sup:peers().


%%%------------------------------------------------------------------
%%%
%%% i/o interface
%%%
%%%------------------------------------------------------------------   

%%
%% create cask
%%  Options:
%%     {peer,       atom()} - peer name
%%     {struct,     atom()} - struct identity
%%     {keylen,  integer()} - length of key   (default 1)
%%     {property, [atom()]} - list of properties
%%     {db,         atom()} - storage database   
%%     {bucket,     atom()} - storage bucket  (default struct)
%%     {index,      atom()} - storage index   (default 'PRIMARY')
-spec(new/1 :: (list()) -> #hcask{}).

new(Opts) ->
	new(Opts, #hcask{}).

new([{peer, X} | Opts], S) ->
   new(Opts, S#hcask{peer=X});
new([{struct, X} | Opts], S) ->
   new(Opts, S#hcask{struct=X});
new([{keylen, X} | Opts], S) ->
   new(Opts, S#hcask{keylen=X});
new([{property, X} | Opts], S) ->
   new(Opts, S#hcask{property=X});
new([{db, X} | Opts], S) ->
   new(Opts, S#hcask{db=X});
new([{bucket, X} | Opts], S) ->
   new(Opts, S#hcask{bucket=X});
new([{index, X} | Opts], S) ->
   new(Opts, S#hcask{index=X});
new([{identity, X} | Opts], S) ->
   new(Opts, S#hcask{identity=X});
new([{entity, X} | Opts], S) ->
   new(Opts, S#hcask{entity=X});
new([_ | Opts], S) ->
   new(Opts, S);
new([], #hcask{peer=undefined}) ->
   exit({bagarg, peer});
new([], #hcask{struct=undefined}) ->
   exit({bagarg, struct});
new([], #hcask{property=undefined}) ->
   exit({bagarg, property});
new([], #hcask{db=undefined}) ->
   exit({bagarg, db});
new([], #hcask{bucket=undefined}=S) ->
   new([], S#hcask{bucket=S#hcask.struct});
new([], #hcask{index=undefined}=S) ->
   new([], S#hcask{index=?CONFIG_DEFAULT_INDEX});
new([], #hcask{uid=undefined, identity=undefined}=S) ->
	new([], uid(S));
new([], Cask) ->
	Cask.

%%
%% lookup unique cask identity
-spec(uid/1 :: (#hcask{}) -> #hcask{}).

uid(#hcask{uid=undefined}=Cask) ->
	Hash = erlang:phash2([
		Cask#hcask.db
	  ,Cask#hcask.bucket
	  ,Cask#hcask.index
	  ,Cask#hcask.property
	]),
	{ok, Uid} = hcask_identity:lookup(Hash),
   Cask#hcask{
   	uid = Uid
   };
uid(Cask) ->
	Cask.

%%
%% create entity to cask
-spec(create/2 :: (#hcask{}, tuple()) -> {ok, integer()} | {error, any()}).

create(Cask, Entity) ->
	case hcask_io:do({create, Entity}, hcask_io:init(?CONFIG_IO_FAMILY, Cask)) of
		{ok, Msg, IO} ->
			ok = hcask_io:free(IO),
			{ok, Msg};
		{error, Reason, IO} ->
			ok = hcask_io:free(IO),
			{error, Reason}
	end.

%%
%% update entity in cask
-spec(update/2 :: (#hcask{}, tuple()) -> {ok, integer()} | {error, any()}).

update(Cask, Entity) ->
	case hcask_io:do({update, Entity}, hcask_io:init(?CONFIG_IO_FAMILY, Cask)) of
      {ok, Msg, IO} ->
         ok = hcask_io:free(IO),
         {ok, Msg};
      {error, Reason, IO} ->
         ok = hcask_io:free(IO),
         {error, Reason}
	end.

%%
%% delete entity from cask
-spec(delete/2 :: (#hcask{}, any()) -> {ok, integer()} | {error, any()}).

delete(Cask, Key) ->
	case hcask_io:do({delete, Key}, hcask_io:init(?CONFIG_IO_FAMILY, Cask)) of
		{ok, Msg, IO} ->
			ok = hcask_io:free(IO),
			{ok, Msg};
		{error, Reason, IO} ->
			ok = hcask_io:free(IO),
			{error, Reason}
	end.

%%
%% lookup entity in cask
-spec(lookup/2 :: (#hcask{}, any()) -> {ok, [tuple()]} | {error, any()}).

lookup(Cask, {lookup, _Key, _Filters, _N}=Req) ->
	case hcask_io:do(Req, hcask_io:init(?CONFIG_IO_FAMILY, Cask)) of
		{ok, Msg, IO} ->
			ok = hcask_io:free(IO),
			{ok, Msg};
		{error, Reason, IO} ->
			ok = hcask_io:free(IO),
			{error, Reason}
	end;
lookup(Cask, Key) ->
   lookup(Cask, q(hcask:eq(key, Key), [], 1)).

%%
%% lookup query
q(Key) ->
   q(Key, {0, 25}).
q(Key, N)
 when is_integer(N) ->
   q(Key, [], {0, N});
q(Key, {M, N})
 when is_integer(N) ->
   q(Key, [], {M, N});
q(Key, Filters)
 when is_list(Filters) ->
   q(Key, Filters, {0, 25}).
q(Key, Filters, N)
 when is_list(Filters), is_integer(N) ->
 	{lookup, Key, Filters, {0, N}};
q(Key, Filters, {M, N})
 when is_list(Filters), is_integer(N) ->
 	{lookup, Key, Filters, {M, N}}.

%%
%% find query utility functions 
eq(Key, Val)  -> constrain(<<$=>>, Key, Val).
gt(Key, Val)  -> constrain(<<$>>>, Key, Val).   
lt(Key, Val)  -> constrain(<<$<>>, Key, Val).   
ge(Key, Val)  -> constrain(<<$>, $=>>, Key, Val).   
le(Key, Val)  -> constrain(<<$<, $=>>, Key, Val).   

constrain(Op, Key, Val) ->
   {Key, Op, Val}.


