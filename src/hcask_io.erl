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
%%   transaction i/o context
-module(hcask_io). 

-include("hcask.hrl").
-include("include/hcask.hrl").

-export([
	init/2,
	free/1,
	do/2
]).

%%
%% create new dirty tx handler
-spec(init/2 :: (atom(), #hcask{}) -> #hio{}).

init(Protocol, #hcask{}=Cask) ->
   {ok, Reader} = hcask_peer_sup:reader(Cask#hcask.peer),
   {ok, Writer} = hcask_peer_sup:writer(Cask#hcask.peer),
	#hio{
		protocol  = Protocol,
		reader    = {pool, Reader},
		writer    = {pool, Writer},
		cask      = [Cask]
	}.

%%
%% release dirty tx handler
-spec(free/1 :: (#hio{}) -> ok).

free(#hio{}=IO) ->
	% free reader socket
	case IO#hio.reader of
		{pool, _} -> ok;
		Reader    -> plib:send(Reader, free)
	end,
	% free writer socket
	case IO#hio.writer of
		{pool, _} -> ok;
		Writer    -> plib:send(Writer, free)
	end,
	ok.

%%
%% execute atomic operation, the function wraps 
%% asynchronous i/o communication to sequence of
%% synchronous primitives. Caller process is blocked
%% until operation completed or timeout
-spec(do/2 :: (any(), #hio{}) -> {ok, any(), #hio{}} | {error, any(), #hio{}}).

do(Req, #hio{}=IO) ->
	prepare_request(Req, IO).

prepare_request({create, Entity}, #hio{protocol=Protocol, cask=[Cask|_]}=I0) ->
   {Db, Bucket, Index} = resolve_phy_bucket(Entity, Cask),
   TxCask       = hcask:uid(Cask#hcask{db=Db, bucket=Bucket, index=Index}),
   {Key, Val}   = split_entity(Entity, TxCask),
   {Socket, IO} = lease_socket(writer, I0),
	do_request(Protocol:encode({create, Key, Val}, TxCask), Socket, TxCask, IO);

prepare_request({update, Entity}, #hio{protocol=Protocol, cask=[Cask|_]}=I0) ->
   {Db, Bucket, Index} = resolve_phy_bucket(Entity, Cask),
   TxCask       = hcask:uid(Cask#hcask{db=Db, bucket=Bucket, index=Index}),
   {Key, Val}   = split_entity(Entity, TxCask),
   {Socket, IO} = lease_socket(writer, I0),
	do_request(Protocol:encode({update, Key, Val}, TxCask), Socket, TxCask, IO);

prepare_request({delete, Entity}, #hio{protocol=Protocol, cask=[Cask|_]}=I0) ->
   {Db, Bucket, Index} = resolve_phy_bucket(Entity, Cask),
   TxCask       = hcask:uid(Cask#hcask{db=Db, bucket=Bucket, index=Index}),
   Key          = key_entity(Entity, TxCask),
   {Socket, IO} = lease_socket(writer, I0),
	do_request(Protocol:encode({delete, Key}, TxCask), Socket, TxCask, IO);

prepare_request({lookup, {key, KOp, Entity}, Filters, N}, #hio{protocol=Protocol, cask=[Cask|_]}=I0) ->
   {Db, Bucket, Index} = resolve_phy_bucket(Entity, Cask),
   TxCask       = hcask:uid(Cask#hcask{db=Db, bucket=Bucket, index=Index}),
   Key          = key_entity(Entity, TxCask),
   {Socket, IO} = lease_socket(reader, I0),
	do_request(Protocol:encode({lookup, {key, KOp, Key}, Filters, N}, TxCask), Socket, TxCask, IO).


do_request(Req, Socket, Cask, #hio{protocol=Protocol}=IO) ->
	case plib:call(Socket, Req) of
		{ok, Msg} ->
			{ok, Protocol:decode(Msg, Cask), IO};
		{error, nolink} ->
			case plib:call(Socket, Protocol:encode(Cask), 3600 * 1000) of
         	% socket is bound
         	{ok, _} -> 
         		do_request(Req, Socket, Cask, IO);
         	% unable to bind socket with bucket
         	{error, Reason} -> 
         		{error, Reason, IO}
			end;
		{error, Reason} ->
			{error, Reason, IO}
	end.

% %%
% %% check request type for socket selection
% -spec(request_type/1 :: (any()) -> reader | writer).

% request_type({create, _}) ->
% 	writer;
% request_type({update, _}) ->
% 	writer;
% request_type({delete, _}) ->
% 	writer;
% request_type({lookup, _}) ->
% 	reader. 

%%
%% lease i/o socket
%% @todo handle lease timeout
-spec(lease_socket/2 :: (reader | writer, #hio{}) -> {pid(), #hio{}}).

lease_socket(reader, #hio{reader={pool, Pool}}=IO) ->
	{ok, Socket} = pq:lease(Pool),
	{Socket, IO#hio{reader = Socket}};
lease_socket(writer, #hio{writer={pool, Pool}}=IO) ->
	{ok, Socket} = pq:lease(Pool),
	{Socket, IO#hio{writer = Socket}};
lease_socket(reader, #hio{}=IO) ->
	{IO#hio.reader, IO};
lease_socket(writer, #hio{}=IO) ->
	{IO#hio.writer, IO}.


%%
%% resolve physical bucket
resolve_phy_bucket(Entity,  #hcask{identity=undefined}=Cask) ->
   {Cask#hcask.db, Cask#hcask.bucket, Cask#hcask.index};
resolve_phy_bucket(Entity,  #hcask{identity=Fun}=Cask) ->
   Fun(Entity).

%%
%% split entity to key / value pair and normalize them
split_entity(Entity, #hcask{entity=undefined}=Cask) ->
   lists:split(
   	Cask#hcask.keylen,
      tl(tuple_to_list(Entity))
   );
split_entity(Entity, #hcask{}=Cask) ->
   Fun = Cask#hcask.entity,
   lists:split(
      Cask#hcask.keylen, 
      tl(tuple_to_list(Fun(Entity)))
   ).

%%
%% normalize key
key_entity(Entity, #hcask{entity=undefined})
 when is_list(Entity) ->
   Entity;
key_entity(Entity, #hcask{entity=undefined})
 when is_tuple(Entity) ->
   tuple_to_list(Entity);   
key_entity(Entity, #hcask{entity=undefined}) ->
	[Entity];
key_entity(Entity, Cask) ->
   Fun = Cask#hcask.entity,
   case Fun(Entity) of
   	X when is_list(X)  -> X;
   	X when is_tuple(X) -> tuple_to_list(X);
   	X  -> [X]
   end.
