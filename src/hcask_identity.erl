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
%%   cargo identity server maps cask physical attributes to unique sequential number
-module(hcask_identity).
-behaviour(kfsm).

-export([
	start_link/0,
	init/1,
	free/2,
	ioctl/2,
	handle/3,
	%% api
	lookup/1
]).

%% internal server state
-record(srv, {
	seq   = 1         :: integer(),
	table = undefined :: integer() 
}).


%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link() ->
	kfsm:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
	{ok, handle, 
		#srv{
			%% @todo: use other data type (e.g. dict, tree)
			table = ets:new(undefined, [protected, set]) 
		}
	}.

free(_, _) ->
	ok.

ioctl(_, _) ->
	throw(not_supported).

%%%------------------------------------------------------------------
%%%
%%% api
%%%
%%%------------------------------------------------------------------   

lookup(Cask) ->
	plib:call(?MODULE, {lookup, Cask}).


%%%------------------------------------------------------------------
%%%
%%% FSM
%%%
%%%------------------------------------------------------------------   

handle({lookup, Cask}, Tx, S) ->
	case ets:lookup(S#srv.table, Cask) of
		[] ->
			true = ets:insert_new(S#srv.table, {Cask, S#srv.seq}),
			plib:ack(Tx, {ok, S#srv.seq}),
			{next_state, handle, S#srv{seq = S#srv.seq + 1}};
		[{_, Uid}] ->
			plib:ack(Tx, {ok, Uid}),
			{next_state, handle, S}
	end;

handle(_, _Tx, S) ->
	{next_state, handle, S}.



