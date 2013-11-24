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
%%   application root supervisor
-module(hcask_peer_root_sup).
-behaviour(supervisor).

-export([
   start_link/0, 
   init/1,
   % peer api
   join/2,
   leave/1,
   peers/0
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 5000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 5000, Type, dynamic}).

%%
%%
start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).
   
init([]) ->   
   {ok,
      {
         {one_for_one, 2, 1800},
         []
      }
   }.

%%
%% join peer to pool
join(Peer, Opts) ->
	supervisor:start_child(?MODULE, ?CHILD(supervisor, Peer, hcask_peer_sup, [Peer, Opts])).

%%
%% leave peer from pool
leave(Peer) ->
	supervisor:terminate_child(?MODULE, Peer),
	supervisor:delete_child(?MODULE, Peer).

%%
%% list known peers
peers() ->
	[erlang:element(1, X) || X <- supervisor:which_children(?MODULE)].

