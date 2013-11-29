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
%%   i/o protocol handler - handler socket
-module(hcask_io_hs).
-behaviour(pipe).

-include("hcask.hrl").
-include("include/hcask.hrl").

-export([
	start_link/3,
	init/1,
	free/2,
	ioctl/2,
	% FSM
	'IDLE'/3,  % i/o object is idle   
	'WIRE'/3,  % i/o object is leased
	% message encode / decode
	encode/1,
	encode/2,
	decode/2
]).

%% internal state
-record(fsm, {
	queue   = undefined :: pid(),    %% queue leader i/o object belongs to
	spinner = undefined :: any(),    %% spinner timeout
	sock    = undefined :: port(),   %% i/o socket
	in      = undefined :: datum:q(),%% input buffer
	q       = undefined :: datum:q() %% on-going tx
}).

%% strip last \n
-define(MSG(X), binary:part(X, 0, byte_size(X) - 1)).

%%%------------------------------------------------------------------
%%%
%%% Factory
%%%
%%%------------------------------------------------------------------   

start_link(Queue, Host, Port) ->
	pipe:start_link(?MODULE, [Queue, Host, Port], []).

init([Queue, Host, Port]) ->
   {ok, Sock} = gen_tcp:connect(Host, Port, ?SO_TCP),
   ?DEBUG("hcask hs: connected ~p ~p", [self(), {Host, Port}]),
	{ok, 'IDLE', 
		#fsm{
			queue   = Queue,
			spinner = opts:val(spin, ?CONFIG_TIMEOUT_SPIN, cargo),
			sock    = Sock,
			in      = deq:new(),
			q       = deq:new()
		}
	}.

free(_, S) ->
   gen_tcp:close(S#fsm.sock).

ioctl(_, _) ->
	throw(not_supported).

%%%------------------------------------------------------------------
%%%
%%% IDLE
%%%
%%%------------------------------------------------------------------   

'IDLE'(free, _, S) ->
	% release i/o object to queue
	{next_state, 'IDLE', S};

'IDLE'({tcp, _, Msg}, _, S) ->
	{next_state, 'IDLE', recv_packet(Msg, S)};
'IDLE'({tcp_closed, _}, _, S) ->
   {stop, normal, S};
'IDLE'({tcp_error, _, Reason}, _, S) ->
   {stop, Reason, S};

'IDLE'(Msg, Tx, S)
 when is_binary(Msg) ->
   gen_tcp:send(S#fsm.sock, [Msg, $\n]),
	{next_state, 'WIRE', 
		S#fsm{
			spinner = tempus:reset(S#fsm.spinner, release),
			q       = q:enq(Tx, S#fsm.q)
		}
	}.

%%%------------------------------------------------------------------
%%%
%%% WIRE
%%%
%%%------------------------------------------------------------------   

'WIRE'(free, _Tx, S) ->
	?DEBUG("cargo hs: ~p released", [self()]),
	pq:release(S#fsm.queue, self()),
	{next_state, 'IDLE', 
		S#fsm{
			spinner = tempus:cancel(S#fsm.spinner)
		}
	};

'WIRE'({tcp, _, Msg}, _, S) ->
	{next_state, 'WIRE', recv_packet(Msg, S)};
'WIRE'({tcp_closed, _}, _, S) ->
   {stop, normal, S};
'WIRE'({tcp_error, _, Reason}, _, S) ->
   {stop, Reason, S};

'WIRE'(Msg, Tx, S)
 when is_binary(Msg) ->
	gen_tcp:send(S#fsm.sock, [Msg, $\n]),
	{next_state, 'WIRE', 
		S#fsm{
			spinner = tempus:reset(S#fsm.spinner, release),
			q       = q:enq(Tx, S#fsm.q)
		}
	}.


recv_packet(Pckt, S) ->
   ok = inet:setopts(S#fsm.sock, [{active, once}]),
   case binary:last(Pckt) of
      $\n ->
         Msg     = iolist_to_binary(deq:list(deq:enq(Pckt, S#fsm.in))),
         {Tx, Q} = q:deq(S#fsm.q),
         _ = recv_message(Msg, Tx),
         S#fsm{in=deq:new(), q=Q};
      _   ->
         S#fsm{in = deq:enq(Pckt, S#fsm.in)}
   end.

%% operation successfully completed
recv_message(<<$0, $\t, $1, $\n>> = Pckt, Tx) ->
   ?DEBUG("hcask hs ~p: ~p", [self(), Pckt]),
   plib:ack(Tx, {ok, 0});

%% operation successfully completed, numeric status is returned
%% * number of impacted row
%% * auto-increment value
recv_message(<<$0, $\t, $1, $\t, Msg/binary>> = Pckt, Tx) -> 
   ?DEBUG("hcask hs ~p: ~p", [self(), Pckt]),
   plib:ack(Tx, {ok, scalar:i(?MSG(Msg))});

%% query successfully completed, tuple is returned
recv_message(<<$0, $\t, Msg/binary>> = Pckt, Tx) -> 
   ?DEBUG("hcask hs ~p: ~p", [self(), Pckt]),
   case binary:match(Msg, <<$\t>>) of
      nomatch -> 
         plib:ack(Tx, {ok, []});
      _       -> 
      	plib:ack(Tx, {ok, ?MSG(Msg)})
   end;
   
%% socket error: bucket is not linked
recv_message(<<_, $\t, $1, $\t, "stmtnum", $\n>>, Tx) ->
   plib:ack(Tx, {error, nolink});

recv_message(<<_, $\t, $1, $\t, "filterfld", $\n>>, Tx) ->
   plib:ack(Tx, {error, nolink});

%% unknown socket response
recv_message(Msg, Tx) ->
   ?DEBUG("hcask hs ~p: ~p", [self(), Msg]),
   plib:ack(Tx, {error, hs_error(?MSG(Msg))}).

%%
%%
hs_error(<<_, $\t, $1, $\t, "121">>) -> duplicate;
% MySQL error code 126: Index file is crashed
hs_error(<<_, $\t, $1, $\t, "126">>) -> corrupted;
% MySQL error code 127: Record-file is crashed
hs_error(<<_, $\t, $1, $\t, "127">>) -> corrupted;
% MySQL error code 135: No more room in record file
hs_error(<<_, $\t, $1, $\t, "135">>) -> out_of_disk;
% MySQL error code 136: No more room in index file
hs_error(<<_, $\t, $1, $\t, "136">>) -> out_of_disk;
hs_error(<<_, $\t, $1, $\t, "kpnum">>)-> badkey;
hs_error(<<_, $\t, $1, $\t, "fld">>) -> badarg;
hs_error(<<_, $\t, $1, $\t, "open_table">>)  -> no_bucket;
hs_error(<<_, $\t, $1, $\t, Reason/binary>>) -> Reason.

% MySQL error code 128: Out of memory
% MySQL error code 130: Incorrect file format
% MySQL error code 131: Command not supported by database
% MySQL error code 132: Old database file
% MySQL error code 133: No record read before update
% MySQL error code 134: Record was already deleted (or record file crashed)
% MySQL error code 137: No more records (read after end of file)
% MySQL error code 138: Unsupported extension used for table
% MySQL error code 139: Too big row
% MySQL error code 140: Wrong create options
% MySQL error code 141: Duplicate unique key or constraint on write or update
% MySQL error code 142: Unknown character set used
% MySQL error code 143: Conflicting table definitions in sub-tables of MERGE table
% MySQL error code 144: Table is crashed and last repair failed
% MySQL error code 145: Table was marked as crashed and should be repaired
% MySQL error code 146: Lock timed out; Retry transaction
% MySQL error code 147: Lock table is full;  Restart program with a larger locktable
% MySQL error code 148: Updates are not allowed under a read only transactions
% MySQL error code 149: Lock deadlock; Retry transaction
% MySQL error code 150: Foreign key constraint is incorrectly formed
% MySQL error code 151: Cannot add a child row
% MySQL error code 152: Cannot delete a parent row



%%%------------------------------------------------------------------
%%%
%%% i/o handler
%%%
%%%------------------------------------------------------------------   

% io(timeout, _Tx, S) ->
% 	%% @todo make busy (leased) / free (released) state (double release NFG)
% 	?DEBUG("cargo hs: ~p spin expired", [self()]),
% 	pq:release(S#fsm.queue, self()),
% 	{next_state, io, S};

% io(free, _Tx, S) ->
% 	?DEBUG("cargo hs: ~p free", [self()]),
% 	pq:release(S#fsm.queue, self()),
% 	{next_state, io, 
% 		S#fsm{
% 			spinner = tempus:cancel(S#fsm.spinner)
% 		}
% 	};

% io({tcp, _, Msg}, _, S) ->
% 	{Tx, Q} = q:deq(S#fsm.q),
% 	plib:ack(Tx, Msg),
% 	{next_state, io,
% 		S#fsm{
% 			q = Q
% 		}
% 	};

% io(Msg, Tx, S) ->
% 	%% @todo socket i/o
% 	erlang:send_after(1, self(), {tcp, undefined, Msg}),
% 	{next_state, io, 
% 		S#fsm{
% 			spinner = tempus:reset(S#fsm.spinner, timeout),
% 			q       = q:enq(Tx, S#fsm.q)
% 		}
% 	}.

%%%------------------------------------------------------------------
%%%
%%% message encode / decode
%%%
%%%------------------------------------------------------------------   

%%
%% serialize client request to protocol format
%%  * resolves physical bucket
%%  * resolves physical bucket handle (32-bit number)
%%  * splits tuple to key / val parts
%%  * serializes request to write format 
encode(#hcask{}=Cask) ->
   %P <indexid> <dbname> <tablename> <indexname> <columns> [<fcolumns>]
   iolist_to_binary(
      join($\t, [
         $P,
         scalar:s(Cask#hcask.uid),
         join($\t, [scalar:s(Cask#hcask.db), scalar:s(Cask#hcask.bucket), scalar:s(Cask#hcask.index)]),
         join($, , [scalar:s(X) || X <- Cask#hcask.property]),
         join($, , [scalar:s(X) || X <- Cask#hcask.property])
      ])
   ).

encode({create, Key, Val}, Cask) ->
   %<indexid> + <vlen> <v1> ... <vn>
   iolist_to_binary(
      join($\t, [
         scalar:s(Cask#hcask.uid),
         $+,
         scalar:s(length(Key) + length(Val)),
         join($\t , [scalar:s(escape(X)) || X <- Key]),
         join($\t , [scalar:s(escape(X)) || X <- Val])
      ])
   );

encode({update, Key, Val}, Cask) ->
   %<indexid> + <vlen> <v1> ... <vn>
   list_to_binary(
      join($\t, [
         scalar:s(Cask#hcask.uid),
         $=,
         scalar:s(length(Key)),
         join($\t , [scalar:s(escape(X)) || X <- Key]),
         $1, $0,  % hardcoded limit (mandatory for update)
         $U,
         join($\t , [scalar:s(escape(X)) || X <- Key]),
         join($\t , [scalar:s(escape(X)) || X <- Val])
      ])
   );

encode({delete, Key}, Cask) ->
   list_to_binary(
      join($\t, [
         scalar:s(Cask#hcask.uid),
         $=,
         scalar:s(length(Key)),
         join($\t , [scalar:s(escape(X)) || X <- Key]),
         $1, $0,  % hardcoded limit for read
         $D
      ])
   );

encode({lookup, {key, Eq, Key}, Filters, {Ofs, Len}}, Cask) ->
   list_to_binary(
      join($\t, [
         scalar:s(Cask#hcask.uid),
         Eq,
         scalar:s(length(Key)),
         join($\t , [scalar:s(escape(X)) || X <- Key]),
         integer_to_list(Len), 
         integer_to_list(Ofs),
         join($\t, lookup_filter(Filters, Cask))
      ])
   ).


lookup_filter(Filters, Cask) ->
   Attrs = Cask#hcask.property,
   lists:foldl(
      fun({Attr, Op, Val}, Acc) -> 
         case string:str(Attrs, [Attr]) of
         	% attribute is not found
            0 -> 
            	Acc;
            % attribute is found
            I -> 
            	[join($\t, [$F, Op, scalar:s(I - 1), scalar:s(escape(Val))]) | Acc]
         end
      end,
      [],
      Filters 
   ).


decode(Msg, Cask)
 when is_binary(Msg) ->
 	% handle raw packet, first items is tuple size
   [N | List] = binary:split(Msg, [<<"\t">>], [global]),
   decode_tuples(Cask#hcask.struct, scalar:i(N), List);
decode(Msg, _Cask) ->
	Msg.

decode_tuples(_Struct, _N, []) ->
	[];
decode_tuples(Struct, N, List) ->
   {H, T} = lists:split(N, List),
   Value  = lists:map(fun(X) -> scalar:decode(unescape(X)) end, H),
   [list_to_tuple([Struct | Value]) | decode_tuples(Struct, N,  T)].

%%%------------------------------------------------------------------
%%%
%%% private
%%%
%%%------------------------------------------------------------------   


%% join io_list with separator
join(_, []) ->
   [];
join(E, [H|T]) ->
   [H] ++ lists:foldr(fun([], A) -> A; (X, A) -> [E, X | A] end, [], T).

%%
%% escape binary
-define(E(C), 
   <<_:Pos/binary, Tkn:Len/binary, C, _/binary>>  ->
      escape(In, Pos + Len + 1, 0, <<Acc/binary, Tkn/binary, 1, (16#40 bxor C)>>);     
).

escape(In)
 when is_binary(In) ->
   escape(In, 0, 0, <<>>);
escape(null) ->
	<<0>>;
escape(undefined) ->
	<<0>>;
escape(In) ->
	In.

escape(In, Pos, Len, Acc) when Pos + Len < size(In) ->
   case In of
      ?E(16#00) ?E(16#01) ?E(16#02) ?E(16#03) ?E(16#04) 
      ?E(16#05) ?E(16#06) ?E(16#07) ?E(16#08) ?E(16#09) 
      ?E(16#0a) ?E(16#0b) ?E(16#0c) ?E(16#0d) ?E(16#0e) ?E(16#0f)
      _ ->
         escape(In, Pos, Len + 1, Acc)
   end;
escape(In, Pos, Len, Acc) ->
   <<_:Pos/binary, Tkn:Len/binary>> = In,
   <<Acc/binary, Tkn/binary>>.

%%
%% unescape from wire format
-define(U(C), 
   <<_:Pos/binary, Tkn:Len/binary, 1, C, _/binary>>  ->
      unescape(In, Pos + Len + 2, 0, <<Acc/binary, Tkn/binary, (16#40 bxor C)>>);     
).

unescape(In) ->
   unescape(In, 0, 0, <<>>).

unescape(In, Pos, Len, Acc) when Pos + Len < size(In) ->
   case In of
      ?U(16#40) ?U(16#41) ?U(16#42) ?U(16#43) ?U(16#44) 
      ?U(16#45) ?U(16#46) ?U(16#47) ?U(16#48) ?U(16#49) 
      ?U(16#4a) ?U(16#4b) ?U(16#4c) ?U(16#4d) ?U(16#4e) ?U(16#4f)
      _ ->
         unescape(In, Pos, Len + 1, Acc)
   end;
unescape(In, Pos, Len, Acc) ->
   <<_:Pos/binary, Tkn:Len/binary>> = In,
   <<Acc/binary, Tkn/binary>>.



