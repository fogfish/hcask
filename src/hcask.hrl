%%%------------------------------------------------------------------
%%%
%%% build time config
%%%
%%%------------------------------------------------------------------   

%% enable debug output
-define(CONFIG_DEBUG,     true).

%% default i/o protocol family
-define(CONFIG_IO_FAMILY, hcask_io_hs).

%% default cask index
-define(CONFIG_DEFAULT_INDEX,  'PRIMARY').

%% default timeout
-define(CONFIG_TIMEOUT_LEASE,       5000). 
-define(CONFIG_TIMEOUT_IO,          5000).
-define(CONFIG_TIMEOUT_SPIN,          10).

%%%------------------------------------------------------------------
%%%
%%% macro
%%%
%%%------------------------------------------------------------------   

%%
%% logger macro
-ifndef(ERROR).
-define(ERROR(Fmt, Args), lager:error(Fmt, Args)).
-endif.

-ifndef(DEBUG).
   -ifdef(CONFIG_DEBUG).
		-define(DEBUG(Msg),       lager:debug(Msg)).
		-define(DEBUG(Fmt, Args), lager:debug(Fmt, Args)).
	-else.
		-define(DEBUG(Msg),       ok).
		-define(DEBUG(Fmt, Args), ok).
	-endif.
-endif.

%%
%% tcp socket options
-define(SO_TCP, [
   binary,
	{packet, line},
	{recbuf, 128 * 1024},
	{sndbuf,  32 * 1024},
   {keepalive,    true},
   {active,       once}
]).




