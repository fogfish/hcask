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


%%%------------------------------------------------------------------
%%%
%%% records
%%%
%%%------------------------------------------------------------------   

%%
%% cask definition
-record(hcask, {
	uid      = undefined  :: integer(), % cask unique identity
	% data type
	struct   = undefined  :: atom(),    % struct identity
	keylen   = 1          :: integer(), % length of key properties 
	property = undefined  :: [atom()],  % list of properties
	% storage
	peer     = undefined  :: atom(),    % peer associated with cask
	db       = undefined  :: atom(),    % storage domain
	bucket   = undefined  :: atom(),    % storage bucket
	index    = undefined  :: atom()     % storage index    
	% physical bucket
  ,identity = undefined  :: function() % bucket identity function
  ,entity   = undefined  :: function() % bucket entity map function
}).

%%
%% i/o context
-record(hio, {
	protocol = undefined  :: atom(),    % i/o protocol
	reader   = undefined  :: pid(),     % reader pool / leased socket
	writer   = undefined  :: pid(),     % writer pool / leased socket
	cask     = []         :: [#hcask{}] % list of cask bound to transaction 
}).


