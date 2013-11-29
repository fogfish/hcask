
%%
%% cask definition
-record(hcask, {
	name     = undefined  :: any(),     % cask name
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
