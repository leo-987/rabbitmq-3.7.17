%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_queue_index).

-export([erase/1, init/3, reset_state/1, recover/6,
         terminate/3, delete_and_terminate/1,
         pre_publish/7, flush_pre_publish_cache/2,
         publish/6, deliver/2, ack/2, sync/1, needs_sync/1, flush/1,
         read/3, next_segment_boundary/1, bounds/1, start/2, stop/1]).

-export([add_queue_ttl/0, avoid_zeroes/0, store_msg_size/0, store_msg/0]).
-export([scan_queue_segments/3]).

%% Migrates from global to per-vhost message stores
-export([move_to_per_vhost_stores/1,
         update_recovery_term/2,
         read_global_recovery_terms/1,
         cleanup_global_recovery_terms/0]).

-define(CLEAN_FILENAME, "clean.dot").

%%----------------------------------------------------------------------------

%% The queue index is responsible for recording the order of messages
%% within a queue on disk. As such it contains records of messages
%% being published, delivered and acknowledged. The publish record
%% includes the sequence ID, message ID and a small quantity of
%% metadata about the message; the delivery and acknowledgement
%% records just contain the sequence ID. A publish record may also
%% contain the complete message if provided to publish/5; this allows
%% the message store to be avoided altogether for small messages. In
%% either case the publish record is stored in memory in the same
%% serialised format it will take on disk.
%%
%% Because of the fact that the queue can decide at any point to send
%% a queue entry to disk, you can not rely on publishes appearing in
%% order. The only thing you can rely on is a message being published,
%% then delivered, then ack'd.
%%
%% In order to be able to clean up ack'd messages, we write to segment
%% files. These files have a fixed number of entries: ?SEGMENT_ENTRY_COUNT
%% publishes, delivers and acknowledgements. They are numbered, and so
%% it is known that the 0th segment contains messages 0 ->
%% ?SEGMENT_ENTRY_COUNT - 1, the 1st segment contains messages
%% ?SEGMENT_ENTRY_COUNT -> 2*?SEGMENT_ENTRY_COUNT - 1 and so on. As
%% such, in the segment files, we only refer to message sequence ids
%% by the LSBs as SeqId rem ?SEGMENT_ENTRY_COUNT. This gives them a
%% fixed size.
%%
%% However, transient messages which are not sent to disk at any point
%% will cause gaps to appear in segment files. Therefore, we delete a
%% segment file whenever the number of publishes == number of acks
%% (note that although it is not fully enforced, it is assumed that a
%% message will never be ackd before it is delivered, thus this test
%% also implies == number of delivers). In practise, this does not
%% cause disk churn in the pathological case because of the journal
%% and caching (see below).
%%
%% Because of the fact that publishes, delivers and acks can occur all
%% over, we wish to avoid lots of seeking. Therefore we have a fixed
%% sized journal to which all actions are appended. When the number of
%% entries in this journal reaches max_journal_entries, the journal
%% entries are scattered out to their relevant files, and the journal
%% is truncated to zero size. Note that entries in the journal must
%% carry the full sequence id, thus the format of entries in the
%% journal is different to that in the segments.
%%
%% The journal is also kept fully in memory, pre-segmented: the state
%% contains a mapping from segment numbers to state-per-segment (this
%% state is held for all segments which have been "seen": thus a
%% segment which has been read but has no pending entries in the
%% journal is still held in this mapping. Also note that a map is
%% used for this mapping, not an array because with an array, you will
%% always have entries from 0). Actions are stored directly in this
%% state. Thus at the point of flushing the journal, firstly no
%% reading from disk is necessary, but secondly if the known number of
%% acks and publishes in a segment are equal, given the known state of
%% the segment file combined with the journal, no writing needs to be
%% done to the segment file either (in fact it is deleted if it exists
%% at all). This is safe given that the set of acks is a subset of the
%% set of publishes. When it is necessary to sync messages, it is
%% sufficient to fsync on the journal: when entries are distributed
%% from the journal to segment files, those segments appended to are
%% fsync'd prior to the journal being truncated.
%%
%% This module is also responsible for scanning the queue index files
%% and seeding the message store on start up.
%%
%% Note that in general, the representation of a message's state as
%% the tuple: {('no_pub'|{IsPersistent, Bin, MsgBin}),
%% ('del'|'no_del'), ('ack'|'no_ack')} is richer than strictly
%% necessary for most operations. However, for startup, and to ensure
%% the safe and correct combination of journal entries with entries
%% read from the segment on disk, this richer representation vastly
%% simplifies and clarifies the code.
%%
%% For notes on Clean Shutdown and startup, see documentation in
%% rabbit_variable_queue.
%%
%%----------------------------------------------------------------------------

%% ---- Journal details ----

-define(JOURNAL_FILENAME, "journal.jif").
-define(QUEUE_NAME_STUB_FILE, ".queue_name").

-define(PUB_PERSIST_JPREFIX, 2#00).
-define(PUB_TRANS_JPREFIX,   2#01).
-define(DEL_JPREFIX,         2#10).
-define(ACK_JPREFIX,         2#11).
-define(JPREFIX_BITS, 2).
-define(SEQ_BYTES, 8).
-define(SEQ_BITS, ((?SEQ_BYTES * 8) - ?JPREFIX_BITS)).

%% ---- Segment details ----

-define(SEGMENT_EXTENSION, ".idx").

%% TODO: The segment size would be configurable, but deriving all the
%% other values is quite hairy and quite possibly noticeably less
%% efficient, depending on how clever the compiler is when it comes to
%% binary generation/matching with constant vs variable lengths.

-define(REL_SEQ_BITS, 14).
%% calculated as trunc(math:pow(2,?REL_SEQ_BITS))).
-define(SEGMENT_ENTRY_COUNT, 16384).  % 一个 idx 文件中 entry 的最大个数

%% seq only is binary 01 followed by 14 bits of rel seq id
%% (range: 0 - 16383)
-define(REL_SEQ_ONLY_PREFIX, 01).
-define(REL_SEQ_ONLY_PREFIX_BITS, 2).
-define(REL_SEQ_ONLY_RECORD_BYTES, 2).

%% publish record is binary 1 followed by a bit for is_persistent,
%% then 14 bits of rel seq id, 64 bits for message expiry, 32 bits of
%% size and then 128 bits of md5sum msg id.
-define(PUB_PREFIX, 1).
-define(PUB_PREFIX_BITS, 1).

-define(EXPIRY_BYTES, 8).
-define(EXPIRY_BITS, (?EXPIRY_BYTES * 8)).
-define(NO_EXPIRY, 0).

-define(MSG_ID_BYTES, 16). %% md5sum is 128 bit or 16 bytes
-define(MSG_ID_BITS, (?MSG_ID_BYTES * 8)).

%% This is the size of the message body content, for stats
-define(SIZE_BYTES, 4).
-define(SIZE_BITS, (?SIZE_BYTES * 8)).

%% This is the size of the message record embedded in the queue
%% index. If 0, the message can be found in the message store.
-define(EMBEDDED_SIZE_BYTES, 4).
-define(EMBEDDED_SIZE_BITS, (?EMBEDDED_SIZE_BYTES * 8)).

%% 16 bytes for md5sum + 8 for expiry
-define(PUB_RECORD_BODY_BYTES, (?MSG_ID_BYTES + ?EXPIRY_BYTES + ?SIZE_BYTES)).  % 28 字节
%% + 4 for size
-define(PUB_RECORD_SIZE_BYTES, (?PUB_RECORD_BODY_BYTES + ?EMBEDDED_SIZE_BYTES)).

%% + 2 for seq, bits and prefix
-define(PUB_RECORD_PREFIX_BYTES, 2).

%% ---- misc ----

-define(PUB, {_, _, _}). %% {IsPersistent, Bin, MsgBin}

-define(READ_MODE, [binary, raw, read]).
-define(WRITE_MODE, [write | ?READ_MODE]).

%%----------------------------------------------------------------------------

-record(qistate, {
  %% queue directory where segment and journal files are stored
  dir,
  %% map of #segment records
  segments,
  %% journal file handle obtained from/used by file_handle_cache
  journal_handle,
  %% how many not yet flushed entries are there
  dirty_count,
  %% this many not yet flushed journal entries will force a flush
  max_journal_entries,
  %% callback function invoked when a message is "handled"
  %% by the index and potentially can be confirmed to the publisher
  on_sync,
  on_sync_msg,
  %% set of IDs of unconfirmed [to publishers] messages
  unconfirmed,
  unconfirmed_msg,
  %% optimisation
  pre_publish_cache,
  %% optimisation
  delivered_cache,
  %% queue name resource record
  queue_name}).

-record(segment, {
  %% segment ID (an integer)
  num,
  %% segment file path (see also ?SEGMENT_EXTENSION)
  path,
  %% index operation log entries in this segment
  journal_entries,    % journal entry 数组，下标用 RelSeq
  entries_to_segment, % segment entry 数组，下标用 RelSeq
  %% counter of unacknowledged messages
  unacked
}).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-rabbit_upgrade({add_queue_ttl,  local, []}).
-rabbit_upgrade({avoid_zeroes,   local, [add_queue_ttl]}).
-rabbit_upgrade({store_msg_size, local, [avoid_zeroes]}).
-rabbit_upgrade({store_msg,      local, [store_msg_size]}).

-type hdl() :: ('undefined' | any()).
-type segment() :: ('undefined' |
                    #segment { num                :: non_neg_integer(),
                               path               :: file:filename(),
                               journal_entries    :: array:array(),
                               entries_to_segment :: array:array(),
                               unacked            :: non_neg_integer()
                             }).
-type seq_id() :: integer().
-type seg_map() :: {map(), [segment()]}.
-type on_sync_fun() :: fun ((gb_sets:set()) -> ok).
-type qistate() :: #qistate { dir                 :: file:filename(),
                              segments            :: 'undefined' | seg_map(),
                              journal_handle      :: hdl(),
                              dirty_count         :: integer(),
                              max_journal_entries :: non_neg_integer(),
                              on_sync             :: on_sync_fun(),
                              on_sync_msg         :: on_sync_fun(),
                              unconfirmed         :: gb_sets:set(),
                              unconfirmed_msg     :: gb_sets:set(),
                              pre_publish_cache   :: list(),
                              delivered_cache     :: list()
                            }.
-type contains_predicate() :: fun ((rabbit_types:msg_id()) -> boolean()).
-type walker(A) :: fun ((A) -> 'finished' |
                               {rabbit_types:msg_id(), non_neg_integer(), A}).
-type shutdown_terms() :: [term()] | 'non_clean_shutdown'.

-spec erase(rabbit_amqqueue:name()) -> 'ok'.
-spec reset_state(qistate()) -> qistate().
-spec init(rabbit_amqqueue:name(),
                 on_sync_fun(), on_sync_fun()) -> qistate().
-spec recover(rabbit_amqqueue:name(), shutdown_terms(), boolean(),
                    contains_predicate(),
                    on_sync_fun(), on_sync_fun()) ->
                        {'undefined' | non_neg_integer(),
                         'undefined' | non_neg_integer(), qistate()}.
-spec terminate(rabbit_types:vhost(), [any()], qistate()) -> qistate().
-spec delete_and_terminate(qistate()) -> qistate().
-spec publish(rabbit_types:msg_id(), seq_id(),
                    rabbit_types:message_properties(), boolean(),
                    non_neg_integer(), qistate()) -> qistate().
-spec deliver([seq_id()], qistate()) -> qistate().
-spec ack([seq_id()], qistate()) -> qistate().
-spec sync(qistate()) -> qistate().
-spec needs_sync(qistate()) -> 'confirms' | 'other' | 'false'.
-spec flush(qistate()) -> qistate().
-spec read(seq_id(), seq_id(), qistate()) ->
                     {[{rabbit_types:msg_id(), seq_id(),
                        rabbit_types:message_properties(),
                        boolean(), boolean()}], qistate()}.
-spec next_segment_boundary(seq_id()) -> seq_id().
-spec bounds(qistate()) ->
                       {non_neg_integer(), non_neg_integer(), qistate()}.
-spec start(rabbit_types:vhost(), [rabbit_amqqueue:name()]) -> {[[any()]], {walker(A), A}}.

-spec add_queue_ttl() -> 'ok'.


%%----------------------------------------------------------------------------
%% public API
%%----------------------------------------------------------------------------

erase(Name) ->
    #qistate { dir = Dir } = blank_state(Name),
    erase_index_dir(Dir).

%% used during variable queue purge when there are no pending acks
reset_state(#qistate{ queue_name     = Name,
                      dir            = Dir,
                      on_sync        = OnSyncFun,
                      on_sync_msg    = OnSyncMsgFun,
                      journal_handle = JournalHdl }) ->
    ok = case JournalHdl of
             undefined -> ok;
             _         -> file_handle_cache:close(JournalHdl)
         end,
    ok = erase_index_dir(Dir),
    blank_state_name_dir_funs(Name, Dir, OnSyncFun, OnSyncMsgFun).

init(Name, OnSyncFun, OnSyncMsgFun) ->
    State = #qistate { dir = Dir } = blank_state(Name),
    false = rabbit_file:is_file(Dir), %% is_file == is file or dir
    State#qistate{on_sync     = OnSyncFun,
                  on_sync_msg = OnSyncMsgFun}.

recover(Name, Terms, MsgStoreRecovered, ContainsCheckFun,
        OnSyncFun, OnSyncMsgFun) ->
    State = blank_state(Name),
    State1 = State #qistate{on_sync     = OnSyncFun,
                            on_sync_msg = OnSyncMsgFun},
    CleanShutdown = Terms /= non_clean_shutdown,
    case CleanShutdown andalso MsgStoreRecovered of
        true  -> RecoveredCounts = proplists:get_value(segments, Terms, []),
                 init_clean(RecoveredCounts, State1);
        false -> init_dirty(CleanShutdown, ContainsCheckFun, State1)
    end.

terminate(VHost, Terms, State = #qistate { dir = Dir }) ->
    {SegmentCounts, State1} = terminate(State),
    rabbit_recovery_terms:store(VHost, filename:basename(Dir),
                                [{segments, SegmentCounts} | Terms]),
    State1.

delete_and_terminate(State) ->
    {_SegmentCounts, State1 = #qistate { dir = Dir }} = terminate(State),
    ok = rabbit_file:recursive_delete([Dir]),
    State1.

pre_publish(MsgOrId, SeqId, MsgProps, IsPersistent, IsDelivered, JournalSizeHint,
            State = #qistate{pre_publish_cache = PPC,
                             delivered_cache   = DC}) ->
    State1 = maybe_needs_confirming(MsgProps, MsgOrId, State),

    {Bin, MsgBin} = create_pub_record_body(MsgOrId, MsgProps),

    PPC1 =
        [[<<(case IsPersistent of
                true  -> ?PUB_PERSIST_JPREFIX;
                false -> ?PUB_TRANS_JPREFIX
            end):?JPREFIX_BITS,
           SeqId:?SEQ_BITS, Bin/binary,
           (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin], PPC],

    DC1 =
        case IsDelivered of
            true ->
                [SeqId | DC];
            false ->
                DC
        end,

    add_to_journal(SeqId, {IsPersistent, Bin, MsgBin},
                   maybe_flush_pre_publish_cache(
                     JournalSizeHint,
                     State1#qistate{pre_publish_cache = PPC1,
                                    delivered_cache   = DC1})).

%% pre_publish_cache is the entry with most elements when compared to
%% delivered_cache so we only check the former in the guard.
maybe_flush_pre_publish_cache(JournalSizeHint,
                              #qistate{pre_publish_cache = PPC} = State)
  when length(PPC) >= ?SEGMENT_ENTRY_COUNT ->
    flush_pre_publish_cache(JournalSizeHint, State);
maybe_flush_pre_publish_cache(_JournalSizeHint, State) ->
    State.

flush_pre_publish_cache(JournalSizeHint, State) ->
    State1 = flush_pre_publish_cache(State),
    State2 = flush_delivered_cache(State1),
    maybe_flush_journal(JournalSizeHint, State2).

flush_pre_publish_cache(#qistate{pre_publish_cache = []} = State) ->
    State;
flush_pre_publish_cache(State = #qistate{pre_publish_cache = PPC}) ->
    {JournalHdl, State1} = get_journal_handle(State),
    file_handle_cache_stats:update(queue_index_journal_write),
    ok = file_handle_cache:append(JournalHdl, lists:reverse(PPC)),
    State1#qistate{pre_publish_cache = []}.

flush_delivered_cache(#qistate{delivered_cache = []} = State) ->
    State;
flush_delivered_cache(State = #qistate{delivered_cache = DC}) ->
    State1 = deliver(lists:reverse(DC), State),
    State1#qistate{delivered_cache = []}.

publish(MsgOrId, SeqId, MsgProps, IsPersistent, JournalSizeHint, State) ->
    {JournalHdl, State1} =
        get_journal_handle(
          maybe_needs_confirming(MsgProps, MsgOrId, State)),
    file_handle_cache_stats:update(queue_index_journal_write),
    {Bin, MsgBin} = create_pub_record_body(MsgOrId, MsgProps),
    ok = file_handle_cache:append(
           JournalHdl, [<<(case IsPersistent of
                               true  -> ?PUB_PERSIST_JPREFIX;
                               false -> ?PUB_TRANS_JPREFIX
                           end):?JPREFIX_BITS,
                          SeqId:?SEQ_BITS, Bin/binary,
                          (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin]),
    maybe_flush_journal(
      JournalSizeHint,
      add_to_journal(SeqId, {IsPersistent, Bin, MsgBin}, State1)).

maybe_needs_confirming(MsgProps, MsgOrId,
        State = #qistate{unconfirmed     = UC,
                         unconfirmed_msg = UCM}) ->
    MsgId = case MsgOrId of
                #basic_message{id = Id} -> Id;
                Id when is_binary(Id)   -> Id
            end,
    ?MSG_ID_BYTES = size(MsgId),
    case {MsgProps#message_properties.needs_confirming, MsgOrId} of
      {true,  MsgId} -> UC1  = gb_sets:add_element(MsgId, UC),
                        State#qistate{unconfirmed     = UC1};
      {true,  _}     -> UCM1 = gb_sets:add_element(MsgId, UCM),
                        State#qistate{unconfirmed_msg = UCM1};
      {false, _}     -> State
    end.

deliver(SeqIds, State) ->
    deliver_or_ack(del, SeqIds, State).

ack(SeqIds, State) ->
    deliver_or_ack(ack, SeqIds, State).

%% This is called when there are outstanding confirms or when the
%% queue is idle and the journal needs syncing (see needs_sync/1).
sync(State = #qistate { journal_handle = undefined }) ->
    State;
sync(State = #qistate { journal_handle = JournalHdl }) ->
    ok = file_handle_cache:sync(JournalHdl),
    notify_sync(State).

needs_sync(#qistate{journal_handle = undefined}) ->
    false;
needs_sync(#qistate{journal_handle  = JournalHdl,
                    unconfirmed     = UC,
                    unconfirmed_msg = UCM}) ->
    case gb_sets:is_empty(UC) andalso gb_sets:is_empty(UCM) of
        true  -> case file_handle_cache:needs_sync(JournalHdl) of
                     true  -> other;
                     false -> false
                 end;
        false -> confirms
    end.

flush(State = #qistate { dirty_count = 0 }) -> State;
flush(State)                                -> flush_journal(State).

read(StartEnd, StartEnd, State) ->
    {[], State};
read(Start, End, State = #qistate { segments = Segments,
                                    dir = Dir }) when Start =< End ->
    %% Start is inclusive, End is exclusive.
    LowerB = {StartSeg, _StartRelSeq} = seq_id_to_seg_and_rel_seq_id(Start),
    UpperB = {EndSeg,   _EndRelSeq}   = seq_id_to_seg_and_rel_seq_id(End - 1),
    {Messages, Segments1} =
        lists:foldr(fun (Seg, Acc) ->
                            read_bounded_segment(Seg, LowerB, UpperB, Acc, Dir)
                    end, {[], Segments}, lists:seq(StartSeg, EndSeg)),
    {Messages, State #qistate { segments = Segments1 }}.

next_segment_boundary(SeqId) ->
    {Seg, _RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),
    reconstruct_seq_id(Seg + 1, 0).

bounds(State = #qistate { segments = Segments }) ->
    %% This is not particularly efficient, but only gets invoked on
    %% queue initialisation.
    SegNums = lists:sort(segment_nums(Segments)),
    %% Don't bother trying to figure out the lowest seq_id, merely the
    %% seq_id of the start of the lowest segment. That seq_id may not
    %% actually exist, but that's fine. The important thing is that
    %% the segment exists and the seq_id reported is on a segment
    %% boundary.
    %%
    %% We also don't really care about the max seq_id. Just start the
    %% next segment: it makes life much easier.
    %%
    %% SegNums is sorted, ascending.
    {LowSeqId, NextSeqId} =
        case SegNums of
            []         -> {0, 0};
            [MinSeg|_] -> {reconstruct_seq_id(MinSeg, 0),
                           reconstruct_seq_id(1 + lists:last(SegNums), 0)}
        end,
    {LowSeqId, NextSeqId, State}.

start(VHost, DurableQueueNames) ->
    ok = rabbit_recovery_terms:start(VHost),
    {DurableTerms, DurableDirectories} =
        lists:foldl(
          fun(QName, {RecoveryTerms, ValidDirectories}) ->
                  DirName = queue_name_to_dir_name(QName),
                  RecoveryInfo = case rabbit_recovery_terms:read(VHost, DirName) of
                                     {error, _}  -> non_clean_shutdown;
                                     {ok, Terms} -> Terms
                                 end,
                  {[RecoveryInfo | RecoveryTerms],
                   sets:add_element(DirName, ValidDirectories)}
          end, {[], sets:new()}, DurableQueueNames),
    %% Any queue directory we've not been asked to recover is considered garbage
    rabbit_file:recursive_delete(
      [DirName ||
        DirName <- all_queue_directory_names(VHost),
        not sets:is_element(filename:basename(DirName), DurableDirectories)]),
    rabbit_recovery_terms:clear(VHost),

    %% The backing queue interface requires that the queue recovery terms
    %% which come back from start/1 are in the same order as DurableQueueNames
    OrderedTerms = lists:reverse(DurableTerms),
    {OrderedTerms, {fun queue_index_walker/1, {start, DurableQueueNames}}}.


stop(VHost) -> rabbit_recovery_terms:stop(VHost).

all_queue_directory_names(VHost) ->
    filelib:wildcard(filename:join([rabbit_vhost:msg_store_dir_path(VHost),
                                    "queues", "*"])).

all_queue_directory_names() ->
    filelib:wildcard(filename:join([rabbit_vhost:msg_store_dir_wildcard(),
                                    "queues", "*"])).

%%----------------------------------------------------------------------------
%% startup and shutdown
%%----------------------------------------------------------------------------

erase_index_dir(Dir) ->
    case rabbit_file:is_dir(Dir) of
        true  -> rabbit_file:recursive_delete([Dir]);
        false -> ok
    end.

% 新建一个 qistate 记录，这个记录很重要
blank_state(QueueName) ->
    Dir = queue_dir(QueueName),
    blank_state_name_dir_funs(QueueName,
                         Dir,
                         fun (_) -> ok end,
                         fun (_) -> ok end).

queue_dir(#resource{ virtual_host = VHost } = QueueName) ->
    %% Queue directory is
    %% {node_database_dir}/msg_stores/vhosts/{vhost}/queues/{queue}
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    QueueDir = queue_name_to_dir_name(QueueName),
    filename:join([VHostDir, "queues", QueueDir]).

queue_name_to_dir_name(#resource { kind = queue,
                                   virtual_host = VHost,
                                   name = QName }) ->
    <<Num:128>> = erlang:md5(<<"queue", VHost/binary, QName/binary>>),
    rabbit_misc:format("~.36B", [Num]).

queue_name_to_dir_name_legacy(Name = #resource { kind = queue }) ->
    <<Num:128>> = erlang:md5(term_to_binary_compat:term_to_binary_1(Name)),
    rabbit_misc:format("~.36B", [Num]).

queues_base_dir() ->
    rabbit_mnesia:dir().

% 一个队列对应一个 qistate
blank_state_name_dir_funs(Name, Dir, OnSyncFun, OnSyncMsgFun) ->
    {ok, MaxJournal} =
        application:get_env(rabbit, queue_index_max_journal_entries),
    #qistate { dir                 = Dir,
               segments            = segments_new(),
               journal_handle      = undefined,
               dirty_count         = 0,
               max_journal_entries = MaxJournal,
               on_sync             = OnSyncFun,
               on_sync_msg         = OnSyncMsgFun,
               unconfirmed         = gb_sets:new(),
               unconfirmed_msg     = gb_sets:new(),
               pre_publish_cache   = [],
               delivered_cache     = [],
               queue_name          = Name }.

init_clean(RecoveredCounts, State) ->
    %% Load the journal. Since this is a clean recovery this (almost)
    %% gets us back to where we were on shutdown.
    State1 = #qistate { dir = Dir, segments = Segments } = load_journal(State),
    %% The journal loading only creates records for segments touched
    %% by the journal, and the counts are based on the journal entries
    %% only. We need *complete* counts for *all* segments. By an
    %% amazing coincidence we stored that information on shutdown.
    Segments1 =
        lists:foldl(
          fun ({Seg, UnackedCount}, SegmentsN) ->
                  Segment = segment_find_or_new(Seg, Dir, SegmentsN),
                  segment_store(Segment #segment { unacked = UnackedCount },
                                SegmentsN)
          end, Segments, RecoveredCounts),
    %% the counts above include transient messages, which would be the
    %% wrong thing to return
    {undefined, undefined, State1 # qistate { segments = Segments1 }}.

init_dirty(CleanShutdown, ContainsCheckFun, State) ->
    %% Recover the journal completely. This will also load segments
    %% which have entries in the journal and remove duplicates. The
    %% counts will correctly reflect the combination of the segment
    %% and the journal.
    State1 = #qistate { dir = Dir, segments = Segments } =
        recover_journal(State),
    {Segments1, Count, Bytes, DirtyCount} =
        %% Load each segment in turn and filter out messages that are
        %% not in the msg_store, by adding acks to the journal. These
        %% acks only go to the RAM journal as it doesn't matter if we
        %% lose them. Also mark delivered if not clean shutdown. Also
        %% find the number of unacked messages. Also accumulate the
        %% dirty count here, so we can call maybe_flush_journal below
        %% and avoid unnecessary file system operations.
        lists:foldl(
          fun (Seg, {Segments2, CountAcc, BytesAcc, DirtyCount}) ->
                  {{Segment = #segment { unacked = UnackedCount }, Dirty},
                   UnackedBytes} =
                      recover_segment(ContainsCheckFun, CleanShutdown,
                                      segment_find_or_new(Seg, Dir, Segments2)),
                  {segment_store(Segment, Segments2),
                   CountAcc + UnackedCount,
                   BytesAcc + UnackedBytes, DirtyCount + Dirty}
          end, {Segments, 0, 0, 0}, all_segment_nums(State1)),
    State2 = maybe_flush_journal(State1 #qistate { segments = Segments1,
                                                   dirty_count = DirtyCount }),
    {Count, Bytes, State2}.

terminate(State = #qistate { journal_handle = JournalHdl,
                             segments = Segments }) ->
    ok = case JournalHdl of
             undefined -> ok;
             _         -> file_handle_cache:close(JournalHdl)
         end,
    SegmentCounts =
        segment_fold(
          fun (#segment { num = Seg, unacked = UnackedCount }, Acc) ->
                  [{Seg, UnackedCount} | Acc]
          end, [], Segments),
    {SegmentCounts, State #qistate { journal_handle = undefined,
                                     segments = undefined }}.

recover_segment(ContainsCheckFun, CleanShutdown,
                Segment = #segment { journal_entries = JEntries }) ->
    {SegEntries, UnackedCount} = load_segment(false, Segment),
    {SegEntries1, UnackedCountDelta} =
        segment_plus_journal(SegEntries, JEntries),
    array:sparse_foldl(
      fun (RelSeq, {{IsPersistent, Bin, MsgBin}, Del, no_ack},
           {SegmentAndDirtyCount, Bytes}) ->
              {MsgOrId, MsgProps} = parse_pub_record_body(Bin, MsgBin),
              {recover_message(ContainsCheckFun(MsgOrId), CleanShutdown,
                               Del, RelSeq, SegmentAndDirtyCount),
               Bytes + case IsPersistent of
                           true  -> MsgProps#message_properties.size;
                           false -> 0
                       end}
      end,
      {{Segment #segment { unacked = UnackedCount + UnackedCountDelta }, 0}, 0},
      SegEntries1).

recover_message( true,  true,   _Del, _RelSeq, SegmentAndDirtyCount) ->
    SegmentAndDirtyCount;
recover_message( true, false,    del, _RelSeq, SegmentAndDirtyCount) ->
    SegmentAndDirtyCount;
recover_message( true, false, no_del,  RelSeq, {Segment, DirtyCount}) ->
    {add_to_journal(RelSeq, del, Segment), DirtyCount + 1};
recover_message(false,     _,    del,  RelSeq, {Segment, DirtyCount}) ->
    {add_to_journal(RelSeq, ack, Segment), DirtyCount + 1};
recover_message(false,     _, no_del,  RelSeq, {Segment, DirtyCount}) ->
    {add_to_journal(RelSeq, ack,
                    add_to_journal(RelSeq, del, Segment)),
     DirtyCount + 2}.

%%----------------------------------------------------------------------------
%% msg store startup delta function
%%----------------------------------------------------------------------------

queue_index_walker({start, DurableQueues}) when is_list(DurableQueues) ->
    {ok, Gatherer} = gatherer:start_link(),
    % 对每一个 queue，启动一个异步任务去调用 queue_index_walker_reader
    [begin
         ok = gatherer:fork(Gatherer),
         ok = worker_pool:submit_async(
                fun () -> link(Gatherer),
                          ok = queue_index_walker_reader(QueueName, Gatherer),
                          unlink(Gatherer),
                          ok
                end)
     end || QueueName <- DurableQueues],
    queue_index_walker({next, Gatherer});

queue_index_walker({next, Gatherer}) when is_pid(Gatherer) ->
    case gatherer:out(Gatherer) of
        empty ->
            unlink(Gatherer),
            ok = gatherer:stop(Gatherer),
            finished;
        {value, {MsgId, Count}} ->
            {MsgId, Count, {next, Gatherer}}
    end.

queue_index_walker_reader(QueueName, Gatherer) ->
    ok = scan_queue_segments( % 扫描一个 queue 中的所有 idx 文件
           fun (_SeqId, MsgId, _MsgProps, true, _IsDelivered, no_ack, ok)
                 when is_binary(MsgId) -> % 消息保存在 idx 文件中才会匹配这个函数
                   gatherer:sync_in(Gatherer, {MsgId, 1});
               (_SeqId, _MsgId, _MsgProps, _IsPersistent, _IsDelivered,
                _IsAcked, Acc) ->
                   Acc
           end, ok, QueueName),
    ok = gatherer:finish(Gatherer).

scan_queue_segments(Fun, Acc, QueueName) ->
    State = #qistate { segments = Segments, dir = Dir } =
        recover_journal(blank_state(QueueName)),  % 读取并恢复一个 journal 文件，转换成相应结构返回
    Result = lists:foldr(
      fun (Seg, AccN) ->
              segment_entries_foldr(
                fun (RelSeq, {{MsgOrId, MsgProps, IsPersistent},
                              IsDelivered, IsAcked}, AccM) ->
                        Fun(reconstruct_seq_id(Seg, RelSeq), MsgOrId, MsgProps,
                            IsPersistent, IsDelivered, IsAcked, AccM)
                end, AccN, segment_find_or_new(Seg, Dir, Segments))
      end, Acc, all_segment_nums(State)), % 遍历每一个 idx 文件
    {_SegmentCounts, _State} = terminate(State),
    Result.

%%----------------------------------------------------------------------------
%% expiry/binary manipulation
%%----------------------------------------------------------------------------

create_pub_record_body(MsgOrId, #message_properties { expiry = Expiry,
                                                      size   = Size }) ->
    ExpiryBin = expiry_to_binary(Expiry),
    case MsgOrId of
        MsgId when is_binary(MsgId) ->
            {<<MsgId/binary, ExpiryBin/binary, Size:?SIZE_BITS>>, <<>>};
        #basic_message{id = MsgId} ->
            MsgBin = term_to_binary(MsgOrId),
            {<<MsgId/binary, ExpiryBin/binary, Size:?SIZE_BITS>>, MsgBin}
    end.

expiry_to_binary(undefined) -> <<?NO_EXPIRY:?EXPIRY_BITS>>;
expiry_to_binary(Expiry)    -> <<Expiry:?EXPIRY_BITS>>.

% segment entry 二进制数据转 basic_message record
parse_pub_record_body(<<MsgIdNum:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                        Size:?SIZE_BITS>>, MsgBin) ->
    %% work around for binary data fragmentation. See
    %% rabbit_msg_file:read_next/2
    <<MsgId:?MSG_ID_BYTES/binary>> = <<MsgIdNum:?MSG_ID_BITS>>,
    Props = #message_properties{expiry = case Expiry of
                                             ?NO_EXPIRY -> undefined;
                                             X          -> X
                                         end,
                                size   = Size},
    case MsgBin of
        <<>> -> {MsgId, Props};
        _    -> Msg = #basic_message{id = MsgId} = binary_to_term(MsgBin),
                {Msg, Props}
    end.

%%----------------------------------------------------------------------------
%% journal manipulation
%%----------------------------------------------------------------------------
%% 传入一个 journal entry(SeqId + Action)和当前状态，把 entry 加到当前状态中形成一个新的状态，然后返回
%% 参数 State 的初始状态参见函数 blank_state_name_dir_funs
add_to_journal(SeqId, Action, State = #qistate { dirty_count = DCount,
                                                 segments = Segments, % 初始结构 {#{},[]}
                                                 dir = Dir }) ->
    {Seg, RelSeq} = seq_id_to_seg_and_rel_seq_id(SeqId),  % 根据消息 id 算出消息所在的 idx 文件序号 Seg 和文件内 entry 序号 RelSeq
    Segment = segment_find_or_new(Seg, Dir, Segments),    % 根据 idx 文件序号 Seg 从 Segments 中读取或新建一个对应的 segment record
    Segment1 = add_to_journal(RelSeq, Action, Segment),   % 根据 RelSeq 和 Action 构造 entry 然后存入 Segment 中，返回更新后的 Segment
    State #qistate { dirty_count = DCount + 1,
                     segments = segment_store(Segment1, Segments) };  % 将 Segment1 保存到 Segments，可能保存到 map 或者缓存数组中

% 根据参数 RelSeq 和 Action 构造一个 entry，把它放到 Segment 内部结构中，然后返回这个更新后的 Segment
add_to_journal(RelSeq, Action,
               Segment = #segment { journal_entries = JEntries, % 数组
                                    entries_to_segment = EToSeg,  % 列表数组，每个列表存一个或多个 entry 二进制数据
                                    unacked = UnackedCount }) ->

    % 从 JEntries 的 RelSeq 位置获取 entry，然后把 Action 状态融合进去
    % 如果指定位置没有，就新建一个带有 Action 状态的 entry
    % Action 可以有三种类型 pub(三元组{IsPersistent, Bin, MsgBin})，delivery(原子)，ack(原子)
    {Fun, Entry} = action_to_entry(RelSeq, Action, JEntries),

    {JEntries1, EToSeg1} =
        case Fun of
            set ->  % set 表示 Entry 是新建的或有更新
                {array:set(RelSeq, Entry, JEntries),  % 将 Entry 放入 JEntries 数组中 RelSeq 指向的位置
                 array:set(RelSeq, entry_to_segment(RelSeq, Entry, []), % entry_to_segment 将 Entry 转换成 idx 文件中 entry 的二进制格式返回
                           EToSeg)};  % EToSeg 存放类似数据 {[pub],[del,ack],[pub,del,ack],...}
            reset ->  % reset 表示当前 entry 需要被删除
                {array:reset(RelSeq, JEntries), % 把数组里下标为 RelSeq 的值重置为默认值
                 array:reset(RelSeq, EToSeg)}   % 把数组里下标为 RelSeq 的值重置为默认值
        end,

    Segment #segment {
      journal_entries = JEntries1,  % 更新 journal entry 数组
      entries_to_segment = EToSeg1, % 更新 segment entry 数组
      unacked = UnackedCount + case Action of % 更新 segment 中未 ack 的消息总数
                                   ?PUB -> +1;
                                   del  ->  0;
                                   ack  -> -1
                               end}.

action_to_entry(RelSeq, Action, JEntries) ->
    case array:get(RelSeq, JEntries) of
        undefined ->
            {set,
             case Action of % journal 文件中没有和 RelSeq 相同的 entry，那么这条 entry 的状态是独立的
                 ?PUB -> {Action, no_del, no_ack};
                 del  -> {no_pub,    del, no_ack};
                 ack  -> {no_pub, no_del,    ack}
             end};
        ({Pub,    no_del, no_ack}) when Action == del ->
            {set, {Pub,    del, no_ack}}; % 合并 pub + del 状态
        ({no_pub,    del, no_ack}) when Action == ack ->
            {set, {no_pub, del,    ack}}; % 合并 del + ack 状态
        ({?PUB,      del, no_ack}) when Action == ack ->
            {reset, none} % 合并 pub + del + ack 状态，返回 none 相当于可以删除这条 entry 了
    end.

maybe_flush_journal(State) ->
    maybe_flush_journal(infinity, State).

maybe_flush_journal(Hint, State = #qistate { dirty_count = DCount,
                                             max_journal_entries = MaxJournal })
  when DCount > MaxJournal orelse (Hint =/= infinity andalso DCount > Hint) ->
    flush_journal(State);
maybe_flush_journal(_Hint, State) ->
    State.

flush_journal(State = #qistate { segments = Segments }) ->
    Segments1 =
        segment_fold(
          fun (#segment { unacked = 0, path = Path }, SegmentsN) ->
                  case rabbit_file:is_file(Path) of
                      true  -> ok = rabbit_file:delete(Path);
                      false -> ok
                  end,
                  SegmentsN;
              (#segment {} = Segment, SegmentsN) ->
                  segment_store(append_journal_to_segment(Segment), SegmentsN)
          end, segments_new(), Segments),
    {JournalHdl, State1} =
        get_journal_handle(State #qistate { segments = Segments1 }),
    ok = file_handle_cache:clear(JournalHdl),
    notify_sync(State1 #qistate { dirty_count = 0 }).

append_journal_to_segment(#segment { journal_entries = JEntries,
                                     entries_to_segment = EToSeg,
                                     path = Path } = Segment) ->
    case array:sparse_size(JEntries) of
        0 -> Segment;
        _ ->
            file_handle_cache_stats:update(queue_index_write),

            {ok, Hdl} = file_handle_cache:open_with_absolute_path(
                          Path, ?WRITE_MODE,
                          [{write_buffer, infinity}]),
            %% the file_handle_cache also does a list reverse, so this
            %% might not be required here, but before we were doing a
            %% sparse_foldr, a lists:reverse/1 seems to be the correct
            %% thing to do for now.
            file_handle_cache:append(Hdl, lists:reverse(array:to_list(EToSeg))),
            ok = file_handle_cache:close(Hdl),
            Segment #segment { journal_entries    = array_new(),
                               entries_to_segment = array_new([]) }
    end.

% 打开一个队列的 journal.jif 文件，返回文件句柄
get_journal_handle(State = #qistate { journal_handle = undefined,
                                      dir = Dir,
                                      queue_name = Name }) ->
    Path = filename:join(Dir, ?JOURNAL_FILENAME),
    ok = rabbit_file:ensure_dir(Path),
    ok = ensure_queue_name_stub_file(Dir, Name),
    {ok, Hdl} = file_handle_cache:open_with_absolute_path(
                  Path, ?WRITE_MODE, [{write_buffer, infinity}]),
    {Hdl, State #qistate { journal_handle = Hdl }};
get_journal_handle(State = #qistate { journal_handle = Hdl }) ->
    {Hdl, State}.

%% Loading Journal. This isn't idempotent and will mess up the counts
%% if you call it more than once on the same state. Assumes the counts
%% are 0 to start with.
%% 读取并解析一个队列的 journal 文件，更新 State 内部结构然后返回
load_journal(State = #qistate { dir = Dir }) ->
    Path = filename:join(Dir, ?JOURNAL_FILENAME),
    case rabbit_file:is_file(Path) of
        true  -> {JournalHdl, State1} = get_journal_handle(State),  % 打开 journal 文件，返回的 State1 只改了参数 State 的 journal_handle 字段而已
                 Size = rabbit_file:file_size(Path),
                 {ok, 0} = file_handle_cache:position(JournalHdl, 0),
                 {ok, JournalBin} = file_handle_cache:read(JournalHdl, Size), % 将 journal 文件的数据读出来
                 parse_journal_entries(JournalBin, State1); % 递归解析从 journal 文件读出来的二进制数据，存入 State1 中
        false -> State
    end.

%% ditto
recover_journal(State) ->
    State1 = #qistate { segments = Segments } = load_journal(State),  % 读取并解析 journal 文件
    % 遍历刚刚解析出来的 journal entry，结合 segment 中相同 entry 的状态做一些调整，最终保存到 State 中并返回
    Segments1 =
        segment_map(
          fun (Segment = #segment { journal_entries = JEntries,
                                    entries_to_segment = EToSeg,
                                    unacked = UnackedCountInJournal }) ->
                  %% We want to keep ack'd entries in so that we can
                  %% remove them if duplicates are in the journal. The
                  %% counts here are purely from the segment itself.
                  {SegEntries, UnackedCountInSeg} = load_segment(true, Segment),  % 加载并解析一个 idx 文件，参数 Segment 只用到了 path 字段
                  {JEntries1, EToSeg1, UnackedCountDuplicates} =
                      journal_minus_segment(JEntries, EToSeg, SegEntries),  % 更新 journal entry 状态，需要减去 segment 中相同 entry 的状态
                  Segment #segment { journal_entries = JEntries1,
                                     entries_to_segment = EToSeg1,
                                     unacked = (UnackedCountInJournal +
                                                    UnackedCountInSeg -
                                                    UnackedCountDuplicates) }
          end, Segments),
    State1 #qistate { segments = Segments1 }.

% 解析消费消息 entry
parse_journal_entries(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>, State) ->
    parse_journal_entries(Rest, add_to_journal(SeqId, del, State)); % 先将解析出的消息保存到对应 segment 中，然后递归解析 journal 文件数据

% 解析确认消息 entry
parse_journal_entries(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>, State) ->
    parse_journal_entries(Rest, add_to_journal(SeqId, ack, State)); % 先将解析出的消息保存到对应 segment 中，然后递归解析 journal 文件数据

% 解析脏数据 entry
parse_journal_entries(<<0:?JPREFIX_BITS, 0:?SEQ_BITS,
                        0:?PUB_RECORD_SIZE_BYTES/unit:8, _/binary>>, State) ->
    %% Journal entry composed only of zeroes was probably
    %% produced during a dirty shutdown so stop reading
    State;

% 解析生产消息 entry
parse_journal_entries(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Bin:?PUB_RECORD_BODY_BYTES/binary,  % <<MsgId:16/binary, Expire:8/binary, Size:4/binary>>
                        MsgSize:?EMBEDDED_SIZE_BITS, MsgBin:MsgSize/binary, % MsgBin 存消息内容
                        Rest/binary>>, State) ->
    IsPersistent = case Prefix of
                       ?PUB_PERSIST_JPREFIX -> true;
                       ?PUB_TRANS_JPREFIX   -> false
                   end,
    parse_journal_entries(
      Rest, add_to_journal(SeqId, {IsPersistent, Bin, MsgBin}, State)); % 先将解析出的消息保存到对应 segment 中，然后递归解析 journal 文件数据
parse_journal_entries(_ErrOrEoF, State) ->
    State.

deliver_or_ack(_Kind, [], State) ->
    State;
deliver_or_ack(Kind, SeqIds, State) ->
    JPrefix = case Kind of ack -> ?ACK_JPREFIX; del -> ?DEL_JPREFIX end,
    {JournalHdl, State1} = get_journal_handle(State),
    file_handle_cache_stats:update(queue_index_journal_write),
    ok = file_handle_cache:append(
           JournalHdl,
           [<<JPrefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>> || SeqId <- SeqIds]),
    maybe_flush_journal(lists:foldl(fun (SeqId, StateN) ->
                                            add_to_journal(SeqId, Kind, StateN)
                                    end, State1, SeqIds)).

notify_sync(State = #qistate{unconfirmed     = UC,
                             unconfirmed_msg = UCM,
                             on_sync         = OnSyncFun,
                             on_sync_msg     = OnSyncMsgFun}) ->
    State1 = case gb_sets:is_empty(UC) of
                 true  -> State;
                 false -> OnSyncFun(UC),
                          State#qistate{unconfirmed = gb_sets:new()}
             end,
    case gb_sets:is_empty(UCM) of
        true  -> State1;
        false -> OnSyncMsgFun(UCM),
                 State1#qistate{unconfirmed_msg = gb_sets:new()}
    end.

%%----------------------------------------------------------------------------
%% segment manipulation
%%----------------------------------------------------------------------------

seq_id_to_seg_and_rel_seq_id(SeqId) ->
    { SeqId div ?SEGMENT_ENTRY_COUNT, SeqId rem ?SEGMENT_ENTRY_COUNT }.

reconstruct_seq_id(Seg, RelSeq) ->
    (Seg * ?SEGMENT_ENTRY_COUNT) + RelSeq.

all_segment_nums(#qistate { dir = Dir, segments = Segments }) ->
    lists:sort(
      sets:to_list(
        lists:foldl(
          fun (SegName, Set) ->
                  sets:add_element(
                    list_to_integer(
                      lists:takewhile(fun (C) -> $0 =< C andalso C =< $9 end,
                                      SegName)), Set)
          end, sets:from_list(segment_nums(Segments)),
          rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir)))).

% 在 Segments 中查找 Seg 序号对应的 idx 文件信息，没找到就创建一个 segment
segment_find_or_new(Seg, Dir, Segments) ->
    case segment_find(Seg, Segments) of
        {ok, Segment} -> Segment; % 将找到的 segment 返回
        error         -> SegName = integer_to_list(Seg)  ++ ?SEGMENT_EXTENSION, % 未找到，新建一个 segment 并返回
                         Path = filename:join(Dir, SegName),  % Path 结构如 0.idx, 1.idx, ...
                         #segment { num                = Seg,
                                    path               = Path,
                                    journal_entries    = array_new(),   % 空数组
                                    entries_to_segment = array_new([]), % 空数组
                                    unacked            = 0 }
    end.

% 最近使用过的两个文件会缓存在列表中，现在列表中查找，然后在映射组中查找
segment_find(Seg, {_Segments, [Segment = #segment { num = Seg } |_]}) ->
    {ok, Segment}; %% 1 or (2, matches head)
segment_find(Seg, {_Segments, [_, Segment = #segment { num = Seg }]}) ->
    {ok, Segment}; %% 2, matches tail
segment_find(Seg, {Segments, _}) -> %% no match
    maps:find(Seg, Segments).

segment_store(Segment = #segment { num = Seg }, %% 1 or (2, matches head)
              {Segments, [#segment { num = Seg } | Tail]}) ->
    {Segments, [Segment | Tail]}; % 命中缓存数组，直接覆盖
segment_store(Segment = #segment { num = Seg }, %% 2, matches tail
              {Segments, [SegmentA, #segment { num = Seg }]}) ->
    {Segments, [Segment, SegmentA]};  % 命中缓存数组，直接覆盖
segment_store(Segment = #segment { num = Seg }, {Segments, []}) ->
    {maps:remove(Seg, Segments), [Segment]};  % 缓存为空
segment_store(Segment = #segment { num = Seg }, {Segments, [SegmentA]}) ->
    {maps:remove(Seg, Segments), [Segment, SegmentA]};  % 缓存有一个位置，把要存入的 segment 从映射组中删除，存到缓存数组中
segment_store(Segment = #segment { num = Seg },
              {Segments, [SegmentA, SegmentB]}) ->
    {maps:put(SegmentB#segment.num, SegmentB, maps:remove(Seg, Segments)),  % 缓存没位置了，淘汰一个放入映射组，当前 segment 存入缓存
     [Segment, SegmentA]}.

segment_fold(Fun, Acc, {Segments, CachedSegments}) ->
    maps:fold(fun (_Seg, Segment, Acc1) -> Fun(Segment, Acc1) end,
              lists:foldl(Fun, Acc, CachedSegments), Segments).

segment_map(Fun, {Segments, CachedSegments}) ->
    {maps:map(fun (_Seg, Segment) -> Fun(Segment) end, Segments), % 对映射组里的值循环执行一个系列操作并返回一个新的映射组
     lists:map(Fun, CachedSegments)}. % 列表里的每一个元素被函数调用

segment_nums({Segments, CachedSegments}) ->
    lists:map(fun (#segment { num = Num }) -> Num end, CachedSegments) ++
        maps:keys(Segments).

segments_new() ->
    {#{}, []}.

% 消息既被消费了，也被 ack 了，就不转存到 idx 文件中了
entry_to_segment(_RelSeq, {?PUB, del, ack}, Initial) ->
    Initial;
% 消息没被消费，且没被 ack，则将消息转成 idx 文件要求的 entry 格式，要注意一条消息在 idx 文件中一个是按照 pub -> delivery -> ack 从前往后存放的
% 转换好的二进制数据放到空数组 Initial 中，然后作为返回值返回
entry_to_segment(RelSeq, {Pub, Del, Ack}, Initial) ->
    %% NB: we are assembling the segment in reverse order here, so
    %% del/ack comes first.
    Buf1 = case {Del, Ack} of
               {no_del, no_ack} ->
                   Initial;
               _ -> % idx 文件中的消费或者确认类型的消息
                   Binary = <<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                              RelSeq:?REL_SEQ_BITS>>,
                   case {Del, Ack} of
                       {del, ack} -> [[Binary, Binary] | Initial];  % 消息被消费且被 ack 了，向 idx 文件中存两条相同的数据，一条表示已消费，一条表示已 ack
                       _          -> [Binary | Initial] % 消息被消费了，还没被 ack，向 idx 文件中写一条数据，表示消费了一条消息
                   end
           end,
    case Pub of
        no_pub ->
            Buf1;
        {IsPersistent, Bin, MsgBin} ->
            % 组装成 idx 文件中的 entry
            [[<<?PUB_PREFIX:?PUB_PREFIX_BITS,
                (bool_to_int(IsPersistent)):1,
                RelSeq:?REL_SEQ_BITS, Bin/binary,
                (size(MsgBin)):?EMBEDDED_SIZE_BITS>>, MsgBin] | Buf1]
    end.

read_bounded_segment(Seg, {StartSeg, StartRelSeq}, {EndSeg, EndRelSeq},
                     {Messages, Segments}, Dir) ->
    Segment = segment_find_or_new(Seg, Dir, Segments),
    {segment_entries_foldr(
       fun (RelSeq, {{MsgOrId, MsgProps, IsPersistent}, IsDelivered, no_ack},
            Acc)
             when (Seg > StartSeg orelse StartRelSeq =< RelSeq) andalso
                  (Seg < EndSeg   orelse EndRelSeq   >= RelSeq) ->
               [{MsgOrId, reconstruct_seq_id(StartSeg, RelSeq), MsgProps,
                 IsPersistent, IsDelivered == del} | Acc];
           (_RelSeq, _Value, Acc) ->
               Acc
       end, Messages, Segment),
     segment_store(Segment, Segments)}.

segment_entries_foldr(Fun, Init,
                      Segment = #segment { journal_entries = JEntries }) ->
    {SegEntries, _UnackedCount} = load_segment(false, Segment), % 又重新解析一遍？
    {SegEntries1, _UnackedCountD} = segment_plus_journal(SegEntries, JEntries),
    array:sparse_foldr(
      fun (RelSeq, {{IsPersistent, Bin, MsgBin}, Del, Ack}, Acc) ->
              {MsgOrId, MsgProps} = parse_pub_record_body(Bin, MsgBin), % 返回值 {basic_message,message_properties} 或者 {msg_id,message_properties}
              Fun(RelSeq, {{MsgOrId, MsgProps, IsPersistent}, Del, Ack}, Acc)
      end, Init, SegEntries1).

%% Loading segments
%%
%% Does not do any combining with the journal at all.
%% 读取并解析一个 idx 文件，最终返回结构如 Empty 所示
load_segment(KeepAcked, #segment { path = Path }) ->
    Empty = {array_new(), 0}, % 结构状态 {{{IsPersistent, Bin, MsgBin}, no_del, no_ack}, Unacked}
    case rabbit_file:is_file(Path) of
        false -> Empty;
        true  -> Size = rabbit_file:file_size(Path),
                 file_handle_cache_stats:update(queue_index_read),
                 {ok, Hdl} = file_handle_cache:open_with_absolute_path(
                               Path, ?READ_MODE, []),
                 {ok, 0} = file_handle_cache:position(Hdl, bof),
                 {ok, SegBin} = file_handle_cache:read(Hdl, Size),
                 ok = file_handle_cache:close(Hdl),
                 Res = parse_segment_entries(SegBin, KeepAcked, Empty), % 解析 idx 文件中完整的二进制数据
                 Res
    end.

% 解析 idx 文件的 pub 消息
parse_segment_entries(<<?PUB_PREFIX:?PUB_PREFIX_BITS,
                        IsPersistNum:1, RelSeq:?REL_SEQ_BITS, Rest/binary>>,
                      KeepAcked, Acc) ->
    parse_segment_publish_entry(  % 把解析出来的数据存到 Acc 中
      Rest, 1 == IsPersistNum, RelSeq, KeepAcked, Acc);

% 解析 idx 文件的 del、ack 消息
parse_segment_entries(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                       RelSeq:?REL_SEQ_BITS, Rest/binary>>, KeepAcked, Acc) ->
    parse_segment_entries(
      Rest, KeepAcked, add_segment_relseq_entry(KeepAcked, RelSeq, Acc));
parse_segment_entries(<<>>, _KeepAcked, Acc) ->
    Acc.

% 把一条 pub 消息存到 SegEntries 数组中，RelSeq 作为数组下标
parse_segment_publish_entry(<<Bin:?PUB_RECORD_BODY_BYTES/binary,
                              MsgSize:?EMBEDDED_SIZE_BITS,
                              MsgBin:MsgSize/binary, Rest/binary>>,
                            IsPersistent, RelSeq, KeepAcked,
                            {SegEntries, Unacked}) ->
    Obj = {{IsPersistent, Bin, MsgBin}, no_del, no_ack},
    SegEntries1 = array:set(RelSeq, Obj, SegEntries),
    parse_segment_entries(Rest, KeepAcked, {SegEntries1, Unacked + 1}); % 递归解析下一个 entry
parse_segment_publish_entry(Rest, _IsPersistent, _RelSeq, KeepAcked, Acc) ->
    parse_segment_entries(Rest, KeepAcked, Acc).

% 把一条 del/ack 消息覆盖到 SegEntries 数组中，注意这里一定可以在数组中找到一条之前的消息
% 因为同一条消息（SeqId）唯一确定一个 idx 文件号和文件内偏移量，无论这条消息是 pub，del，ack
add_segment_relseq_entry(KeepAcked, RelSeq, {SegEntries, Unacked}) ->
    case array:get(RelSeq, SegEntries) of
        {Pub, no_del, no_ack} ->
            {array:set(RelSeq, {Pub, del, no_ack}, SegEntries), Unacked};
        {Pub, del, no_ack} when KeepAcked ->
            {array:set(RelSeq, {Pub, del, ack},    SegEntries), Unacked - 1};
        {_Pub, del, no_ack} ->
            {array:reset(RelSeq,                   SegEntries), Unacked - 1}
    end.

array_new() ->
    array_new(undefined).

array_new(Default) ->
    array:new([{default, Default}, fixed, {size, ?SEGMENT_ENTRY_COUNT}]). % 创建一个固定大小，默认值为 Default 的数组

bool_to_int(true ) -> 1;
bool_to_int(false) -> 0.

%%----------------------------------------------------------------------------
%% journal & segment combination
%%----------------------------------------------------------------------------

%% Combine what we have just read from a segment file with what we're
%% holding for that segment in memory. There must be no duplicates.
%% 把 segment entry 和 journal entry 的状态结合起来，规则是
%% 1. entry 只在 journal 文件中，并且还未 ack，则移动到 segment 中
%% 2. entry 只在 journal 文件中，并且已经 ack，不做操作
%% 3. segment entry 的状态是 pub，journal entry 的状态是 del，把 segment entry 状态合并成 pub+del
%% 4. segment entry 的状态还未 ack，journal entry 状态已 ack，把 segment entry 删除
segment_plus_journal(SegEntries, JEntries) ->
    array:sparse_foldl(
      fun (RelSeq, JObj, {SegEntriesOut, AdditionalUnacked}) -> % JObj 是 journal entry
              SegEntry = array:get(RelSeq, SegEntriesOut),
              {Obj, AdditionalUnackedDelta} =
                  segment_plus_journal1(SegEntry, JObj),  % 将 segment entry 和 journal entry 相结合
              {case Obj of
                   undefined -> array:reset(RelSeq, SegEntriesOut);
                   _         -> array:set(RelSeq, Obj, SegEntriesOut) % 将结合状态之后的 entry 保存到 segment entry 数组中
               end,
               AdditionalUnacked + AdditionalUnackedDelta}
      end, {SegEntries, 0}, JEntries).

%% Here, the result is a tuple with the first element containing the
%% item which we may be adding to (for items only in the journal),
%% modifying in (bits in both), or, when returning 'undefined',
%% erasing from (ack in journal, not segment) the segment array. The
%% other element of the tuple is the delta for AdditionalUnacked.
segment_plus_journal1(undefined, {?PUB, no_del, no_ack} = Obj) ->
    {Obj, 1};
segment_plus_journal1(undefined, {?PUB, del, no_ack} = Obj) ->
    {Obj, 1};
segment_plus_journal1(undefined, {?PUB, del, ack}) ->
    {undefined, 0};

segment_plus_journal1({?PUB = Pub, no_del, no_ack}, {no_pub, del, no_ack}) ->
    {{Pub, del, no_ack}, 0};
segment_plus_journal1({?PUB, no_del, no_ack},       {no_pub, del, ack}) ->
    {undefined, -1};
segment_plus_journal1({?PUB, del, no_ack},          {no_pub, no_del, ack}) ->
    {undefined, -1}.

%% Remove from the journal entries for a segment, items that are
%% duplicates of entries found in the segment itself. Used on start up
%% to clean up the journal.
%%
%% We need to update the entries_to_segment since they are just a
%% cache of what's on the journal.
%% 遍历 JEntries 中每一个 journal entry，跟 SegEntries 中的 segment entry 进行对比
%% 来决定要对 journal entry 修改、删除还是原样保存
%% 以保证两种 entry 的状态不冲突、不重合，另外 SegEntries 内容不做修改
journal_minus_segment(JEntries, EToSeg, SegEntries) ->
    array:sparse_foldl(
      fun (RelSeq, JObj, {JEntriesOut, EToSegOut, UnackedRemoved}) ->
              SegEntry = array:get(RelSeq, SegEntries), % 从 segment entry 数组中找 entry，没找到返回 undefined
              % 根据 RelSeq 对应 entry 分别在 journal 文件和 idx 文件中的存在情况和相应的状态，返回一个二元组
              % 二元组用来指导下面对 JEntriesOut 和 EToSegOut 两个数组的修改
              {Obj, UnackedRemovedDelta} =
                  journal_minus_segment1(JObj, SegEntry),
              {JEntriesOut1, EToSegOut1} =
                  case Obj of
                      keep      ->
                          % entry 继续保存在 journal 数组中，有几种情况需要这么做
                          % 1. segment entry 不存在，journal entry 中有 pub 状态
                          % 2. segment entry 还没有 ack，journal entry 有更新的状态，例如 del、ack
                          {JEntriesOut, EToSegOut};
                      undefined ->
                          % 把 entry 从 journal 数组中删除，有几种情况需要这么做
                          % 1. segment entry 和 journal entry 状态完全相同
                          % 2. segment entry 的状态是 journal entry 状态的超集
                          % 3. segment entry 不存在，并且 journal entry 没有 pub 状态，属于非法消息
                          {array:reset(RelSeq, JEntriesOut),
                           array:reset(RelSeq, EToSegOut)};
                      _         ->
                          % entry 继续保存在 journal 数组中，但是需要 entry 状态有修改
                          % 这种情况是 journal entry 和 segment entry 有重叠的状态时发生，需要把 journal entry 中的重叠状态删除，然后返回到 Obj
                          % 例如 journal entry 状态是 {pub,del,no_ack}，segment entry 状态是 {pub,no_del,no_ack}，那么需要返回 {no_pub,del,no_ack}
                          % 即把 journal entry 中重叠的 pub 状态删除
                          {array:set(RelSeq, Obj, JEntriesOut),
                           array:set(RelSeq, entry_to_segment(RelSeq, Obj, []),
                                     EToSegOut)}
                  end,
               {JEntriesOut1, EToSegOut1, UnackedRemoved + UnackedRemovedDelta}
      end, {JEntries, EToSeg, 0}, JEntries).

%% Here, the result is a tuple with the first element containing the
%% item we are adding to or modifying in the (initially fresh) journal
%% array. If the item is 'undefined' we leave the journal array
%% alone. The other element of the tuple is the deltas for
%% UnackedRemoved.

%% Both the same. Must be at least the publish
%% idx 文件和 journal 文件中拥有相同的 entry，把 journal 文件中的 entry 删掉
journal_minus_segment1({?PUB, _Del, no_ack} = Obj, Obj) ->
    {undefined, 1};
journal_minus_segment1({?PUB, _Del, ack} = Obj,    Obj) ->
    {undefined, 0};

%% Just publish in journal
%% journal 文件中有 pub 类型的 entry，idx 文件中没有，继续保留在 journal 文件中
journal_minus_segment1({?PUB, no_del, no_ack},     undefined) ->
    {keep, 0};

%% Publish and deliver in journal
journal_minus_segment1({?PUB, del, no_ack},        undefined) ->
    {keep, 0};
journal_minus_segment1({?PUB = Pub, del, no_ack},  {Pub, no_del, no_ack}) ->
    {{no_pub, del, no_ack}, 1}; % journal entry 有 pub、del，segment entry 有 pub，删除 journal entry 中的 pub，保留 del

%% Publish, deliver and ack in journal
journal_minus_segment1({?PUB, del, ack},           undefined) ->
    {keep, 0};
journal_minus_segment1({?PUB = Pub, del, ack},     {Pub, no_del, no_ack}) ->
    {{no_pub, del, ack}, 1};
journal_minus_segment1({?PUB = Pub, del, ack},     {Pub, del, no_ack}) ->
    {{no_pub, no_del, ack}, 1};

%% Just deliver in journal
journal_minus_segment1({no_pub, del, no_ack},      {?PUB, no_del, no_ack}) ->
    {keep, 0};
journal_minus_segment1({no_pub, del, no_ack},      {?PUB, del, no_ack}) ->
    {undefined, 0};

%% Just ack in journal
journal_minus_segment1({no_pub, no_del, ack},      {?PUB, del, no_ack}) ->
    {keep, 0};
journal_minus_segment1({no_pub, no_del, ack},      {?PUB, del, ack}) ->
    {undefined, -1};

%% Deliver and ack in journal
journal_minus_segment1({no_pub, del, ack},         {?PUB, no_del, no_ack}) ->
    {keep, 0};
journal_minus_segment1({no_pub, del, ack},         {?PUB, del, no_ack}) ->
    {{no_pub, no_del, ack}, 0};
journal_minus_segment1({no_pub, del, ack},         {?PUB, del, ack}) ->
    {undefined, -1};

%% Missing segment. If flush_journal/1 is interrupted after deleting
%% the segment but before truncating the journal we can get these
%% cases: a delivery and an acknowledgement in the journal, or just an
%% acknowledgement in the journal, but with no segment. In both cases
%% we have really forgotten the message; so ignore what's in the
%% journal.
journal_minus_segment1({no_pub, no_del, ack},      undefined) ->
    {undefined, 0};
journal_minus_segment1({no_pub, del, ack},         undefined) ->
    {undefined, 0}.

%%----------------------------------------------------------------------------
%% upgrade
%%----------------------------------------------------------------------------

add_queue_ttl() ->
    foreach_queue_index({fun add_queue_ttl_journal/1,
                         fun add_queue_ttl_segment/1}).

add_queue_ttl_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
add_queue_ttl_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        MsgId:?MSG_ID_BYTES/binary, Rest/binary>>) ->
    {[<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, MsgId,
      expiry_to_binary(undefined)], Rest};
add_queue_ttl_journal(_) ->
    stop.

add_queue_ttl_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                        RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BYTES/binary,
                        Rest/binary>>) ->
    {[<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS>>,
      MsgId, expiry_to_binary(undefined)], Rest};
add_queue_ttl_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
add_queue_ttl_segment(_) ->
    stop.

avoid_zeroes() ->
    foreach_queue_index({none, fun avoid_zeroes_segment/1}).

avoid_zeroes_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS,  IsPersistentNum:1,
                       RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                       Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS>>, Rest};
avoid_zeroes_segment(<<0:?REL_SEQ_ONLY_PREFIX_BITS,
                       RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
avoid_zeroes_segment(_) ->
    stop.

%% At upgrade time we just define every message's size as 0 - that
%% will save us a load of faff with the message store, and means we
%% can actually use the clean recovery terms in VQ. It does mean we
%% don't count message bodies from before the migration, but we can
%% live with that.
store_msg_size() ->
    foreach_queue_index({fun store_msg_size_journal/1,
                         fun store_msg_size_segment/1}).

store_msg_size_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                        Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_size_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                         MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS,
                         Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_journal(_) ->
    stop.

store_msg_size_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                         RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                         Expiry:?EXPIRY_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, 0:?SIZE_BITS>>, Rest};
store_msg_size_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                        RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_size_segment(_) ->
    stop.

store_msg() ->
    foreach_queue_index({fun store_msg_journal/1,
                         fun store_msg_segment/1}).

store_msg_journal(<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?DEL_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    Rest/binary>>) ->
    {<<?ACK_JPREFIX:?JPREFIX_BITS, SeqId:?SEQ_BITS>>, Rest};
store_msg_journal(<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS,
                    MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
                    Rest/binary>>) ->
    {<<Prefix:?JPREFIX_BITS, SeqId:?SEQ_BITS, MsgId:?MSG_ID_BITS,
       Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_journal(_) ->
    stop.

store_msg_segment(<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1,
                    RelSeq:?REL_SEQ_BITS, MsgId:?MSG_ID_BITS,
                    Expiry:?EXPIRY_BITS, Size:?SIZE_BITS, Rest/binary>>) ->
    {<<?PUB_PREFIX:?PUB_PREFIX_BITS, IsPersistentNum:1, RelSeq:?REL_SEQ_BITS,
       MsgId:?MSG_ID_BITS, Expiry:?EXPIRY_BITS, Size:?SIZE_BITS,
       0:?EMBEDDED_SIZE_BITS>>, Rest};
store_msg_segment(<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS,
                    RelSeq:?REL_SEQ_BITS, Rest/binary>>) ->
    {<<?REL_SEQ_ONLY_PREFIX:?REL_SEQ_ONLY_PREFIX_BITS, RelSeq:?REL_SEQ_BITS>>,
     Rest};
store_msg_segment(_) ->
    stop.



%%----------------------------------------------------------------------------
%% Migration functions
%%----------------------------------------------------------------------------

foreach_queue_index(Funs) ->
    QueueDirNames = all_queue_directory_names(),
    {ok, Gatherer} = gatherer:start_link(),
    [begin
         ok = gatherer:fork(Gatherer),
         ok = worker_pool:submit_async(
                fun () ->
                        transform_queue(QueueDirName, Gatherer, Funs)
                end)
     end || QueueDirName <- QueueDirNames],
    empty = gatherer:out(Gatherer),
    unlink(Gatherer),
    ok = gatherer:stop(Gatherer).

transform_queue(Dir, Gatherer, {JournalFun, SegmentFun}) ->
    ok = transform_file(filename:join(Dir, ?JOURNAL_FILENAME), JournalFun),
    [ok = transform_file(filename:join(Dir, Seg), SegmentFun)
     || Seg <- rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir)],
    ok = gatherer:finish(Gatherer).

transform_file(_Path, none) ->
    ok;
transform_file(Path, Fun) when is_function(Fun)->
    PathTmp = Path ++ ".upgrade",
    case rabbit_file:file_size(Path) of
        0    -> ok;
        Size -> {ok, PathTmpHdl} =
                    file_handle_cache:open_with_absolute_path(
                      PathTmp, ?WRITE_MODE,
                      [{write_buffer, infinity}]),

                {ok, PathHdl} = file_handle_cache:open_with_absolute_path(
                                  Path, ?READ_MODE, [{read_buffer, Size}]),
                {ok, Content} = file_handle_cache:read(PathHdl, Size),
                ok = file_handle_cache:close(PathHdl),

                ok = drive_transform_fun(Fun, PathTmpHdl, Content),

                ok = file_handle_cache:close(PathTmpHdl),
                ok = rabbit_file:rename(PathTmp, Path)
    end.

drive_transform_fun(Fun, Hdl, Contents) ->
    case Fun(Contents) of
        stop                -> ok;
        {Output, Contents1} -> ok = file_handle_cache:append(Hdl, Output),
                               drive_transform_fun(Fun, Hdl, Contents1)
    end.

move_to_per_vhost_stores(#resource{} = QueueName) ->
    OldQueueDir = filename:join([queues_base_dir(), "queues",
                                 queue_name_to_dir_name_legacy(QueueName)]),
    NewQueueDir = queue_dir(QueueName),
    rabbit_log_upgrade:info("About to migrate queue directory '~s' to '~s'",
                            [OldQueueDir, NewQueueDir]),
    case rabbit_file:is_dir(OldQueueDir) of
        true  ->
            ok = rabbit_file:ensure_dir(NewQueueDir),
            ok = rabbit_file:rename(OldQueueDir, NewQueueDir),
            ok = ensure_queue_name_stub_file(NewQueueDir, QueueName);
        false ->
            Msg  = "Queue index directory '~s' not found for ~s~n",
            Args = [OldQueueDir, rabbit_misc:rs(QueueName)],
            rabbit_log_upgrade:error(Msg, Args),
            rabbit_log:error(Msg, Args)
    end,
    ok.

ensure_queue_name_stub_file(Dir, #resource{virtual_host = VHost, name = QName}) ->
    QueueNameFile = filename:join(Dir, ?QUEUE_NAME_STUB_FILE),
    file:write_file(QueueNameFile, <<"VHOST: ", VHost/binary, "\n",
                                     "QUEUE: ", QName/binary, "\n">>).

read_global_recovery_terms(DurableQueueNames) ->
    ok = rabbit_recovery_terms:open_global_table(),

    DurableTerms =
        lists:foldl(
          fun(QName, RecoveryTerms) ->
                  DirName = queue_name_to_dir_name_legacy(QName),
                  RecoveryInfo = case rabbit_recovery_terms:read_global(DirName) of
                                     {error, _}  -> non_clean_shutdown;
                                     {ok, Terms} -> Terms
                                 end,
                  [RecoveryInfo | RecoveryTerms]
          end, [], DurableQueueNames),

    ok = rabbit_recovery_terms:close_global_table(),
    %% The backing queue interface requires that the queue recovery terms
    %% which come back from start/1 are in the same order as DurableQueueNames
    OrderedTerms = lists:reverse(DurableTerms),
    {OrderedTerms, {fun queue_index_walker/1, {start, DurableQueueNames}}}.

cleanup_global_recovery_terms() ->
    rabbit_file:recursive_delete([filename:join([queues_base_dir(), "queues"])]),
    rabbit_recovery_terms:delete_global_table(),
    ok.


update_recovery_term(#resource{virtual_host = VHost} = QueueName, Term) ->
    Key = queue_name_to_dir_name(QueueName),
    rabbit_recovery_terms:store(VHost, Key, Term).
