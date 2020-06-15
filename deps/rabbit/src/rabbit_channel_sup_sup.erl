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

-module(rabbit_channel_sup_sup).

%% Supervisor for AMQP 0-9-1 channels. Every AMQP 0-9-1 connection has
%% one of these.
%%
%% See also rabbit_channel_sup, rabbit_connection_helper_sup, rabbit_reader.

-behaviour(supervisor2).

-export([start_link/0, start_channel/2]).

-export([init/1]).

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().
-spec start_channel(pid(), rabbit_channel_sup:start_link_args()) ->
          {'ok', pid(), {pid(), any()}}.

%%----------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link(?MODULE, []).  % 启动一个 supervisor 进程，本模块作为回调模块，[] 是 init 的参数，返回值格式 {ok, supervisor_pid}

% 启动在 init 函数中设置的子进程，参数 Pid 是 start_link 中返回的 supervisor_pid
% 实际调用 rabbit_channel_sup:start_link
start_channel(Pid, Args) ->
    supervisor2:start_child(Pid, [Args]).

%%----------------------------------------------------------------------------

% supervisor2:start_link 中会同步调用 init 方法，注意这里只会注册子进程，并不会启动
init([]) ->
    {ok, {{simple_one_for_one, 0, 1}, % 监控策略，通过 simple_one_for_one 监控的子进程拥有相同的 MF，并通过 start_child 进行启动
          % channel_sup 是一个原子类型的标签，将来可以用它指代工作进程
          % {Mod, Func, ArgList}
          [{channel_sup, {rabbit_channel_sup, start_link, []},
            temporary, infinity, supervisor, [rabbit_channel_sup]}]}}.
