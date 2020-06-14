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

-module(rabbit_access_control).

-include("rabbit.hrl").

-export([check_user_pass_login/2, check_user_login/2, check_user_loopback/2,
         check_vhost_access/3, check_resource_access/3, check_topic_access/4]).

%%----------------------------------------------------------------------------

-export_type([permission_atom/0]).

-type permission_atom() :: 'configure' | 'read' | 'write'.

-spec check_user_pass_login
        (rabbit_types:username(), rabbit_types:password()) ->
            {'ok', rabbit_types:user()} |
            {'refused', rabbit_types:username(), string(), [any()]}.
-spec check_user_login
        (rabbit_types:username(), [{atom(), any()}]) ->
            {'ok', rabbit_types:user()} |
            {'refused', rabbit_types:username(), string(), [any()]}.
-spec check_user_loopback
        (rabbit_types:username(), rabbit_net:socket() | inet:ip_address()) ->
            'ok' | 'not_allowed'.
-spec check_vhost_access
        (rabbit_types:user(), rabbit_types:vhost(),
         rabbit_net:socket() | #authz_socket_info{}) ->
            'ok' | rabbit_types:channel_exit().
-spec check_resource_access
        (rabbit_types:user(), rabbit_types:r(atom()), permission_atom()) ->
            'ok' | rabbit_types:channel_exit().

%%----------------------------------------------------------------------------

check_user_pass_login(Username, Password) ->
    check_user_login(Username, [{password, Password}]).

check_user_login(Username, AuthProps) ->
    {ok, Modules} = application:get_env(rabbit, auth_backends),
    R = lists:foldl(
          fun ({ModN, ModZs0}, {refused, _, _, _}) ->
                  ModZs = case ModZs0 of
                              A when is_atom(A) -> [A];
                              L when is_list(L) -> L
                          end,
                  %% Different modules for authN vs authZ. So authenticate
                  %% with authN module, then if that succeeds do
                  %% passwordless (i.e pre-authenticated) login with authZ.
                  case try_authenticate(ModN, Username, AuthProps) of
                      {ok, ModNUser = #auth_user{username = Username2}} ->
                          rabbit_log:debug("User '~s' authenticated successfully by backend ~s", [Username2, ModN]),
                          user(ModNUser, try_authorize(ModZs, Username2));
                      Else ->
                          Else
                  end;
              (Mod, {refused, _, _, _}) ->
                  %% Same module for authN and authZ. Just take the result
                  %% it gives us
                  case try_authenticate(Mod, Username, AuthProps) of
                      {ok, ModNUser = #auth_user{username = Username2, impl = Impl}} ->
                          rabbit_log:debug("User '~s' authenticated successfully by backend ~s", [Username2, Mod]),
                          user(ModNUser, {ok, [{Mod, Impl}], []});
                      Else ->
                          Else
                  end;
              (_, {ok, User}) ->
                  %% We've successfully authenticated. Skip to the end...
                  {ok, User}
          end,
          {refused, Username, "No modules checked '~s'", [Username]}, Modules),
    R.

try_authenticate(Module, Username, AuthProps) ->
    case Module:user_login_authentication(Username, AuthProps) of
        {ok, AuthUser}  -> {ok, AuthUser};
        {error, E}      -> {refused, Username,
                            "~s failed authenticating ~s: ~p~n",
                            [Module, Username, E]};
        {refused, F, A} -> {refused, Username, F, A}
    end.

try_authorize(Modules, Username) ->
    lists:foldr(
      fun (Module, {ok, ModsImpls, ModsTags}) ->
              case Module:user_login_authorization(Username) of
                  {ok, Impl, Tags}-> {ok, [{Module, Impl} | ModsImpls], ModsTags ++ Tags};
                  {ok, Impl}      -> {ok, [{Module, Impl} | ModsImpls], ModsTags};
                  {error, E}      -> {refused, Username,
                                        "~s failed authorizing ~s: ~p~n",
                                        [Module, Username, E]};
                  {refused, F, A} -> {refused, Username, F, A}
              end;
          (_,      {refused, F, A}) ->
              {refused, Username, F, A}
      end, {ok, [], []}, Modules).

user(#auth_user{username = Username, tags = Tags}, {ok, ModZImpls, ModZTags}) ->
    {ok, #user{username       = Username,
               tags           = Tags ++ ModZTags,
               authz_backends = ModZImpls}};
user(_AuthUser, Error) ->
    Error.

auth_user(#user{username = Username, tags = Tags}, Impl) ->
    % 创建一条 auth_user 记录并返回
    #auth_user{username = Username,
               tags     = Tags,
               impl     = Impl}.

check_user_loopback(Username, SockOrAddr) ->
    {ok, Users} = application:get_env(rabbit, loopback_users),
    case rabbit_net:is_loopback(SockOrAddr)
        orelse not lists:member(Username, Users) of
        true  -> ok;
        false -> not_allowed
    end.

check_vhost_access(User = #user{username       = Username,
                                authz_backends = Modules}, VHostPath, Sock) ->
    lists:foldl(
      fun({Mod, Impl}, ok) ->
              check_access(
                fun() ->
                        rabbit_vhost:exists(VHostPath) andalso
                            Mod:check_vhost_access(
                              auth_user(User, Impl), VHostPath, Sock)
                end,
                Mod, "access to vhost '~s' refused for user '~s'",
                [VHostPath, Username], not_allowed);
         (_, Else) ->
              Else
      end, ok, Modules).

check_resource_access(User, R = #resource{kind = exchange, name = <<"">>},
                      Permission) ->
    check_resource_access(User, R#resource{name = <<"amq.default">>}, % 消息中未指定 exchange 名，则默认使用名为 amq.default 的 exchange
                          Permission);
check_resource_access(User = #user{username       = Username,
                                   authz_backends = Modules},
                      Resource, Permission) ->
    lists:foldl(
      fun({Module, Impl}, ok) ->
              check_access( % 最终调用传入的 fun 检查权限，返回 true 或 false 或 error
                fun() -> Module:check_resource_access(  % 调用 rabbit_auth_backend_internal:check_resource_access
                           auth_user(User, Impl), Resource, Permission) end,
                Module, "access to ~s refused for user '~s'",
                [rabbit_misc:rs(Resource), Username]);
         (_, Else) -> Else  % fun 的子句，第一个子句只匹配 ok 的累积器，第二个子句匹配非 ok 的累积器
                            % 当没有该子句，且 check_access 返回 false 时，由于没有匹配的函数，foldl 会抛出异常
                            % 注意这里只要有一次遍历返回非 ok，整个 foldl 都返回非 ok
      end, ok, Modules).

check_topic_access(User = #user{username = Username,
                                authz_backends = Modules},  % [{rabbit_auth_backend_internal,none}] 注意是个列表
                            Resource, Permission, Context) ->
    lists:foldl(
        fun({Module, Impl}, ok) ->
            check_access( % 最终调用传入的 fun 检查权限，返回 true 或 false 或 error
                fun() -> Module:check_topic_access( % 调用 rabbit_auth_backend_internal:check_topic_access
                    auth_user(User, Impl), Resource, Permission, Context) end,
                Module, "access to topic '~s' in exchange ~s refused for user '~s'",    % 校验失败打印的错误信息
                [maps:get(routing_key, Context), rabbit_misc:rs(Resource), Username]);  % 校验失败打印的错误信息
            (_, Else) -> Else % 原理同 check_resource_access 函数
        end, ok, Modules).

check_access(Fun, Module, ErrStr, ErrArgs) ->
    check_access(Fun, Module, ErrStr, ErrArgs, access_refused).

% 校验各类资源权限都会走到这个函数，例如 topic，vhost，queue，exchange 等
% 如果校验失败，会抛出异常
check_access(Fun, Module, ErrStr, ErrArgs, ErrName) ->
    case Fun() of
        true ->
            ok;
        false ->
            rabbit_misc:protocol_error(ErrName, ErrStr, ErrArgs); % 抛出异常
        {error, E}  ->
            FullErrStr = ErrStr ++ ", backend ~s returned an error: ~p~n",
            FullErrArgs = ErrArgs ++ [Module, E],
            rabbit_log:error(FullErrStr, FullErrArgs),
            rabbit_misc:protocol_error(ErrName, FullErrStr, FullErrArgs)  % 抛出异常
    end.
