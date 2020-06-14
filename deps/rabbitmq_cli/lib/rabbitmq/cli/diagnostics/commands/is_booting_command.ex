## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.IsBootingCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :is_booting, [node_name], timeout)
  end

  def output(true, %{node: node_name} = _options) do
    {:ok, "RabbitMQ on node #{node_name} is booting"}
  end

  def output(false, %{node: node_name} = _options) do
    {:ok,
     "RabbitMQ on node #{node_name} is fully booted (check with is_running), stopped or has not started booting yet"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Checks if RabbitMQ is still booting on the target node"

  def usage, do: "is_booting"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} for its boot status ..."
  end
end
