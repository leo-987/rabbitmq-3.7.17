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

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckRunningCommand do
  @moduledoc """
  Exits with a non-zero code if the RabbitMQ app on the target node is not running.

  This command is meant to be used in health checks.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    # Note: we use is_booted/1 over is_running/1 to avoid
    # returning a positive result when the node is still booting
    :rabbit_misc.rpc_call(node_name, :rabbit, :is_booted, [node_name], timeout)
  end

  def output(true, %{node: node_name} = _options) do
    {:ok, "RabbitMQ on node #{node_name} is fully booted and running"}
  end

  def output(false, %{node: node_name} = _options) do
    {:error,
     "RabbitMQ on node #{node_name} is not running or has not fully booted yet (check with is_booting)"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Health check that exits with a non-zero code if the RabbitMQ app on the target node is not running"

  def usage, do: "check_running"

  def banner([], %{node: node_name}) do
    "Checking if RabbitMQ is running on node #{node_name} ..."
  end
end
