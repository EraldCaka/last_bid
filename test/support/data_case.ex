defmodule LastBid.DataCase do
  @moduledoc """
  Test case template for context/schema tests.
  Wraps tests in a SQL sandbox transaction.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      alias LastBid.Repo
      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import LastBid.DataCase
    end
  end

  setup tags do
    LastBid.DataCase.setup_sandbox(tags)
    :ok
  end

  def setup_sandbox(tags) do
    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(LastBid.Repo, shared: not tags[:async])
    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)
  end

  @doc "Create errors map from a changeset."
  def errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end
end
