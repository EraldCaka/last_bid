defmodule LastBidWeb.ChannelCase do
  @moduledoc """
  Test case template for channel tests.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Phoenix.ChannelTest
      import LastBidWeb.ChannelCase

      @endpoint LastBidWeb.Endpoint
    end
  end

  setup tags do
    LastBid.DataCase.setup_sandbox(tags)
    :ok
  end

  @doc "Build a signed auth token for the given user."
  def user_auth_token(user) do
    Phoenix.Token.sign(LastBidWeb.Endpoint, "user_auth", user.id)
  end
end
