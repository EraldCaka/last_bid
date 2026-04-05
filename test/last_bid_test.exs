defmodule LastBidTest do
  use ExUnit.Case

  test "application module is defined" do
    assert Code.ensure_loaded?(LastBid)
  end
end
