defmodule LastBidWeb.MatchChannelTest do
  use LastBidWeb.ChannelCase

  alias LastBid.{Accounts, Lobby}

  setup do
    # Register two users
    {:ok, alice} = Accounts.register_user(%{
      email: "alice@test.com",
      username: "alice_test",
      password: "alicespassword1!"
    })

    {:ok, bob} = Accounts.register_user(%{
      email: "bob@test.com",
      username: "bob_test",
      password: "bobspassword123!"
    })

    # Create a match with alice as host
    {:ok, match} = Lobby.create_match(alice, %{"name" => "Test Match"})

    # Bob joins
    {:ok, _} = Lobby.join_match(bob, match.id)

    %{alice: alice, bob: bob, match: match}
  end

  describe "join" do
    test "player can join their match channel", %{alice: alice, match: match} do
      token = user_auth_token(alice)
      {:ok, socket} = connect(LastBidWeb.UserSocket, %{"token" => token})

      assert {:ok, _, _socket} =
               subscribe_and_join(socket, LastBidWeb.MatchChannel, "match:#{match.id}")
    end

    test "non-player cannot join a match channel", %{match: match} do
      {:ok, stranger} = Accounts.register_user(%{
        email: "stranger@test.com",
        username: "stranger_test",
        password: "strangerpass12!"
      })

      token = user_auth_token(stranger)
      {:ok, socket} = connect(LastBidWeb.UserSocket, %{"token" => token})

      assert {:error, %{reason: "unauthorized"}} =
               subscribe_and_join(socket, LastBidWeb.MatchChannel, "match:#{match.id}")
    end

    test "cannot join a channel with made-up match_id", %{alice: alice} do
      token = user_auth_token(alice)
      {:ok, socket} = connect(LastBidWeb.UserSocket, %{"token" => token})

      assert {:error, %{reason: "unauthorized"}} =
               subscribe_and_join(socket, LastBidWeb.MatchChannel, "match:00000000-0000-0000-0000-000000000000")
    end
  end

  describe "channel authorization" do
    test "unauthenticated socket cannot connect" do
      assert :error = connect(LastBidWeb.UserSocket, %{"token" => "invalid_token"})
    end

    test "socket with no token cannot connect" do
      assert :error = connect(LastBidWeb.UserSocket, %{})
    end
  end
end
