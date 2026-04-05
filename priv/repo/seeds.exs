# Seeds for development.
# Run with: mix run priv/repo/seeds.exs

alias LastBid.{Accounts, Lobby}

IO.puts("Seeding development data...")

# Create two test users
{:ok, alice} =
  Accounts.register_user(%{
    email: "alice@example.com",
    username: "alice_fund",
    password: "supersecret123!"
  })

IO.puts("Created user: #{alice.username}")

{:ok, bob} =
  Accounts.register_user(%{
    email: "bob@example.com",
    username: "bob_quant",
    password: "supersecret123!"
  })

IO.puts("Created user: #{bob.username}")

# Create a test match
{:ok, match} = Lobby.create_match(alice, %{"name" => "Test Match Alpha"})
IO.puts("Created match: #{match.name} (#{match.id})")

{:ok, _} = Lobby.join_match(bob, match.id)
IO.puts("Bob joined match")

IO.puts("Done. Login at http://localhost:4000/users/login")
IO.puts("  alice@example.com / supersecret123!")
IO.puts("  bob@example.com   / supersecret123!")
