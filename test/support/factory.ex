defmodule LastBid.Factory do
  @moduledoc "ExMachina factory for test data."

  use ExMachina.Ecto, repo: LastBid.Repo

  alias LastBid.Accounts.User
  alias LastBid.Lobby.{Match, MatchPlayer}
  alias LastBid.Market.Company
  alias LastBid.Chat.Message

  def user_factory do
    %User{
      email: sequence(:email, &"user#{&1}@example.com"),
      username: sequence(:username, &"player#{&1}"),
      hashed_password: Bcrypt.hash_pwd_salt("password123456"),
      role: "player"
    }
  end

  def match_factory do
    %Match{
      name: sequence(:name, &"Match #{&1}"),
      status: "waiting",
      current_round: 0,
      max_players: 4,
      total_rounds: 8
    }
  end

  def match_player_factory do
    %MatchPlayer{
      seat_number: 1,
      faction: "activist_fund",
      ready: false
    }
  end

  def company_factory do
    %Company{
      name: sequence(:company_name, &"Company #{&1}"),
      ticker: sequence(:ticker, &"T#{&1}"),
      base_price: Decimal.new("100.00"),
      current_price: Decimal.new("100.00"),
      volatility: 0.1,
      regulatory_heat: 0
    }
  end

  def message_factory do
    %Message{
      content: "Test message",
      message_type: "public"
    }
  end
end
