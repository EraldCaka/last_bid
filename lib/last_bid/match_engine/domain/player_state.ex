defmodule LastBid.MatchEngine.Domain.PlayerState do
  @moduledoc """
  Pure struct representing a single player's in-memory state during a match.

  Never serialize this struct directly to clients — use Serializer instead.
  """

  @starting_cash Decimal.new("10000.00")

  @type t :: %__MODULE__{
          user_id: String.t(),
          username: String.t(),
          faction: atom() | nil,
          seat_number: pos_integer(),
          cash: Decimal.t(),
          # %{ticker => integer shares}
          portfolio: map(),
          # %{ticker => integer shorted}
          short_positions: map(),
          regulatory_heat: non_neg_integer(),
          liquidity_frozen: boolean(),
          has_submitted: boolean()
        }

  defstruct [
    :user_id,
    :username,
    :faction,
    :seat_number,
    cash: @starting_cash,
    portfolio: %{},
    short_positions: %{},
    regulatory_heat: 0,
    liquidity_frozen: false,
    has_submitted: false
  ]

  @doc "Build a PlayerState from a MatchPlayer and User."
  def from_match_player(match_player, user) do
    %__MODULE__{
      user_id: user.id,
      username: user.username,
      faction: match_player.faction && String.to_atom(match_player.faction),
      seat_number: match_player.seat_number,
      cash: @starting_cash,
      portfolio: %{},
      short_positions: %{},
      regulatory_heat: 0,
      liquidity_frozen: false,
      has_submitted: false
    }
  end

  @doc "Net portfolio value at current prices."
  def portfolio_value(%__MODULE__{portfolio: portfolio}, price_map) do
    Enum.reduce(portfolio, Decimal.new("0"), fn {ticker, shares}, acc ->
      price = Map.get(price_map, ticker, Decimal.new("0"))
      Decimal.add(acc, Decimal.mult(price, Decimal.new(shares)))
    end)
  end

  @doc "Net worth = cash + portfolio value - short obligations."
  def net_worth(%__MODULE__{cash: cash, short_positions: shorts} = player, price_map) do
    long_value = portfolio_value(player, price_map)

    short_liability =
      Enum.reduce(shorts, Decimal.new("0"), fn {ticker, shares}, acc ->
        price = Map.get(price_map, ticker, Decimal.new("0"))
        Decimal.add(acc, Decimal.mult(price, Decimal.new(shares)))
      end)

    cash |> Decimal.add(long_value) |> Decimal.sub(short_liability)
  end

  @doc "Mark a player as having submitted their action for the round."
  def mark_submitted(%__MODULE__{} = player), do: %{player | has_submitted: true}

  @doc "Reset submission flag for next round."
  def reset_submitted(%__MODULE__{} = player), do: %{player | has_submitted: false}
end
