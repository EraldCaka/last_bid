defmodule LastBid.MatchEngine.Domain.CompanyState do
  @moduledoc """
  Pure struct representing a company's in-memory state during a match.
  """

  @type t :: %__MODULE__{
          ticker: String.t(),
          name: String.t(),
          price: Decimal.t(),
          base_price: Decimal.t(),
          volatility: float(),
          regulatory_heat: non_neg_integer(),
          # Pending hype/leak effects accumulated this round (cleared after resolution)
          pending_hype: integer(),
          pending_leak: integer()
        }

  defstruct [
    :ticker,
    :name,
    :price,
    :base_price,
    volatility: 0.1,
    regulatory_heat: 0,
    pending_hype: 0,
    pending_leak: 0
  ]

  @doc "Build a CompanyState from a Market.Company schema."
  def from_company(company) do
    %__MODULE__{
      ticker: company.ticker,
      name: company.name,
      price: company.current_price,
      base_price: company.base_price,
      volatility: company.volatility,
      regulatory_heat: company.regulatory_heat
    }
  end

  @doc "Apply a percentage price delta (e.g., 0.05 = +5%)."
  def apply_price_delta(%__MODULE__{price: price} = company, delta_pct) do
    change = Decimal.mult(price, Decimal.from_float(delta_pct))
    new_price = Decimal.add(price, change) |> Decimal.max(Decimal.new("1"))
    %{company | price: new_price}
  end

  @doc "Clear pending hype/leak effects after resolution."
  def clear_pending(%__MODULE__{} = company) do
    %{company | pending_hype: 0, pending_leak: 0}
  end

  @doc "A map from ticker to current price, suitable for net-worth calculations."
  def price_map(companies) when is_list(companies) do
    Map.new(companies, fn c -> {c.ticker, c.price} end)
  end

  def price_map(companies) when is_map(companies) do
    Map.new(companies, fn {ticker, c} -> {ticker, c.price} end)
  end
end
