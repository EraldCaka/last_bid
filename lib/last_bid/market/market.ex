defmodule LastBid.Market do
  @moduledoc """
  Market context.

  Manages companies and persists price snapshots after round resolution.
  """

  import Ecto.Query
  alias LastBid.Repo
  alias LastBid.Market.Company

  @doc "List companies for a match."
  def list_companies(match_id) do
    Repo.all(from c in Company, where: c.match_id == ^match_id, order_by: c.ticker)
  end

  @doc "Get a company by id."
  def get_company!(id), do: Repo.get!(Company, id)

  @doc "Get a company by match + ticker."
  def get_company_by_ticker(match_id, ticker) do
    Repo.get_by(Company, match_id: match_id, ticker: ticker)
  end

  @doc "Update the current price for a company."
  def update_price(%Company{} = company, new_price) do
    company
    |> Ecto.Changeset.change(current_price: new_price)
    |> Repo.update()
  end

  @doc "Bulk update company prices from a map of %{ticker => new_price}."
  def bulk_update_prices(match_id, price_map) do
    Enum.each(price_map, fn {ticker, price} ->
      case get_company_by_ticker(match_id, ticker) do
        nil -> :skip
        company -> update_price(company, price)
      end
    end)

    :ok
  end
end
