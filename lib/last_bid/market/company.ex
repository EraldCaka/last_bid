defmodule LastBid.Market.Company do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @ticker_regex ~r/^[A-Z]{2,5}$/

  schema "companies" do
    field :name, :string
    field :ticker, :string
    field :base_price, :decimal
    field :current_price, :decimal
    field :volatility, :float, default: 0.1
    field :regulatory_heat, :integer, default: 0

    belongs_to :match, LastBid.Lobby.Match

    timestamps(type: :utc_datetime)
  end

  @doc "Changeset for creating a company in a match."
  def create_changeset(company, attrs) do
    company
    |> cast(attrs, [:name, :ticker, :base_price, :current_price, :volatility, :match_id])
    |> validate_required([:name, :ticker, :base_price, :match_id])
    |> validate_format(:ticker, @ticker_regex, message: "must be 2-5 uppercase letters")
    |> validate_number(:base_price, greater_than: Decimal.new("0"))
    |> put_current_price_from_base()
    |> unique_constraint([:match_id, :ticker], name: :companies_match_id_ticker_index)
  end

  defp put_current_price_from_base(changeset) do
    case get_change(changeset, :base_price) do
      nil -> changeset
      base -> put_change(changeset, :current_price, base)
    end
  end
end
