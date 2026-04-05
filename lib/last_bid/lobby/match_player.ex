defmodule LastBid.Lobby.MatchPlayer do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @factions ~w(activist_fund quant_predator shell_network media_syndicate distressed_debt regulatory_fixer)

  schema "match_players" do
    field :faction, :string
    field :seat_number, :integer
    field :ready, :boolean, default: false

    belongs_to :match, LastBid.Lobby.Match
    belongs_to :user, LastBid.Accounts.User

    timestamps(type: :utc_datetime)
  end

  @doc "Changeset for adding a player to a match."
  def join_changeset(match_player, attrs) do
    match_player
    |> cast(attrs, [:match_id, :user_id, :seat_number, :faction])
    |> validate_required([:match_id, :user_id, :seat_number])
    |> validate_inclusion(:faction, @factions)
    |> unique_constraint([:match_id, :user_id], name: :match_players_match_id_user_id_index)
    |> unique_constraint([:match_id, :seat_number], name: :match_players_match_id_seat_number_index)
  end

  @doc "All valid player factions."
  def factions, do: @factions
end
