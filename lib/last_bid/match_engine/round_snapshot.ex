defmodule LastBid.MatchEngine.RoundSnapshot do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  schema "round_snapshots" do
    field :round_number, :integer
    # Public state that can be shown to all players
    field :public_state, :map
    # Full state for replay/audit only — never sent to clients
    field :full_state, :map

    belongs_to :match, LastBid.Lobby.Match

    timestamps(type: :utc_datetime)
  end

  def create_changeset(snapshot, attrs) do
    snapshot
    |> cast(attrs, [:match_id, :round_number, :public_state, :full_state])
    |> validate_required([:match_id, :round_number, :public_state])
    |> unique_constraint([:match_id, :round_number])
  end
end
