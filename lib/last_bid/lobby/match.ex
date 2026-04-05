defmodule LastBid.Lobby.Match do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @statuses ~w(waiting active finished abandoned)
  @max_players 4
  @total_rounds 8

  schema "matches" do
    field :name, :string
    field :status, :string, default: "waiting"
    field :current_round, :integer, default: 0
    field :current_phase, :string
    field :max_players, :integer, default: @max_players
    field :total_rounds, :integer, default: @total_rounds

    belongs_to :host, LastBid.Accounts.User
    has_many :match_players, LastBid.Lobby.MatchPlayer
    has_many :players, through: [:match_players, :user]

    timestamps(type: :utc_datetime)
  end

  @doc "Changeset for creating a new match."
  def create_changeset(match, attrs) do
    match
    |> cast(attrs, [:name, :host_id, :max_players, :total_rounds])
    |> validate_required([:name, :host_id])
    |> validate_length(:name, min: 3, max: 60)
    |> validate_inclusion(:max_players, 2..4)
    |> validate_inclusion(:total_rounds, 1..20)
    |> put_change(:status, "waiting")
  end

  @doc "Changeset for transitioning match status."
  def status_changeset(match, status) when status in @statuses do
    change(match, status: status)
  end

  @doc "Is the match open for new players?"
  def open?(%__MODULE__{status: "waiting"}), do: true
  def open?(_), do: false

  @doc "Is the match currently active?"
  def active?(%__MODULE__{status: "active"}), do: true
  def active?(_), do: false
end
