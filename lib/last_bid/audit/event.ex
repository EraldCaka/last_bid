defmodule LastBid.Audit.Event do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @event_types ~w(
    match_created match_started match_finished match_abandoned
    player_joined player_left
    round_started round_resolved
    action_submitted action_resolved
    chat_moderated
    admin_action
  )

  schema "audit_events" do
    field :event_type, :string
    field :payload, :map
    field :round_number, :integer

    belongs_to :match, LastBid.Lobby.Match
    belongs_to :user, LastBid.Accounts.User

    timestamps(type: :utc_datetime, updated_at: false)
  end

  def create_changeset(event, attrs) do
    event
    |> cast(attrs, [:event_type, :payload, :match_id, :user_id, :round_number])
    |> validate_required([:event_type, :payload])
    |> validate_inclusion(:event_type, @event_types)
  end
end
