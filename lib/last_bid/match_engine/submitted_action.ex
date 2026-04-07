defmodule LastBid.MatchEngine.SubmittedAction do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @action_types ~w(buy short leak hype freeze_liquidity report_to_regulator acquire_stake)

  schema "submitted_actions" do
    field(:round_number, :integer)
    field(:action_type, :string)
    field(:payload, :map)
    field(:resolved_at, :utc_datetime)

    belongs_to(:match, LastBid.Lobby.Match)
    belongs_to(:user, LastBid.Accounts.User)

    timestamps(type: :utc_datetime)
  end

  @doc "Changeset for submitting an action. Never trust the client's claimed state."
  def submit_changeset(action, attrs) do
    action
    |> cast(attrs, [:match_id, :user_id, :round_number, :action_type, :payload])
    |> validate_required([:match_id, :user_id, :round_number, :action_type, :payload])
    |> validate_inclusion(:action_type, @action_types)
    |> validate_number(:round_number, greater_than: 0)
    |> validate_payload()
  end

  # DoS guard
  defp validate_payload(changeset) do
    case get_change(changeset, :payload) do
      nil ->
        changeset

      payload when is_map(payload) ->
        if map_size(payload) > 10 do
          add_error(changeset, :payload, "payload too large")
        else
          changeset
        end

      _ ->
        add_error(changeset, :payload, "must be a map")
    end
  end

  @doc "All valid action type atoms."
  def action_types, do: Enum.map(@action_types, &String.to_atom/1)
end
