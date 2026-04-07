defmodule LastBid.Chat.Message do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @types ~w(public whisper system)
  @max_content_length 500

  schema "chat_messages" do
    field(:content, :string)
    field(:message_type, :string, default: "public")
    field(:round_number, :integer)

    belongs_to(:match, LastBid.Lobby.Match)
    belongs_to(:sender, LastBid.Accounts.User)
    belongs_to(:recipient, LastBid.Accounts.User)

    timestamps(type: :utc_datetime)
  end

  @doc "Changeset for a public chat message."
  def public_changeset(message, attrs) do
    message
    |> cast(attrs, [:match_id, :sender_id, :content, :round_number])
    |> validate_required([:match_id, :sender_id, :content])
    |> put_change(:message_type, "public")
    |> validate_content()
  end

  @doc "Changeset for a whisper (private) message."
  def whisper_changeset(message, attrs) do
    message
    |> cast(attrs, [:match_id, :sender_id, :recipient_id, :content, :round_number])
    |> validate_required([:match_id, :sender_id, :recipient_id, :content])
    |> put_change(:message_type, "whisper")
    |> validate_content()
  end

  @doc "Changeset for a system-generated message."
  def system_changeset(message, attrs) do
    message
    |> cast(attrs, [:match_id, :content, :round_number])
    |> validate_required([:match_id, :content])
    |> put_change(:message_type, "system")
    |> validate_content()
  end

  defp validate_content(changeset) do
    changeset
    |> validate_required([:content])
    |> validate_length(:content, min: 1, max: @max_content_length)
    |> sanitize_content()
  end

  defp sanitize_content(changeset) do
    case get_change(changeset, :content) do
      nil -> changeset
      content -> put_change(changeset, :content, strip_html(content))
    end
  end

  defp strip_html(content) do
    content
    |> String.replace(~r/<[^>]*>/, "")
    |> String.trim()
  end

  @doc "All valid message types."
  def types, do: @types
end
