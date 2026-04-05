defmodule LastBid.Chat do
  @moduledoc """
  Chat context.

  Handles persisting and retrieving chat messages.
  Authorization checks belong in the channel layer — this context
  trusts that callers have already verified permissions.
  """

  import Ecto.Query
  alias LastBid.Repo
  alias LastBid.Chat.Message
  alias LastBid.Audit

  @page_size 50

  @doc "Post a public message. Returns {:ok, message} or {:error, changeset}."
  def post_public_message(attrs) do
    result =
      %Message{}
      |> Message.public_changeset(attrs)
      |> Repo.insert()

    case result do
      {:ok, msg} ->
        Audit.log(:chat_message_sent, %{type: "public"},
          match_id: msg.match_id,
          user_id: msg.sender_id
        )

        # Preload sender so channel formatting (which expects msg.sender.username)
        # won't fail when broadcasting the newly inserted message.
        msg = Repo.preload(msg, :sender)

        {:ok, msg}

      error ->
        error
    end
  end

  @doc """
  Post a whisper message from sender to recipient.

  The caller must verify that both sender and recipient are in the same match
  before calling this function.
  """
  def post_whisper(attrs) do
    result =
      %Message{}
      |> Message.whisper_changeset(attrs)
      |> Repo.insert()

    case result do
      {:ok, msg} ->
        Audit.log(:chat_message_sent, %{type: "whisper"},
          match_id: msg.match_id,
          user_id: msg.sender_id
        )

        {:ok, msg}

      error ->
        error
    end
  end

  @doc "Post a system-generated message (phase transitions, round results, etc.)."
  def post_system_message(match_id, content, round_number \\ nil) do
    %Message{}
    |> Message.system_changeset(%{
      match_id: match_id,
      content: content,
      round_number: round_number
    })
    |> Repo.insert()
  end

  @doc "List recent public messages for a match. Paginated."
  def list_public_messages(match_id, page \\ 1) do
    offset = (page - 1) * @page_size

    Repo.all(
      from(m in Message,
        where: m.match_id == ^match_id and m.message_type in ["public", "system"],
        order_by: [asc: m.inserted_at],
        limit: ^@page_size,
        offset: ^offset,
        preload: [:sender]
      )
    )
  end

  @doc """
  List whisper messages between two users in a match.

  Only the sender and recipient should be able to call this.
  """
  def list_whispers(match_id, user_id_a, user_id_b) do
    Repo.all(
      from(m in Message,
        where:
          m.match_id == ^match_id and
            m.message_type == "whisper" and
            ((m.sender_id == ^user_id_a and m.recipient_id == ^user_id_b) or
               (m.sender_id == ^user_id_b and m.recipient_id == ^user_id_a)),
        order_by: [asc: m.inserted_at],
        preload: [:sender]
      )
    )
  end
end
