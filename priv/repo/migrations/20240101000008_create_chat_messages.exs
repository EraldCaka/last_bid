defmodule LastBid.Repo.Migrations.CreateChatMessages do
  use Ecto.Migration

  def change do
    create table(:chat_messages, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :match_id, references(:matches, type: :binary_id, on_delete: :delete_all), null: false
      add :sender_id, references(:users, type: :binary_id, on_delete: :restrict), null: false
      add :recipient_id, references(:users, type: :binary_id, on_delete: :nilify_all)
      add :content, :string, null: false
      add :message_type, :string, null: false, default: "public"
      add :round_number, :integer

      timestamps(type: :utc_datetime)
    end

    create index(:chat_messages, [:match_id, :inserted_at])
    create index(:chat_messages, [:sender_id])

    execute """
              ALTER TABLE chat_messages ADD CONSTRAINT chat_messages_type_check
              CHECK (message_type IN ('public', 'whisper', 'system'))
            """,
            "ALTER TABLE chat_messages DROP CONSTRAINT chat_messages_type_check"

    # Whisper requires a recipient
    execute """
              ALTER TABLE chat_messages ADD CONSTRAINT chat_messages_whisper_has_recipient
              CHECK (message_type != 'whisper' OR recipient_id IS NOT NULL)
            """,
            "ALTER TABLE chat_messages DROP CONSTRAINT chat_messages_whisper_has_recipient"
  end
end
