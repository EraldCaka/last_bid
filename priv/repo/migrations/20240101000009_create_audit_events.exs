defmodule LastBid.Repo.Migrations.CreateAuditEvents do
  use Ecto.Migration

  def change do
    create table(:audit_events, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :match_id, references(:matches, type: :binary_id, on_delete: :nilify_all)
      add :user_id, references(:users, type: :binary_id, on_delete: :nilify_all)
      add :event_type, :string, null: false
      add :payload, :map, null: false, default: "{}"
      add :round_number, :integer

      timestamps(type: :utc_datetime, updated_at: false)
    end

    create index(:audit_events, [:match_id, :inserted_at])
    create index(:audit_events, [:user_id])
    create index(:audit_events, [:event_type])
  end
end
