defmodule LastBid.Repo.Migrations.CreateSubmittedActions do
  use Ecto.Migration

  def change do
    create table(:submitted_actions, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :match_id, references(:matches, type: :binary_id, on_delete: :delete_all), null: false
      add :user_id, references(:users, type: :binary_id, on_delete: :restrict), null: false
      add :round_number, :integer, null: false
      add :action_type, :string, null: false
      add :payload, :map, null: false, default: "{}"
      add :resolved_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end

    create index(:submitted_actions, [:match_id, :round_number])
    create index(:submitted_actions, [:user_id])

    execute """
              ALTER TABLE submitted_actions ADD CONSTRAINT submitted_actions_type_check
              CHECK (action_type IN (
                'buy', 'short', 'leak', 'hype',
                'freeze_liquidity', 'report_to_regulator', 'acquire_stake'
              ))
            """,
            "ALTER TABLE submitted_actions DROP CONSTRAINT submitted_actions_type_check"
  end
end
