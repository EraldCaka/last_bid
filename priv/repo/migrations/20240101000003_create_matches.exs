defmodule LastBid.Repo.Migrations.CreateMatches do
  use Ecto.Migration

  def change do
    create table(:matches, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :name, :string, null: false
      add :status, :string, null: false, default: "waiting"
      add :current_round, :integer, null: false, default: 0
      add :current_phase, :string
      add :max_players, :integer, null: false, default: 4
      add :total_rounds, :integer, null: false, default: 8
      add :host_id, references(:users, type: :binary_id, on_delete: :nilify_all)

      timestamps(type: :utc_datetime)
    end

    create index(:matches, [:host_id])
    create index(:matches, [:status])

    execute """
              ALTER TABLE matches ADD CONSTRAINT matches_status_check
              CHECK (status IN ('waiting', 'active', 'finished', 'abandoned'))
            """,
            "ALTER TABLE matches DROP CONSTRAINT matches_status_check"

    execute """
              ALTER TABLE matches ADD CONSTRAINT matches_round_check
              CHECK (current_round >= 0 AND current_round <= total_rounds)
            """,
            "ALTER TABLE matches DROP CONSTRAINT matches_round_check"
  end
end
