defmodule LastBid.Repo.Migrations.CreateRoundSnapshots do
  use Ecto.Migration

  def change do
    create table(:round_snapshots, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :match_id, references(:matches, type: :binary_id, on_delete: :delete_all), null: false
      add :round_number, :integer, null: false
      # Public state sent to players after resolution
      add :public_state, :map, null: false, default: "{}"
      # Full state for audits/replay — never sent to clients directly
      add :full_state, :map

      timestamps(type: :utc_datetime)
    end

    create unique_index(:round_snapshots, [:match_id, :round_number])
  end
end
