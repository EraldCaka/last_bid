defmodule LastBid.Repo.Migrations.CreateMatchPlayers do
  use Ecto.Migration

  def change do
    create table(:match_players, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :match_id, references(:matches, type: :binary_id, on_delete: :delete_all), null: false
      add :user_id, references(:users, type: :binary_id, on_delete: :restrict), null: false
      add :faction, :string
      add :seat_number, :integer, null: false
      add :ready, :boolean, null: false, default: false

      timestamps(type: :utc_datetime)
    end

    create unique_index(:match_players, [:match_id, :user_id])
    create unique_index(:match_players, [:match_id, :seat_number])
    create index(:match_players, [:user_id])
  end
end
