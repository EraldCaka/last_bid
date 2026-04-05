defmodule LastBid.Repo.Migrations.CreateUsers do
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS citext", "DROP EXTENSION IF EXISTS citext"

    create table(:users, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :email, :string, null: false
      add :username, :citext, null: false
      add :hashed_password, :string, null: false
      add :role, :string, null: false, default: "player"
      add :confirmed_at, :utc_datetime

      timestamps(type: :utc_datetime)
    end

    create unique_index(:users, [:email])
    create unique_index(:users, [:username])

    execute "ALTER TABLE users ADD CONSTRAINT users_role_check CHECK (role IN ('player', 'admin', 'moderator'))",
            "ALTER TABLE users DROP CONSTRAINT users_role_check"
  end
end
