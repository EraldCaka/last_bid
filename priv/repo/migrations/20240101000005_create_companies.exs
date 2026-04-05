defmodule LastBid.Repo.Migrations.CreateCompanies do
  use Ecto.Migration

  def change do
    create table(:companies, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :match_id, references(:matches, type: :binary_id, on_delete: :delete_all), null: false
      add :name, :string, null: false
      add :ticker, :string, null: false
      add :base_price, :decimal, null: false
      add :current_price, :decimal, null: false
      add :volatility, :float, null: false, default: 0.1
      add :regulatory_heat, :integer, null: false, default: 0

      timestamps(type: :utc_datetime)
    end

    create unique_index(:companies, [:match_id, :ticker])
    create index(:companies, [:match_id])

    execute """
              ALTER TABLE companies ADD CONSTRAINT companies_price_positive
              CHECK (base_price > 0 AND current_price > 0)
            """,
            "ALTER TABLE companies DROP CONSTRAINT companies_price_positive"
  end
end
