defmodule LastBid.Lobby do
  @moduledoc """
  Lobby context.

  Handles match lifecycle: create, join, leave, list.
  All mutations are wrapped in transactions for consistency.
  """

  import Ecto.Query
  alias LastBid.Repo
  alias LastBid.Lobby.{Match, MatchPlayer}
  alias LastBid.Market.Company
  alias LastBid.Audit

  @default_companies [
    %{name: "Apex Industries", ticker: "APEX", base_price: Decimal.new("100.00")},
    %{name: "Nexus Capital", ticker: "NEXC", base_price: Decimal.new("75.50")},
    %{name: "Vortex Media", ticker: "VRTX", base_price: Decimal.new("42.00")},
    %{name: "Lumina Energy", ticker: "LMNA", base_price: Decimal.new("88.25")},
    %{name: "Stratos Finance", ticker: "STRF", base_price: Decimal.new("130.00")}
  ]

  ## Match creation

  @doc "Create a new match. Automatically adds the creator as the host/first player."
  def create_match(user, attrs \\ %{}) do
    Ecto.Multi.new()
    |> Ecto.Multi.insert(
      :match,
      Match.create_changeset(%Match{}, Map.put(attrs, "host_id", user.id))
    )
    |> Ecto.Multi.run(:player, fn repo, %{match: match} ->
      %MatchPlayer{}
      |> MatchPlayer.join_changeset(%{
        match_id: match.id,
        user_id: user.id,
        seat_number: 1
      })
      |> repo.insert()
    end)
    |> Ecto.Multi.run(:companies, fn repo, %{match: match} ->
      companies =
        Enum.map(@default_companies, fn company_attrs ->
          %Company{}
          |> Company.create_changeset(Map.put(company_attrs, :match_id, match.id))
          |> repo.insert!()
        end)

      {:ok, companies}
    end)
    |> Repo.transaction()
    |> case do
      {:ok, %{match: match}} ->
        Audit.log(:match_created, %{name: match.name}, match_id: match.id, user_id: user.id)
        {:ok, match}

      {:error, :match, changeset, _} ->
        {:error, changeset}

      {:error, :player, changeset, _} ->
        {:error, changeset}
    end
  end

  ## Match listing

  @doc "List all open matches (status: waiting)."
  def list_open_matches do
    Repo.all(
      from(m in Match,
        where: m.status == "waiting",
        preload: [:host, :match_players],
        order_by: [desc: m.inserted_at]
      )
    )
  end

  @doc "List matches for a given user."
  def list_matches_for_user(user_id) do
    Repo.all(
      from(m in Match,
        join: mp in MatchPlayer,
        on: mp.match_id == m.id and mp.user_id == ^user_id,
        where: m.status in ["waiting", "active"],
        preload: [:host, :match_players],
        order_by: [desc: m.inserted_at]
      )
    )
  end

  ## Match retrieval

  @doc "Get a match by id."
  def get_match(id), do: Repo.get(Match, id)

  @doc "Get a match by id, raising if not found."
  def get_match!(id), do: Repo.get!(Match, id)

  @doc "Get a match with players preloaded."
  def get_match_with_players!(id) do
    Match
    |> Repo.get!(id)
    |> Repo.preload(match_players: [:user])
  end

  ## Joining / leaving

  @doc "Join an open match. Returns {:ok, match_player} or {:error, reason}."
  def join_match(user, match_id) do
    # If the user is already a player in this match, return the existing record.
    case get_match_player(user.id, match_id) do
      %MatchPlayer{} = mp ->
        {:ok, mp}

      nil ->
        with {:ok, match} <- fetch_open_match(match_id),
             {:ok, seat} <- next_available_seat(match),
             {:ok, mp} <- insert_match_player(user, match, seat) do
          Audit.log(:player_joined, %{}, match_id: match.id, user_id: user.id)
          {:ok, mp}
        else
          # Propagate errors (changeset or domain errors) unchanged
          {:error, _} = err -> err
        end
    end
  end

  @doc "Leave a waiting match. Players cannot leave active matches through this path."
  def leave_match(user, match_id) do
    with {:ok, match} <- fetch_waiting_match(match_id),
         {:ok, mp} <- fetch_match_player(user.id, match.id) do
      Repo.delete!(mp)
      Audit.log(:player_left, %{}, match_id: match.id, user_id: user.id)
      {:ok, :left}
    end
  end

  @doc "Returns true if the given user_id is a player in the given match."
  def player_in_match?(user_id, match_id) do
    Repo.exists?(
      from(mp in MatchPlayer,
        where: mp.user_id == ^user_id and mp.match_id == ^match_id
      )
    )
  end

  @doc "Get a MatchPlayer by user_id and match_id."
  def get_match_player(user_id, match_id) do
    Repo.get_by(MatchPlayer, user_id: user_id, match_id: match_id)
  end

  ## Status transitions (called by MatchEngine.Runtime)

  @doc "Mark a match as active."
  def start_match!(match) do
    match
    |> Match.status_changeset("active")
    |> Repo.update!()
  end

  @doc "Mark a match as finished."
  def finish_match!(match) do
    match
    |> Match.status_changeset("finished")
    |> Repo.update!()
  end

  @doc "Mark a match as abandoned."
  def abandon_match!(match) do
    match
    |> Match.status_changeset("abandoned")
    |> Repo.update!()
  end

  ## Private helpers

  defp fetch_open_match(match_id) do
    case get_match(match_id) do
      %Match{status: "waiting"} = match ->
        {:ok, match}

      # In development only, allow joining matches that are already active
      # so you can test multiple clients locally without strict status checks.
      %Match{status: "active"} = match ->
        # Mix.env/0 cannot be used in a guard; check at runtime instead.
        if Mix.env() == :dev do
          {:ok, match}
        else
          {:error, :match_not_open}
        end

      %Match{} ->
        {:error, :match_not_open}

      nil ->
        {:error, :match_not_found}
    end
  end

  defp fetch_waiting_match(match_id) do
    case get_match(match_id) do
      %Match{status: "waiting"} = match -> {:ok, match}
      %Match{} -> {:error, :match_not_waiting}
      nil -> {:error, :match_not_found}
    end
  end

  defp fetch_match_player(user_id, match_id) do
    case get_match_player(user_id, match_id) do
      nil -> {:error, :not_a_player}
      mp -> {:ok, mp}
    end
  end

  defp next_available_seat(match) do
    players = Repo.all(from(mp in MatchPlayer, where: mp.match_id == ^match.id))

    if length(players) >= match.max_players do
      {:error, :match_full}
    else
      taken = MapSet.new(players, & &1.seat_number)
      seat = Enum.find(1..match.max_players, fn s -> s not in taken end)
      {:ok, seat}
    end
  end

  defp insert_match_player(user, match, seat) do
    %MatchPlayer{}
    |> MatchPlayer.join_changeset(%{
      match_id: match.id,
      user_id: user.id,
      seat_number: seat
    })
    |> Repo.insert()
  end
end
