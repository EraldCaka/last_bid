defmodule LastBid.Lobby do
  @moduledoc """
  Lobby context.

  Handles match lifecycle: create, join, leave, list.
  All mutations are wrapped in transactions for consistency.
  """

  import Ecto.Query
  alias Ecto.Multi
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

  @doc "Create a new match. Automatically adds the creator as the host/first player."
  def create_match(user, attrs \\ %{}) do
    Multi.new()
    |> Multi.insert(
      :match,
      Match.create_changeset(%Match{}, Map.put(attrs, "host_id", user.id))
    )
    |> Multi.run(:player, fn repo, %{match: match} ->
      %MatchPlayer{}
      |> MatchPlayer.join_changeset(%{
        match_id: match.id,
        user_id: user.id,
        seat_number: 1
      })
      |> repo.insert()
    end)
    |> Multi.run(:companies, fn repo, %{match: match} ->
      @default_companies
      |> Enum.reduce_while({:ok, []}, fn company_attrs, {:ok, acc} ->
        attrs =
          company_attrs
          |> Map.put(:match_id, match.id)
          |> Map.put(:current_price, company_attrs.base_price)

        case %Company{} |> Company.create_changeset(attrs) |> repo.insert() do
          {:ok, company} -> {:cont, {:ok, [company | acc]}}
          {:error, changeset} -> {:halt, {:error, changeset}}
        end
      end)
      |> case do
        {:ok, companies} -> {:ok, Enum.reverse(companies)}
        {:error, changeset} -> {:error, changeset}
      end
    end)
    |> Repo.transaction()
    |> case do
      {:ok, %{match: match}} ->
        Audit.log(:match_created, %{name: match.name}, match_id: match.id, user_id: user.id)
        {:ok, match}

      {:error, _step, %Ecto.Changeset{} = changeset, _changes_so_far} ->
        {:error, changeset}
    end
  end

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

  @doc "Get a match by id."
  def get_match(id), do: Repo.get(Match, id)

  @doc "Get a match by id, raising if not found."
  def get_match!(id), do: Repo.get!(Match, id)

  @doc "Get a match with host and players preloaded."
  def get_match_with_players!(id) do
    Match
    |> Repo.get!(id)
    |> Repo.preload([
      :host,
      match_players: from(mp in MatchPlayer, order_by: [asc: mp.seat_number], preload: [:user])
    ])
  end

  @doc "Join an open match. Returns {:ok, match_player} or {:error, reason}."
  def join_match(user, match_id) do
    case get_match_player(user.id, match_id) do
      %MatchPlayer{} = mp ->
        {:ok, mp}

      nil ->
        Multi.new()
        |> Multi.run(:match, fn repo, _changes ->
          case repo.one(
                 from(m in Match,
                   where: m.id == ^match_id,
                   lock: "FOR UPDATE"
                 )
               ) do
            %Match{status: "waiting"} = match ->
              {:ok, match}

            %Match{status: "active"} = match ->
              if Mix.env() == :dev, do: {:ok, match}, else: {:error, :match_not_open}

            %Match{} ->
              {:error, :match_not_open}

            nil ->
              {:error, :match_not_found}
          end
        end)
        |> Multi.run(:seat, fn repo, %{match: match} ->
          players =
            repo.all(
              from(mp in MatchPlayer,
                where: mp.match_id == ^match.id,
                select: mp.seat_number
              )
            )

          if length(players) >= match.max_players do
            {:error, :match_full}
          else
            taken = MapSet.new(players)
            seat = Enum.find(1..match.max_players, fn s -> not MapSet.member?(taken, s) end)
            {:ok, seat}
          end
        end)
        |> Multi.run(:player, fn repo, %{match: match, seat: seat} ->
          %MatchPlayer{}
          |> MatchPlayer.join_changeset(%{
            match_id: match.id,
            user_id: user.id,
            seat_number: seat
          })
          |> repo.insert()
        end)
        |> Repo.transaction()
        |> case do
          {:ok, %{player: mp, match: match}} ->
            Audit.log(:player_joined, %{}, match_id: match.id, user_id: user.id)
            {:ok, mp}

          {:error, _step, reason, _changes_so_far} ->
            {:error, normalize_join_error(reason)}
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

  defp normalize_join_error(%Ecto.Changeset{} = changeset) do
    cond do
      seat_taken_error?(changeset) -> :match_full
      duplicate_player_error?(changeset) -> :already_joined
      true -> changeset
    end
  end

  defp normalize_join_error(reason), do: reason

  defp seat_taken_error?(changeset) do
    Enum.any?(changeset.errors, fn
      {:seat_number, {"has already been taken", _}} -> true
      _ -> false
    end)
  end

  defp duplicate_player_error?(changeset) do
    Enum.any?(changeset.errors, fn
      {:user_id, {"has already been taken", _}} -> true
      _ -> false
    end)
  end
end
