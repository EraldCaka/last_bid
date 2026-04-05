defmodule LastBid.MatchEngine.Domain.MatchState do
  @moduledoc """
  Pure struct representing the full in-memory state of an active match.

  This is owned by MatchServer. Never send it raw to clients.
  Use Serializer.public_match_view/1 or Serializer.private_player_view/2.
  """

  alias LastBid.MatchEngine.Domain.{PlayerState, CompanyState}

  @phases ~w(news action_submission negotiation resolution disclosure finished)a
  @total_rounds 8

  @type phase ::
          :news
          | :action_submission
          | :negotiation
          | :resolution
          | :disclosure
          | :finished

  @type t :: %__MODULE__{
          match_id: String.t(),
          round: non_neg_integer(),
          phase: phase(),
          total_rounds: pos_integer(),
          # %{user_id => PlayerState.t()}
          players: map(),
          # %{ticker => CompanyState.t()}
          companies: map(),
          # Current round's submitted actions (cleared after resolution)
          pending_actions: list(),
          # Public events for the current round (news + resolution results)
          public_events: list(),
          # Negotiation ends at this datetime
          negotiation_deadline: DateTime.t() | nil
        }

  defstruct [
    :match_id,
    :negotiation_deadline,
    round: 0,
    phase: :news,
    total_rounds: @total_rounds,
    players: %{},
    companies: %{},
    pending_actions: [],
    public_events: []
  ]

  @doc "Build initial MatchState from match record, players, and companies."
  def build(match, match_players_with_users, companies) do
    players =
      Map.new(match_players_with_users, fn {mp, user} ->
        {user.id, PlayerState.from_match_player(mp, user)}
      end)

    company_map = Map.new(companies, fn c -> {c.ticker, CompanyState.from_company(c)} end)

    %__MODULE__{
      match_id: match.id,
      round: 1,
      phase: :news,
      total_rounds: match.total_rounds,
      players: players,
      companies: company_map,
      pending_actions: [],
      public_events: []
    }
  end

  @doc "All valid phases in order."
  def phases, do: @phases

  @doc "Is this phase valid?"
  def valid_phase?(phase), do: phase in @phases

  @doc "Is the match accepting action submissions?"
  def accepting_actions?(%__MODULE__{phase: :action_submission}), do: true
  def accepting_actions?(_), do: false

  @doc "Is the match in negotiation (public chat open)?"
  def in_negotiation?(%__MODULE__{phase: :negotiation}), do: true
  def in_negotiation?(_), do: false

  @doc "Is the match finished?"
  def finished?(%__MODULE__{phase: :finished}), do: true
  def finished?(%__MODULE__{round: r, total_rounds: t}) when r > t, do: true
  def finished?(_), do: false

  @doc "Have all players submitted their actions for this round?"
  def all_submitted?(%__MODULE__{players: players}) do
    Enum.all?(players, fn {_, player} -> player.has_submitted end)
  end
end
