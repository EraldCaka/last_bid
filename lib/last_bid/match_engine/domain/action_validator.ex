defmodule LastBid.MatchEngine.Domain.ActionValidator do
  @moduledoc """
  Pure validation of submitted actions against the current match state.

  Never trust the client's claimed game state. All validation runs on
  the server-side MatchState.
  """

  alias LastBid.MatchEngine.Domain.MatchState

  @action_types ~w(buy short leak hype freeze_liquidity report_to_regulator acquire_stake)a

  @doc """
  Validate an action against the current MatchState.

  Returns {:ok, normalized_params} or {:error, reason}.
  """
  def validate(%MatchState{} = state, user_id, action_type, params)
      when action_type in @action_types do
    with :ok <- check_player_exists(state, user_id),
         :ok <- check_phase(state),
         :ok <- check_not_already_submitted(state, user_id),
         :ok <- check_liquidity(state, user_id),
         {:ok, normalized} <- validate_action(state, user_id, action_type, params) do
      {:ok, normalized}
    end
  end

  def validate(_state, _user_id, action_type, _params) do
    {:error, {:invalid_action_type, action_type}}
  end

  # Guards

  defp check_player_exists(%MatchState{players: players}, user_id) do
    if Map.has_key?(players, user_id), do: :ok, else: {:error, :not_a_player}
  end

  defp check_phase(%MatchState{phase: :action_submission}), do: :ok
  defp check_phase(%MatchState{phase: phase}), do: {:error, {:wrong_phase, phase}}

  defp check_not_already_submitted(%MatchState{players: players}, user_id) do
    player = Map.fetch!(players, user_id)
    if player.has_submitted, do: {:error, :already_submitted}, else: :ok
  end

  defp check_liquidity(%MatchState{players: players}, user_id) do
    player = Map.fetch!(players, user_id)
    if player.liquidity_frozen, do: {:error, :liquidity_frozen}, else: :ok
  end

  # Per-action validations

  defp validate_action(state, user_id, :buy, params) do
    with {:ok, ticker} <- require_ticker(params, state),
         {:ok, quantity} <- require_positive_integer(params, "quantity"),
         :ok <- check_cash(state, user_id, ticker, quantity) do
      {:ok, %{action: :buy, ticker: ticker, quantity: quantity}}
    end
  end

  defp validate_action(state, user_id, :short, params) do
    with {:ok, ticker} <- require_ticker(params, state),
         {:ok, quantity} <- require_positive_integer(params, "quantity"),
         :ok <- check_short_limit(state, user_id, ticker, quantity) do
      {:ok, %{action: :short, ticker: ticker, quantity: quantity}}
    end
  end

  defp validate_action(state, _user_id, :leak, params) do
    with {:ok, ticker} <- require_ticker(params, state),
         {:ok, target_id} <- require_user_id(params, state) do
      {:ok, %{action: :leak, ticker: ticker, target_user_id: target_id}}
    end
  end

  defp validate_action(state, _user_id, :hype, params) do
    with {:ok, ticker} <- require_ticker(params, state) do
      {:ok, %{action: :hype, ticker: ticker}}
    end
  end

  defp validate_action(state, _user_id, :freeze_liquidity, params) do
    with {:ok, target_id} <- require_user_id(params, state) do
      {:ok, %{action: :freeze_liquidity, target_user_id: target_id}}
    end
  end

  defp validate_action(state, _user_id, :report_to_regulator, params) do
    with {:ok, target_id} <- require_user_id(params, state) do
      {:ok, %{action: :report_to_regulator, target_user_id: target_id}}
    end
  end

  defp validate_action(state, user_id, :acquire_stake, params) do
    with {:ok, ticker} <- require_ticker(params, state),
         {:ok, quantity} <- require_positive_integer(params, "quantity"),
         :ok <- check_cash(state, user_id, ticker, quantity) do
      {:ok, %{action: :acquire_stake, ticker: ticker, quantity: quantity}}
    end
  end

  # Helpers

  defp require_ticker(%{"ticker" => ticker}, %MatchState{companies: companies}) do
    if Map.has_key?(companies, ticker), do: {:ok, ticker}, else: {:error, {:unknown_ticker, ticker}}
  end

  defp require_ticker(_, _), do: {:error, :missing_ticker}

  defp require_positive_integer(%{} = params, key) do
    case Map.get(params, key) do
      n when is_integer(n) and n > 0 -> {:ok, n}
      n when is_binary(n) -> case Integer.parse(n) do
        {v, ""} when v > 0 -> {:ok, v}
        _ -> {:error, {:invalid_quantity, n}}
      end
      _ -> {:error, {:missing_or_invalid, key}}
    end
  end

  defp require_user_id(%{"target_user_id" => tid}, %MatchState{players: players}) do
    if Map.has_key?(players, tid), do: {:ok, tid}, else: {:error, {:unknown_player, tid}}
  end

  defp require_user_id(_, _), do: {:error, :missing_target_user_id}

  defp check_cash(%MatchState{players: players, companies: companies}, user_id, ticker, quantity) do
    player = Map.fetch!(players, user_id)
    company = Map.fetch!(companies, ticker)
    cost = Decimal.mult(company.price, Decimal.new(quantity))

    if Decimal.compare(player.cash, cost) != :lt do
      :ok
    else
      {:error, :insufficient_cash}
    end
  end

  defp check_short_limit(%MatchState{players: players}, user_id, ticker, quantity) do
    player = Map.fetch!(players, user_id)
    current_shorts = Map.get(player.short_positions, ticker, 0)
    # Limit: no more than 500 shares shorted per ticker
    if current_shorts + quantity <= 500, do: :ok, else: {:error, :short_limit_exceeded}
  end
end
