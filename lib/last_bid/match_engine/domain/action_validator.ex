defmodule LastBid.MatchEngine.Domain.ActionValidator do
  @moduledoc false

  alias LastBid.MatchEngine.Domain.MatchState

  @quantity_actions [:buy, :sell, :short, :acquire_stake]
  @ticker_actions [:buy, :sell, :short, :hype, :leak, :acquire_stake]
  @target_actions [:freeze_liquidity, :report_to_regulator, :leak]

  def validate(%MatchState{phase: phase}, _user_id, _action_type, _params)
      when phase != :action_submission do
    {:error, :invalid_phase}
  end

  def validate(%MatchState{} = state, user_id, action_type, params) when is_binary(action_type) do
    validate(state, user_id, String.to_existing_atom(action_type), params)
  rescue
    ArgumentError -> {:error, :invalid_action_type}
  end

  def validate(%MatchState{} = state, user_id, action_type, params) when is_atom(action_type) do
    with {:ok, player} <- fetch_player(state, user_id),
         :ok <- ensure_can_act(player),
         {:ok, normalized} <- normalize(state, action_type, params) do
      {:ok, Map.put(normalized, :action, action_type)}
    end
  end

  defp fetch_player(%MatchState{players: players}, user_id) do
    case Map.get(players, user_id) do
      nil -> {:error, :not_a_player}
      player -> {:ok, player}
    end
  end

  defp ensure_can_act(%{liquidity_frozen: true}), do: {:error, :liquidity_frozen}
  defp ensure_can_act(%{has_submitted: true}), do: {:error, :already_submitted}
  defp ensure_can_act(_player), do: :ok

  defp normalize(state, action_type, params) do
    with :ok <- validate_ticker_if_needed(state, action_type, params),
         :ok <- validate_target_if_needed(state, action_type, params),
         :ok <- validate_quantity_if_needed(action_type, params) do
      {:ok,
       %{}
       |> maybe_put_ticker(params)
       |> maybe_put_target(params)
       |> maybe_put_quantity(params)}
    end
  end

  defp validate_ticker_if_needed(state, action_type, params) do
    if action_type in @ticker_actions do
      ticker = param(params, :ticker)

      cond do
        is_nil(ticker) or ticker == "" ->
          {:error, :ticker_required}

        Map.has_key?(state.companies, ticker) ->
          :ok

        true ->
          {:error, :unknown_ticker}
      end
    else
      :ok
    end
  end

  defp validate_target_if_needed(state, action_type, params) do
    if action_type in @target_actions do
      target_user_id = param(params, :target_user_id)

      cond do
        is_nil(target_user_id) or target_user_id == "" ->
          {:error, :target_required}

        Map.has_key?(state.players, target_user_id) ->
          :ok

        true ->
          {:error, :unknown_target}
      end
    else
      :ok
    end
  end

  defp validate_quantity_if_needed(action_type, params) do
    if action_type in @quantity_actions do
      quantity = param(params, :quantity)

      case parse_positive_int(quantity) do
        nil -> {:error, :quantity_required}
        _ -> :ok
      end
    else
      :ok
    end
  end

  defp maybe_put_ticker(acc, params) do
    case param(params, :ticker) do
      nil -> acc
      ticker -> Map.put(acc, :ticker, ticker)
    end
  end

  defp maybe_put_target(acc, params) do
    case param(params, :target_user_id) do
      nil -> acc
      target_user_id -> Map.put(acc, :target_user_id, target_user_id)
    end
  end

  defp maybe_put_quantity(acc, params) do
    case parse_positive_int(param(params, :quantity)) do
      nil -> acc
      quantity -> Map.put(acc, :quantity, quantity)
    end
  end

  defp param(params, key) when is_map(params) do
    Map.get(params, key) || Map.get(params, Atom.to_string(key))
  end

  defp parse_positive_int(value) when is_integer(value) and value > 0, do: value

  defp parse_positive_int(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} when int > 0 -> int
      _ -> nil
    end
  end

  defp parse_positive_int(_), do: nil
end
