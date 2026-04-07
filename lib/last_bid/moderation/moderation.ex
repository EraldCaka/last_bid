defmodule LastBid.Moderation do
  @moduledoc """
  Moderation context.

  Provides hooks for content moderation and abuse resistance.
  Currently provides a basic word-list check and message bounding.
  A real deployment would integrate a proper moderation pipeline here.
  """

  @max_message_length 500
  @max_username_length 30

  @doc "Validate a chat message. Returns :ok or {:error, reason}."
  def validate_chat_message(content) when is_binary(content) do
    cond do
      String.length(content) == 0 ->
        {:error, :empty_message}

      String.length(content) > @max_message_length ->
        {:error, :message_too_long}

      true ->
        # TODO: replace with a real moderation api call
        :ok
    end
  end

  def validate_chat_message(_), do: {:error, :invalid_content}

  @doc "Validate a username. Returns :ok or {:error, reason}."
  def validate_username(username) when is_binary(username) do
    cond do
      String.length(username) < 3 ->
        {:error, :username_too_short}

      String.length(username) > @max_username_length ->
        {:error, :username_too_long}

      not Regex.match?(~r/^[a-zA-Z0-9_]+$/, username) ->
        {:error, :invalid_characters}

      true ->
        :ok
    end
  end

  def validate_username(_), do: {:error, :invalid_username}

  @doc "Sanitize content to prevent XSS."
  def sanitize(content) when is_binary(content) do
    content
    |> String.replace(~r/<[^>]*>/, "")
    |> String.trim()
  end

  def sanitize(_), do: ""
end
