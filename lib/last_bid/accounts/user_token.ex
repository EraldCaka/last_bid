defmodule LastBid.Accounts.UserToken do
  use Ecto.Schema
  import Ecto.Query

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @session_validity_in_days 60
  @reset_password_validity_in_days 1
  @confirm_validity_in_days 7

  @rand_size 32
  @hash_algorithm :sha256

  schema "user_tokens" do
    field(:token, :binary)
    field(:context, :string)
    field(:sent_to, :string)

    belongs_to(:user, LastBid.Accounts.User)

    timestamps(type: :utc_datetime, updated_at: false)
  end

  @doc "Build a session token for the given user."
  def build_session_token(user) do
    token = :crypto.strong_rand_bytes(@rand_size)
    {token, %__MODULE__{token: token, context: "session", user_id: user.id}}
  end

  @doc "Build a hashed token for the given user and context (email-based tokens)."
  def build_email_token(user, context) do
    build_hashed_token(user, context, user.email)
  end

  defp build_hashed_token(user, context, sent_to) do
    token = :crypto.strong_rand_bytes(@rand_size)
    hashed_token = :crypto.hash(@hash_algorithm, token)

    {Base.url_encode64(token, padding: false),
     %__MODULE__{
       token: hashed_token,
       context: context,
       sent_to: sent_to,
       user_id: user.id
     }}
  end

  @doc "Query for session token, checking validity."
  def verify_session_token_query(token) do
    query =
      from(token in token_and_context_query(token, "session"),
        join: user in assoc(token, :user),
        where: token.inserted_at > ago(@session_validity_in_days, "day"),
        select: user
      )

    {:ok, query}
  end

  @doc "Query for a hashed email token."
  def verify_email_token_query(token, context) do
    case Base.url_decode64(token, padding: false) do
      {:ok, decoded_token} ->
        hashed_token = :crypto.hash(@hash_algorithm, decoded_token)
        days = days_for_context(context)

        query =
          from(token in token_and_context_query(hashed_token, context),
            join: user in assoc(token, :user),
            where: token.inserted_at > ago(^days, "day"),
            select: {user, token}
          )

        {:ok, query}

      :error ->
        :error
    end
  end

  defp days_for_context("confirm"), do: @confirm_validity_in_days
  defp days_for_context("reset_password"), do: @reset_password_validity_in_days

  @doc "Query for all tokens of a given user in a given context."
  def user_and_contexts_query(user, :all) do
    from(t in __MODULE__, where: t.user_id == ^user.id)
  end

  def user_and_contexts_query(user, [_ | _] = contexts) do
    from(t in __MODULE__, where: t.user_id == ^user.id and t.context in ^contexts)
  end

  @doc "Query for a token by value and context."
  def token_and_context_query(token, context) do
    from(__MODULE__, where: [token: ^token, context: ^context])
  end
end
