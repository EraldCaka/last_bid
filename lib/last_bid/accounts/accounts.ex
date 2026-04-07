defmodule LastBid.Accounts do
  @moduledoc """
  The Accounts context.

  Handles user registration, authentication, session management,
  and password reset flows.
  """

  alias LastBid.Repo
  alias LastBid.Accounts.{User, UserToken}

  @doc "Gets a user by id. Returns nil if not found."
  def get_user(id), do: Repo.get(User, id)

  @doc "Gets a user by id. Raises if not found."
  def get_user!(id), do: Repo.get!(User, id)

  @doc "Gets a user by email."
  def get_user_by_email(email) when is_binary(email) do
    Repo.get_by(User, email: String.downcase(email))
  end

  @doc "Gets a user by username."
  def get_user_by_username(username) when is_binary(username) do
    Repo.get_by(User, username: username)
  end

  @doc "Gets a user by email and password. Returns nil if credentials are invalid."
  def get_user_by_email_and_password(email, password)
      when is_binary(email) and is_binary(password) do
    user = get_user_by_email(email)
    if User.valid_password?(user, password), do: user
  end

  @doc "Registers a new user."
  def register_user(attrs) do
    %User{}
    |> User.registration_changeset(attrs)
    |> Repo.insert()
  end

  @doc "Returns a changeset for registration (no validation run)."
  def change_user_registration(%User{} = user, attrs \\ %{}) do
    User.registration_changeset(user, attrs, hash_password: false, validate_email: false)
  end

  ## Session management

  @doc "Generates a session token for the user."
  def generate_user_session_token(user) do
    {token, user_token} = UserToken.build_session_token(user)
    Repo.insert!(user_token)
    token
  end

  @doc "Gets the user associated with a session token."
  def get_user_by_session_token(token) do
    {:ok, query} = UserToken.verify_session_token_query(token)
    Repo.one(query)
  end

  @doc "Deletes a session token."
  def delete_user_session_token(token) do
    Repo.delete_all(UserToken.token_and_context_query(token, "session"))
    :ok
  end

  defdelegate token_and_context_query(token, context), to: UserToken

  @doc "Delivers a password reset email token."
  def deliver_user_reset_password_instructions(%User{} = user, reset_url_fun)
      when is_function(reset_url_fun, 1) do
    {encoded_token, user_token} = UserToken.build_email_token(user, "reset_password")
    Repo.insert!(user_token)

    LastBid.Accounts.UserNotifier.deliver_reset_password_instructions(
      user,
      reset_url_fun.(encoded_token)
    )
  end

  @doc "Gets the user by reset token, or nil."
  def get_user_by_reset_password_token(token) do
    with {:ok, query} <- UserToken.verify_email_token_query(token, "reset_password"),
         {user, _token} <- Repo.one(query) do
      user
    else
      _ -> nil
    end
  end

  @doc "Resets the user's password."
  def reset_user_password(user, attrs) do
    Ecto.Multi.new()
    |> Ecto.Multi.update(:user, User.password_changeset(user, attrs))
    |> Ecto.Multi.delete_all(
      :tokens,
      UserToken.user_and_contexts_query(user, ["reset_password"])
    )
    |> Repo.transaction()
    |> case do
      {:ok, %{user: user}} -> {:ok, user}
      {:error, :user, changeset, _} -> {:error, changeset}
    end
  end

  @doc "Returns a changeset for changing the user's email."
  def change_user_email(user, attrs \\ %{}) do
    User.email_changeset(user, attrs, validate_email: false)
  end

  @doc "Returns a changeset for changing the user's password."
  def change_user_password(user, attrs \\ %{}) do
    User.password_changeset(user, attrs, hash_password: false)
  end
end
