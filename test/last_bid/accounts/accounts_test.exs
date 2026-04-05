defmodule LastBid.AccountsTest do
  use LastBid.DataCase

  alias LastBid.Accounts
  alias LastBid.Accounts.User

  describe "register_user/1" do
    test "creates a user with valid attributes" do
      attrs = %{
        email: "test@example.com",
        username: "testuser",
        password: "supersecure123!"
      }

      assert {:ok, %User{} = user} = Accounts.register_user(attrs)
      assert user.email == "test@example.com"
      assert user.username == "testuser"
      assert user.hashed_password != nil
      # Raw password must not be stored
      assert user.hashed_password != "supersecure123!"
    end

    test "rejects duplicate email" do
      attrs = %{email: "dup@example.com", username: "user1", password: "secure_password_1!"}
      {:ok, _} = Accounts.register_user(attrs)

      dup_attrs = %{email: "dup@example.com", username: "user2", password: "secure_password_2!"}
      assert {:error, changeset} = Accounts.register_user(dup_attrs)
      assert "has already been taken" in errors_on(changeset).email
    end

    test "rejects duplicate username" do
      attrs = %{email: "a@example.com", username: "samehandle", password: "secure_password_1!"}
      {:ok, _} = Accounts.register_user(attrs)

      dup_attrs = %{email: "b@example.com", username: "samehandle", password: "secure_password_2!"}
      assert {:error, changeset} = Accounts.register_user(dup_attrs)
      assert "has already been taken" in errors_on(changeset).username
    end

    test "rejects passwords shorter than 12 characters" do
      attrs = %{email: "x@example.com", username: "xuser", password: "short"}
      assert {:error, changeset} = Accounts.register_user(attrs)
      assert "should be at least 12 character(s)" in errors_on(changeset).password
    end

    test "rejects invalid username format" do
      attrs = %{email: "x@example.com", username: "bad name!", password: "supersecure123!"}
      assert {:error, changeset} = Accounts.register_user(attrs)
      assert errors_on(changeset).username != []
    end

    test "rejects invalid email format" do
      attrs = %{email: "notanemail", username: "xuser", password: "supersecure123!"}
      assert {:error, changeset} = Accounts.register_user(attrs)
      assert errors_on(changeset).email != []
    end
  end

  describe "get_user_by_email_and_password/2" do
    test "returns user for valid credentials" do
      {:ok, _user} = Accounts.register_user(%{
        email: "auth@example.com",
        username: "authuser",
        password: "correctpassword1!"
      })

      assert %User{} = Accounts.get_user_by_email_and_password("auth@example.com", "correctpassword1!")
    end

    test "returns nil for wrong password" do
      {:ok, _} = Accounts.register_user(%{
        email: "auth2@example.com",
        username: "authuser2",
        password: "correctpassword1!"
      })

      assert nil == Accounts.get_user_by_email_and_password("auth2@example.com", "wrongpassword")
    end

    test "returns nil for unknown email" do
      assert nil == Accounts.get_user_by_email_and_password("nobody@example.com", "any_password")
    end
  end

  describe "session tokens" do
    test "generate and verify session token" do
      {:ok, user} = Accounts.register_user(%{
        email: "session@example.com",
        username: "sessionuser",
        password: "goodpassword123!"
      })

      token = Accounts.generate_user_session_token(user)
      assert is_binary(token)

      retrieved = Accounts.get_user_by_session_token(token)
      assert retrieved.id == user.id
    end

    test "delete session token" do
      {:ok, user} = Accounts.register_user(%{
        email: "del@example.com",
        username: "deluser",
        password: "goodpassword123!"
      })

      token = Accounts.generate_user_session_token(user)
      Accounts.delete_user_session_token(token)
      assert nil == Accounts.get_user_by_session_token(token)
    end
  end
end
