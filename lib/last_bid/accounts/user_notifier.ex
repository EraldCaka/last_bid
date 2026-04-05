defmodule LastBid.Accounts.UserNotifier do
  @moduledoc "Sends user-facing emails via Swoosh."

  import Swoosh.Email
  alias LastBid.Mailer

  defp deliver(recipient, subject, body) do
    email =
      new()
      |> to(recipient)
      |> from({"Dark Pool", "no-reply@darkpool.local"})
      |> subject(subject)
      |> text_body(body)

    with {:ok, _metadata} <- Mailer.deliver(email) do
      {:ok, email}
    end
  end

  @doc "Delivers a password reset email."
  def deliver_reset_password_instructions(user, url) do
    deliver(user.email, "Reset your Dark Pool password", """
    Hi #{user.username},

    You can reset your password by visiting the URL below:

    #{url}

    If you didn't request this, please ignore this email.

    The link is valid for 24 hours.
    """)
  end

  @doc "Delivers a confirmation email."
  def deliver_confirmation_instructions(user, url) do
    deliver(user.email, "Confirm your Dark Pool account", """
    Hi #{user.username},

    Welcome to Dark Pool! Please confirm your account:

    #{url}

    If you didn't create an account, please ignore this email.
    """)
  end
end
