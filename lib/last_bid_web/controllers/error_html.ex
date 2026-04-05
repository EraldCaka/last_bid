defmodule LastBidWeb.ErrorHTML do
  @moduledoc false
  use LastBidWeb, :html

  # Renders 404 and 500 inline for simplicity.
  def render("404.html", _assigns), do: "Not Found"
  def render("500.html", _assigns), do: "Internal Server Error"
  def render(template, _assigns), do: Phoenix.Controller.status_message_from_template(template)
end
