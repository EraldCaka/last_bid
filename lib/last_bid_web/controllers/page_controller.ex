defmodule LastBidWeb.PageController do
  use LastBidWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
