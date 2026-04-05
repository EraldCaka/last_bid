defmodule LastBidWeb do
  @moduledoc """
  The entrypoint for the LastBidWeb web layer.

  Defines `use` helpers for controllers, channels, routers, and live views.
  """

  def static_paths, do: ~w(assets fonts images favicon.ico robots.txt)

  def router do
    quote do
      use Phoenix.Router, helpers: false

      import Plug.Conn
      import Phoenix.Controller
      import Phoenix.LiveView.Router
    end
  end

  def channel do
    quote do
      use Phoenix.Channel
    end
  end

  def controller do
    quote do
      use Phoenix.Controller,
        formats: [:html, :json],
        layouts: [html: LastBidWeb.Layouts]

      import Plug.Conn
      use Gettext, backend: LastBidWeb.Gettext
      unquote(verified_routes())
    end
  end

  def html do
    quote do
      use Phoenix.Component
      import Phoenix.Controller, only: [get_csrf_token: 0, view_module: 1, view_template: 1]
      import Phoenix.HTML
      use Gettext, backend: LastBidWeb.Gettext
      import LastBidWeb.CoreComponents, only: [translate_error: 1]
      unquote(verified_routes())
    end
  end

  def live_view do
    quote do
      use Phoenix.LiveView, layout: {LastBidWeb.Layouts, :app}
      unquote(html_helpers())
    end
  end

  def live_component do
    quote do
      use Phoenix.LiveComponent
      unquote(html_helpers())
    end
  end

  defp html_helpers do
    quote do
      use Phoenix.Component
      import Phoenix.LiveView.Helpers
      use Gettext, backend: LastBidWeb.Gettext
      unquote(verified_routes())
    end
  end

  defp verified_routes do
    quote do
      use Phoenix.VerifiedRoutes,
        endpoint: LastBidWeb.Endpoint,
        router: LastBidWeb.Router,
        statics: LastBidWeb.static_paths()
    end
  end

  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
