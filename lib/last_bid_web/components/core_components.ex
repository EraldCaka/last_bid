defmodule LastBidWeb.CoreComponents do
  @moduledoc """
  Core UI components forLast Bid.
  """

  use Phoenix.Component
  use Gettext, backend: LastBidWeb.Gettext

  @doc "Renders a button."
  attr(:type, :string, default: nil)
  attr(:class, :string, default: nil)
  attr(:rest, :global, include: ~w(disabled form name value))
  slot(:inner_block, required: true)

  def button(assigns) do
    ~H"""
    <button
      type={@type}
      class={[
        "phx-submit-loading:opacity-75 rounded-lg bg-emerald-600 hover:bg-emerald-500",
        "py-2 px-4 text-sm font-semibold leading-6 text-white active:text-white/80",
        @class
      ]}
      {@rest}
    >
      <%= render_slot(@inner_block) %>
    </button>
    """
  end

  @doc "Renders a text input with label."
  attr(:id, :any, default: nil)
  attr(:name, :any)
  attr(:label, :string, default: nil)
  attr(:value, :any)
  attr(:type, :string, default: "text")
  attr(:field, Phoenix.HTML.FormField, doc: "a form field struct")
  attr(:errors, :list, default: [])
  attr(:rest, :global, include: ~w(autocomplete disabled placeholder required readonly))

  def input(%{field: %Phoenix.HTML.FormField{} = field} = assigns) do
    assigns
    |> assign(field: nil, id: assigns.id || field.id)
    |> assign(:errors, Enum.map(field.errors, &translate_error/1))
    |> assign_new(:name, fn -> field.name end)
    |> assign_new(:value, fn -> field.value end)
    |> input()
  end

  def input(assigns) do
    ~H"""
    <div class="mb-4">
      <label :if={@label} for={@id} class="block text-sm font-medium text-gray-300 mb-1">
        <%= @label %>
      </label>
      <input
        type={@type}
        name={@name}
        id={@id}
        value={Phoenix.HTML.Form.normalize_value(@type, @value)}
        class={[
          "block w-full rounded-md bg-gray-800 border border-gray-700",
          "text-gray-100 px-3 py-2 text-sm placeholder-gray-500",
          "focus:outline-none focus:ring-2 focus:ring-emerald-500",
          @errors != [] && "border-red-500"
        ]}
        {@rest}
      />
      <p :for={msg <- @errors} class="mt-1 text-sm text-red-400"><%= msg %></p>
    </div>
    """
  end

  @doc "Renders a simple form."
  attr(:for, :any, required: true)
  attr(:as, :atom, default: nil)
  attr(:action, :string, default: nil)
  attr(:method, :string, default: "post")
  attr(:rest, :global, include: ~w(autocomplete name rel enctype novalidate target multipart))
  slot(:inner_block, required: true)

  def simple_form(assigns) do
    ~H"""
    <.form :let={f} for={@for} as={@as} action={@action} method={@method} {@rest}>
      <%= render_slot(@inner_block, f) %>
    </.form>
    """
  end

  @doc "Translates an error tuple."
  def translate_error({msg, opts}) do
    Enum.reduce(opts, msg, fn {k, v}, acc ->
      String.replace(acc, "%{#{k}}", fn _ -> to_string(v) end)
    end)
  end

  @doc "Renders flash notices."
  attr(:flash, :map, default: %{})

  def flash_group(assigns) do
    ~H"""
    <div>
      <p :if={msg = Phoenix.Flash.get(@flash, :info)} class="alert alert-info" role="alert">
        <%= msg %>
      </p>
      <p :if={msg = Phoenix.Flash.get(@flash, :error)} class="alert alert-danger" role="alert">
        <%= msg %>
      </p>
    </div>
    """
  end
end
