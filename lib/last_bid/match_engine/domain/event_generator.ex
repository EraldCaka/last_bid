defmodule LastBid.MatchEngine.Domain.EventGenerator do
  @moduledoc """
  Pure module that generates the news/event for a round.

  Events affect one or more companies and are public knowledge.
  The actual price effect is applied during resolution.
  """

  @events [
    %{
      type: :earnings_beat,
      description: "%s reports better-than-expected earnings.",
      price_delta: 0.08,
      targets: 1
    },
    %{
      type: :antitrust_probe,
      description: "Regulators open an antitrust probe into %s.",
      price_delta: -0.06,
      targets: 1
    },
    %{
      type: :supply_shock,
      description: "Supply disruption hits %s.",
      price_delta: -0.04,
      targets: 2
    },
    %{
      type: :executive_scandal,
      description: "Executive misconduct allegations surface at %s.",
      price_delta: -0.10,
      targets: 1
    },
    %{
      type: :debt_downgrade,
      description: "%s debt is downgraded by a major rating agency.",
      price_delta: -0.05,
      targets: 1
    },
    %{
      type: :market_rally,
      description: "Broad market rally lifts %s.",
      price_delta: 0.05,
      targets: 3
    },
    %{
      type: :merger_rumor,
      description: "Acquisition rumors circulate around %s.",
      price_delta: 0.12,
      targets: 1
    },
    %{
      type: :sector_rotation,
      description: "Institutional funds rotate out of %s.",
      price_delta: -0.03,
      targets: 2
    }
  ]

  @doc "Generate a random event for the given round, targeting companies from the state."
  def generate(state, round_number) do
    tickers = Map.keys(state.companies)
    event_template = Enum.random(@events)
    targets = Enum.take(Enum.shuffle(tickers), event_template.targets)

    %{
      type: event_template.type,
      description: build_description(event_template.description, targets),
      targets: targets,
      price_delta: event_template.price_delta,
      round: round_number,
      public: true
    }
  end

  defp build_description(template, [single]), do: String.replace(template, "%s", single)

  defp build_description(template, tickers) do
    String.replace(template, "%s", Enum.join(tickers, " and "))
  end
end
