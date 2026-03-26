"use client"

import { generateDummyPortfolioData, generateDummyExplanations } from "@/lib/dummy-data"
import PortfolioStats from "@/components/portfolio-stats"
import CurrentHoldings from "@/components/current-holdings"
import ExplainabilityPanel from "@/components/explainability-panel"

export default function PortfolioPage() {
  const portfolioData = generateDummyPortfolioData()
  const explanations = generateDummyExplanations()

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-6">
        <PortfolioStats data={portfolioData} />
      </div>

      <div className="grid grid-cols-3 gap-6 h-[calc(100vh-340px)]">
        {/* Left: Holdings */}
        <div className="col-span-2 overflow-hidden">
          <CurrentHoldings holdings={portfolioData.holdings} />
        </div>

        {/* Right: Explainability - Full height */}
        <div className="col-span-1">
          <ExplainabilityPanel explanations={explanations} />
        </div>
      </div>
    </div>
  )
}
