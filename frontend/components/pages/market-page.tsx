"use client"

import { generateDummyStocks } from "@/lib/dummy-data"
import MarketData from "@/components/market-data"
import TrendAnalysis from "@/components/trend-analysis"

export default function MarketPage() {
  const stocks = generateDummyStocks()

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-6">
        {/* Left: Market Trends & Analysis */}
        <div className="col-span-1">
          <TrendAnalysis />
        </div>

        {/* Center & Right: Market Data */}
        <div className="col-span-2">
          <MarketData stocks={stocks} />
        </div>
      </div>
    </div>
  )
}
