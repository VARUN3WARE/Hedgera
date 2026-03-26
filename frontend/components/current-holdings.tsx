"use client"

import { TrendingUp, TrendingDown } from "lucide-react"
import { SparklineChart } from "./sparkline-chart"

interface Holding {
  symbol: string
  name: string
  shares: number
  avgCost: number
  currentPrice: number
  change: number
  value: number
  sparklineData?: number[]
}

interface CurrentHoldingsProps {
  holdings: Holding[]
}

export default function CurrentHoldings({ holdings }: CurrentHoldingsProps) {
  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden"
    >
      <div className="p-6 border-b border-border">
        <h2 className="text-xl font-bold text-foreground">Current Holdings</h2>
        <p className="text-sm text-muted-foreground mt-1">Your active stock positions</p>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-secondary/20">
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Symbol</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Shares</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Avg Cost</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Current</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Change</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Chart</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Value</th>
            </tr>
          </thead>
          <tbody>
            {holdings.map((holding, idx) => (
              <tr key={idx} className="border-b border-border hover:bg-secondary/10 transition-colors">
                <td className="px-6 py-4">
                  <div>
                    <p className="font-semibold text-foreground">{holding.symbol}</p>
                    <p className="text-xs text-muted-foreground">{holding.name}</p>
                  </div>
                </td>
                <td className="px-6 py-4 text-foreground">{holding.shares}</td>
                <td className="px-6 py-4 text-foreground">${holding.avgCost.toFixed(2)}</td>
                <td className="px-6 py-4 text-foreground">${holding.currentPrice.toFixed(2)}</td>
                <td
                  className={`px-6 py-4 font-medium flex items-center gap-1 ${holding.change > 0 ? "text-accent" : "text-destructive"}`}
                >
                  {holding.change > 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                  {holding.change > 0 ? "+" : ""}
                  {holding.change.toFixed(2)}%
                </td>
                <td className="px-6 py-4">
                  {holding.sparklineData && (
                    <SparklineChart data={holding.sparklineData} isPositive={holding.change > 0} />
                  )}
                </td>
                <td className="px-6 py-4 font-semibold text-foreground">${holding.value.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
