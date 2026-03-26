"use client"

import { useState } from "react"
import { TrendingUp, TrendingDown, Filter } from "lucide-react"

interface Trade {
  id: string
  symbol: string
  name: string
  action: "BUY" | "SELL"
  shares: number
  price: number
  total: number
  date: string
  time: string
  aiDecision: string
  confidence: number
  profitLoss?: number
}

interface TradeHistoryProps {
  trades: Trade[]
}

export default function TradeHistory({ trades }: TradeHistoryProps) {
  const [filterAction, setFilterAction] = useState<"ALL" | "BUY" | "SELL">("ALL")

  const filteredTrades = trades.filter((trade) => {
    if (filterAction === "ALL") return true
    return trade.action === filterAction
  })

  const totalValue = filteredTrades.reduce((sum, trade) => sum + trade.total, 0)
  const successRate = (
    (filteredTrades.filter((t) => (t.profitLoss || 0) > 0).length / filteredTrades.length) *
    100
  ).toFixed(1)

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="p-6 border-b border-border">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-xl font-bold text-foreground">Trade History</h2>
            <p className="text-sm text-muted-foreground mt-1">Last 3 months of trading activity</p>
          </div>
          <div className="flex gap-4">
            <div className="text-right">
              <p className="text-xs text-muted-foreground">Total Volume</p>
              <p className="text-2xl font-bold text-foreground">
                ${totalValue.toLocaleString("en-US", { maximumFractionDigits: 0 })}
              </p>
            </div>
            <div className="text-right">
              <p className="text-xs text-muted-foreground">Success Rate</p>
              <p className="text-2xl font-bold text-accent">{successRate}%</p>
            </div>
          </div>
        </div>

        <div className="flex gap-2">
          {(["ALL", "BUY", "SELL"] as const).map((action) => (
            <button
              key={action}
              onClick={() => setFilterAction(action)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors flex items-center gap-2 ${
                filterAction === action
                  ? action === "BUY"
                    ? "bg-accent text-accent-foreground"
                    : action === "SELL"
                      ? "bg-destructive text-destructive-foreground"
                      : "bg-primary text-primary-foreground"
                  : "bg-secondary text-muted-foreground hover:text-foreground"
              }`}
            >
              <Filter className="w-4 h-4" />
              {action}
            </button>
          ))}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-secondary/20">
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Date</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Stock</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Action</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Shares</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Price</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Total</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">AI Decision</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Confidence</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Result</th>
            </tr>
          </thead>
          <tbody>
            {filteredTrades.map((trade, idx) => (
              <tr key={idx} className="border-b border-border hover:bg-secondary/10 transition-colors">
                <td className="px-6 py-4 text-muted-foreground text-xs">{trade.date}</td>
                <td className="px-6 py-4">
                  <div>
                    <p className="font-semibold text-foreground">{trade.symbol}</p>
                    <p className="text-xs text-muted-foreground">{trade.name}</p>
                  </div>
                </td>
                <td className="px-6 py-4">
                  <div
                    className={`px-2 py-1 rounded text-xs font-bold w-fit ${
                      trade.action === "BUY" ? "bg-accent/20 text-accent" : "bg-destructive/20 text-destructive"
                    }`}
                  >
                    {trade.action}
                  </div>
                </td>
                <td className="px-6 py-4 text-foreground">{trade.shares}</td>
                <td className="px-6 py-4 text-foreground">${trade.price.toFixed(2)}</td>
                <td className="px-6 py-4 font-semibold text-foreground">${trade.total.toFixed(2)}</td>
                <td className="px-6 py-4 text-xs text-muted-foreground max-w-xs">{trade.aiDecision}</td>
                <td className="px-6 py-4">
                  <div className="flex items-center gap-2">
                    <div className="w-12 bg-secondary/30 rounded-full h-1.5">
                      <div
                        className="bg-primary h-1.5 rounded-full"
                        style={{ width: `${(trade.confidence / 5) * 100}%` }}
                      />
                    </div>
                    <span className="text-xs font-semibold text-primary w-8">{trade.confidence.toFixed(1)}</span>
                  </div>
                </td>
                <td
                  className={`px-6 py-4 font-semibold flex items-center gap-1 ${trade.profitLoss && trade.profitLoss > 0 ? "text-accent" : "text-destructive"}`}
                >
                  {trade.profitLoss && trade.profitLoss > 0 ? (
                    <TrendingUp className="w-4 h-4" />
                  ) : (
                    <TrendingDown className="w-4 h-4" />
                  )}
                  {trade.profitLoss && trade.profitLoss > 0 ? "+" : ""}
                  {trade.profitLoss?.toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
