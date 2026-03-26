"use client"

import { useState } from "react"
import { TrendingUp, TrendingDown, Search } from "lucide-react"
import { SparklineChart } from "./sparkline-chart"

interface Stock {
  symbol: string
  name: string
  price: number
  change: number
  changePercent: number
  volume: number
  marketCap: string
  pe: number
  sparklineData?: number[]
}

interface MarketDataProps {
  stocks: Stock[]
}

export default function MarketData({ stocks }: MarketDataProps) {
  const [searchTerm, setSearchTerm] = useState("")
  const [sortBy, setSortBy] = useState<"symbol" | "change" | "price">("symbol")

  const filteredStocks = stocks
    .filter(
      (stock) =>
        stock.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
        stock.name.toLowerCase().includes(searchTerm.toLowerCase()),
    )
    .sort((a, b) => {
      if (sortBy === "change") return b.changePercent - a.changePercent
      if (sortBy === "price") return b.price - a.price
      return a.symbol.localeCompare(b.symbol)
    })

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="p-6 border-b border-border">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-xl font-bold text-foreground">Market Data</h2>
            <p className="text-sm text-muted-foreground mt-1">Real-time stock prices and trends</p>
          </div>
        </div>

        <div className="flex gap-4 items-center">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <input
              type="text"
              placeholder="Search stocks..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 bg-secondary border border-border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
            />
          </div>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as any)}
            className="px-4 py-2 bg-secondary border border-border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary text-foreground"
          >
            <option value="symbol">Sort by Symbol</option>
            <option value="change">Sort by Change</option>
            <option value="price">Sort by Price</option>
          </select>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-secondary/20">
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Symbol</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Price</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Change</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Change %</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Chart</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Volume</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">Market Cap</th>
              <th className="px-6 py-3 text-left font-semibold text-muted-foreground">P/E</th>
            </tr>
          </thead>
          <tbody>
            {filteredStocks.map((stock, idx) => (
              <tr key={idx} className="border-b border-border hover:bg-secondary/10 transition-colors cursor-pointer">
                <td className="px-6 py-4">
                  <div>
                    <p className="font-semibold text-foreground">{stock.symbol}</p>
                    <p className="text-xs text-muted-foreground">{stock.name}</p>
                  </div>
                </td>
                <td className="px-6 py-4 font-semibold text-foreground">${stock.price.toFixed(2)}</td>
                <td
                  className={`px-6 py-4 font-medium flex items-center gap-1 ${stock.change > 0 ? "text-accent" : "text-destructive"}`}
                >
                  {stock.change > 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                  {stock.change > 0 ? "+" : ""}
                  {stock.change.toFixed(2)}
                </td>
                <td className={`px-6 py-4 font-medium ${stock.changePercent > 0 ? "text-accent" : "text-destructive"}`}>
                  {stock.changePercent > 0 ? "+" : ""}
                  {stock.changePercent.toFixed(2)}%
                </td>
                <td className="px-6 py-4">
                  {stock.sparklineData && <SparklineChart data={stock.sparklineData} isPositive={stock.change > 0} />}
                </td>
                <td className="px-6 py-4 text-foreground">{(stock.volume / 1000000).toFixed(1)}M</td>
                <td className="px-6 py-4 text-foreground">{stock.marketCap}</td>
                <td className="px-6 py-4 text-foreground">{stock.pe.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
