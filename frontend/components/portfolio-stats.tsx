"use client"

import { TrendingUp, TrendingDown } from "lucide-react"

interface PortfolioStatsProps {
  data: any
}

export default function PortfolioStats({ data }: PortfolioStatsProps) {
  const stats = [
    {
      label: "Total Balance",
      value: "$145,230.50",
      change: "+12.5%",
      positive: true,
    },
    {
      label: "Today's Gain",
      value: "$3,420.75",
      change: "+2.1%",
      positive: true,
    },
    {
      label: "Holdings",
      value: "12 Stocks",
      change: "-1 since yesterday",
      positive: false,
    },
  ]

  return (
    <>
      {stats.map((stat, idx) => (
        <div key={idx} className="bg-card border border-border rounded-lg p-6">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-sm text-muted-foreground font-medium">{stat.label}</p>
              <h3 className="text-3xl font-bold text-foreground mt-2">{stat.value}</h3>
            </div>
            <div className={`flex items-center gap-1 ${stat.positive ? "text-accent" : "text-destructive"}`}>
              {stat.positive ? <TrendingUp className="w-5 h-5" /> : <TrendingDown className="w-5 h-5" />}
              <span className="text-sm font-medium">{stat.change}</span>
            </div>
          </div>
        </div>
      ))}
    </>
  )
}
