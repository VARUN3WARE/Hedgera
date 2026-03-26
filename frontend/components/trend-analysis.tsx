"use client"

import { TrendingUp, AlertTriangle, Zap } from "lucide-react"

export default function TrendAnalysis() {
  const trends = [
    {
      title: "Market Sentiment",
      value: "Bullish",
      description: "Strong upward momentum",
      icon: TrendingUp,
      color: "text-accent",
      bg: "bg-accent/10",
    },
    {
      title: "Volatility Index",
      value: "Moderate",
      description: "Market stability good",
      icon: Zap,
      color: "text-primary",
      bg: "bg-primary/10",
    },
    {
      title: "Risk Alert",
      value: "Low",
      description: "No major concerns",
      icon: AlertTriangle,
      color: "text-accent",
      bg: "bg-accent/10",
    },
  ]

  return (
    <div className="space-y-4">
      {trends.map((trend, idx) => {
        const Icon = trend.icon
        return (
          <div key={idx} className="bg-card border border-border rounded-lg p-4">
            <div className="flex items-start gap-3">
              <div className={`p-2 rounded-lg ${trend.bg}`}>
                <Icon className={`w-5 h-5 ${trend.color}`} />
              </div>
              <div className="flex-1">
                <p className="text-xs text-muted-foreground font-medium">{trend.title}</p>
                <p className="text-lg font-bold text-foreground mt-1">{trend.value}</p>
                <p className="text-xs text-muted-foreground mt-1">{trend.description}</p>
              </div>
            </div>
          </div>
        )
      })}

      {/* Market Overview */}
      <div className="bg-card border border-border rounded-lg p-4">
        <h3 className="font-bold text-foreground text-sm mb-3">Technical Indicators</h3>
        <div className="space-y-2 text-xs">
          <div className="flex justify-between">
            <span className="text-muted-foreground">RSI (14)</span>
            <span className="text-primary font-semibold">58.2</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">MACD</span>
            <span className="text-accent font-semibold">Positive</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">SMA 50/200</span>
            <span className="text-accent font-semibold">Bullish</span>
          </div>
        </div>
      </div>

      {/* Market News */}
      <div className="bg-card border border-border rounded-lg p-4">
        <h3 className="font-bold text-foreground text-sm mb-3">Market News</h3>
        <div className="space-y-2 text-xs">
          <p className="text-muted-foreground">Fed maintains interest rates steady</p>
          <p className="text-muted-foreground">Tech stocks lead market rally</p>
          <p className="text-muted-foreground">Energy sector shows strength</p>
        </div>
      </div>
    </div>
  )
}
