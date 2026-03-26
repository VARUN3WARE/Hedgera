"use client"

import { CheckCircle, AlertCircle } from "lucide-react"

interface Decision {
  id: string
  stock: string
  action: "BUY" | "SELL" | "HOLD"
  confidence: number
  positivePoints: string[]
  negativePoints: string[]
  timestamp: string
}

interface ExplainabilityPanelProps {
  explanations: Decision[]
}

export default function ExplainabilityPanel({ explanations }: ExplainabilityPanelProps) {
  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden flex flex-col h-full">
      <div className="p-6 border-b border-border">
        <h2 className="text-lg font-bold text-foreground">AI Decisions Explained</h2>
        <p className="text-xs text-muted-foreground mt-1">Agentic system reasoning</p>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {explanations.map((decision, idx) => (
          <div key={idx} className="bg-secondary/20 border border-border rounded-lg p-4 space-y-3">
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <p className="font-semibold text-foreground">{decision.stock}</p>
                <p className="text-xs text-muted-foreground mt-0.5">{decision.timestamp}</p>
              </div>
              <div
                className={`px-2 py-1 rounded text-xs font-bold ${
                  decision.action === "BUY"
                    ? "bg-accent/20 text-accent"
                    : decision.action === "SELL"
                      ? "bg-destructive/20 text-destructive"
                      : "bg-muted/20 text-muted-foreground"
                }`}
              >
                {decision.action}
              </div>
            </div>

            {/* Confidence Score */}
            <div className="flex items-center gap-2">
              <p className="text-xs text-muted-foreground">Confidence:</p>
              <div className="flex-1 bg-secondary/30 rounded-full h-1.5">
                <div
                  className="bg-primary h-1.5 rounded-full"
                  style={{ width: `${(decision.confidence / 5) * 100}%` }}
                />
              </div>
              <p className="text-xs font-semibold text-primary">{decision.confidence.toFixed(1)}/5</p>
            </div>

            {/* Positive Points */}
            {decision.positivePoints.length > 0 && (
              <div className="space-y-1">
                <p className="text-xs font-semibold text-accent flex items-center gap-1">
                  <CheckCircle className="w-3 h-3" />
                  Positive Factors
                </p>
                <ul className="text-xs text-muted-foreground space-y-0.5">
                  {decision.positivePoints.map((point, i) => (
                    <li key={i} className="ml-4 flex gap-2">
                      <span className="text-accent">•</span>
                      <span>{point}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Negative Points */}
            {decision.negativePoints.length > 0 && (
              <div className="space-y-1">
                <p className="text-xs font-semibold text-destructive flex items-center gap-1">
                  <AlertCircle className="w-3 h-3" />
                  Risk Factors
                </p>
                <ul className="text-xs text-muted-foreground space-y-0.5">
                  {decision.negativePoints.map((point, i) => (
                    <li key={i} className="ml-4 flex gap-2">
                      <span className="text-destructive">•</span>
                      <span>{point}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
