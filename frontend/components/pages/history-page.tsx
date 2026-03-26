"use client"

import { generateDummyTradeHistory } from "@/lib/dummy-data"
import TradeHistory from "@/components/trade-history"

export default function HistoryPage() {
  const history = generateDummyTradeHistory()

  return (
    <div className="space-y-6">
      <TradeHistory trades={history} />
    </div>
  )
}
