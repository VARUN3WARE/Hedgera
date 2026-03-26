"use client"

import { LineChart, Line, ResponsiveContainer } from "recharts"

interface SparklineChartProps {
  data: number[]
  isPositive: boolean
}

export function SparklineChart({ data, isPositive }: SparklineChartProps) {
  // Convert simple number array to chart data
  const chartData = data.map((value, index) => ({
    value,
    index,
  }))

  return (
    <div className="w-20 h-10">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
          <Line
            type="monotone"
            dataKey="value"
            stroke={isPositive ? "#00ff9f" : "#ff4444"}
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
