"use client"

import { useState, useEffect } from "react"
import { Save, Info, Loader2 } from "lucide-react"
import { toast } from "sonner"
import { useAuth } from "@/hooks/use-auth"

export default function SettingsPage() {
  const { user, updateUserState } = useAuth()
  
  const [settings, setSettings] = useState({
    targetProfit: 15,
    maxRisk: 5,
  })

  const [loading, setLoading] = useState(false)
  const [saved, setSaved] = useState(false)

  // Initialize from user data when available
  useEffect(() => {
    if (user) {
      setSettings({
        targetProfit: user.target_profit || 15,
        maxRisk: user.max_risk || 5,
      })
    }
  }, [user])

  const handleChange = (field: string, value: any) => {
    setSettings((prev) => ({ ...prev, [field]: value }))
    setSaved(false)
  }

  const handleSave = async () => {
    setLoading(true)
    try {
      const res = await fetch("/api/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          target_profit: settings.targetProfit,
          max_risk: settings.maxRisk
        }),
      })

      if (!res.ok) throw new Error("Failed to update settings")

      // Update local context
      updateUserState({
        target_profit: settings.targetProfit,
        max_risk: settings.maxRisk
      })

      setSaved(true)
      toast.success("Preferences saved successfully")
      setTimeout(() => setSaved(false), 3000)
    } catch (error) {
      toast.error("Failed to save preferences. Please try again.")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="max-w-2xl space-y-6">
      <div className="bg-card border border-border rounded-lg p-6">
        <h2 className="text-2xl font-bold text-foreground mb-6">Trading Preferences</h2>

        {/* Risk & Profit Settings */}
        <div className="space-y-6">
          <div>
            <label className="block text-sm font-semibold text-foreground mb-2">
              Target Profit Goal (%): {settings.targetProfit}%
            </label>
            <input
              type="range"
              min="5"
              max="50"
              value={settings.targetProfit}
              onChange={(e) => handleChange("targetProfit", Number.parseInt(e.target.value))}
              className="w-full h-2 bg-secondary rounded-lg appearance-none cursor-pointer accent-primary"
            />
            <p className="text-xs text-muted-foreground mt-2">Desired return on your investment portfolio</p>
          </div>

          <div>
            <label className="block text-sm font-semibold text-foreground mb-2">
              Maximum Risk Tolerance (%): {settings.maxRisk}%
            </label>
            <input
              type="range"
              min="1"
              max="20"
              value={settings.maxRisk}
              onChange={(e) => handleChange("maxRisk", Number.parseInt(e.target.value))}
              className="w-full h-2 bg-secondary rounded-lg appearance-none cursor-pointer accent-primary"
            />
            <p className="text-xs text-muted-foreground mt-2">Maximum acceptable portfolio drawdown</p>
          </div>
        </div>
      </div>

      {/* Info Cards */}
      <div className="grid grid-cols-2 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex gap-3">
            <Info className="w-5 h-5 text-primary flex-shrink-0 mt-0.5" />
            <div>
              <h3 className="font-semibold text-foreground text-sm mb-2">How It Works</h3>
              <p className="text-xs text-muted-foreground">
                Our AI system uses your preferences to generate personalized trading recommendations that align with
                your financial goals and risk tolerance.
              </p>
            </div>
          </div>
        </div>

        <div className="bg-card border border-border rounded-lg p-6">
          <div className="flex gap-3">
            <Info className="w-5 h-5 text-accent flex-shrink-0 mt-0.5" />
            <div>
              <h3 className="font-semibold text-foreground text-sm mb-2">Key Metrics</h3>
              <p className="text-xs text-muted-foreground">
                The AI considers technical indicators, sentiment analysis, market trends, and your custom preferences
                before making recommendations.
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Save Button */}
      <button
        onClick={handleSave}
        disabled={loading}
        className="w-full px-6 py-3 bg-primary text-primary-foreground font-semibold rounded-lg hover:bg-primary/90 transition-colors flex items-center justify-center gap-2 disabled:opacity-50"
      >
        {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Save className="w-5 h-5" />}
        {loading ? "Saving..." : "Save Preferences"}
      </button>

      {saved && (
        <div className="p-4 bg-green-500/10 border border-green-500/20 rounded-lg animate-in fade-in">
          <p className="text-sm font-medium text-green-500">✓ Settings saved successfully</p>
        </div>
      )}
    </div>
  )
}