"use client"

import { useState } from "react"
import { useRouter } from 'next/navigation'
import { Loader2 } from "lucide-react"
import { toast } from "sonner"

export default function OnboardingPage() {
  const router = useRouter()
  const [step, setStep] = useState(1)

  const [formData, setFormData] = useState({
    brokerage_api_key: "",
    brokerage_secret_key: "",
    account_nickname: "",
  })

  const [isLoading, setIsLoading] = useState(false)

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target
    setFormData((prev) => ({
      ...prev,
      [name]: value,
    }))
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)

    try {
      console.log("Submitting form data:", formData);
      const res = await fetch("/api/onboarding", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData), // snake_case body is sent
      })

      if (!res.ok) {
        const error = await res.json()
        throw new Error(error.message || "Setup failed")
      }

      toast.success("Brokerage connected successfully!")
      window.location.href = "/"

    } catch (error: any) {
      console.error(error)
      toast.error(error.message || "Failed to save settings. Please try again.")
    } finally {
      setIsLoading(false)
    }
  }

  const totalSteps = 2

  return (
    <div className="min-h-screen bg-background flex flex-col items-center justify-center px-4 relative overflow-hidden">
      <div className="absolute inset-0 -z-10">
        <div className="absolute top-1/2 right-1/4 w-96 h-96 bg-primary/15 rounded-full blur-3xl opacity-25"></div>
        <div className="absolute bottom-0 left-1/3 w-96 h-96 bg-accent/15 rounded-full blur-3xl opacity-25"></div>
      </div>

      <div className="w-full max-w-md">
        <div className="flex items-center gap-2 mb-12 justify-center">
          <div className="w-8 h-8 bg-gradient-to-br from-primary to-accent rounded-lg flex items-center justify-center shadow-[0_0_20px_rgba(10,124,255,0.3)]">
            <span className="text-white font-bold text-lg">✦</span>
          </div>
          <span
            className="text-lg font-bold"
            style={{
              background: 'linear-gradient(to right, #0A7CFF, #1f87ff, #0A7CFF)',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent',
              backgroundClip: 'text',
            }}
          >
            TradeAI
          </span>
        </div>

        <div className="mb-8">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-2xl font-bold text-foreground">
              {step === 1 ? "Setup Your Account" : "Brokerage Connection"}
            </h2>
            <span className="text-sm text-foreground/50">
              Step {step} of {totalSteps}
            </span>
          </div>

          <div className="h-1 bg-border/40 rounded-full overflow-hidden">
            <div
              className="h-full bg-gradient-to-r from-primary to-accent transition-all duration-300"
              style={{ width: `${(step / totalSteps) * 100}%` }}
            />
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-6">
          {step === 1 ? (
            <div
              className="space-y-4 rounded-xl p-6 border-2"
              style={{
                background: 'rgba(10, 124, 255, 0.04)',
                backdropFilter: 'blur(20px)',
                borderColor: 'rgba(10, 124, 255, 0.2)',
              }}
            >
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  Account Nickname
                </label>

                {/* 🔥 updated input name */}
                <input
                  type="text"
                  name="account_nickname"
                  value={formData.account_nickname}
                  onChange={handleChange}
                  placeholder="My Trading Account"
                  className="input w-full"
                  required
                />
              </div>

              <div className="text-center text-sm text-foreground/60">
                <div className="mb-4 text-2xl">🔒</div>
                <p>Your credentials are encrypted and stored securely.</p>
              </div>
            </div>
          ) : (
            <div
              className="space-y-4 rounded-xl p-6 border-2"
              style={{
                background: 'rgba(10, 124, 255, 0.04)',
                backdropFilter: 'blur(20px)',
                borderColor: 'rgba(10, 124, 255, 0.2)',
              }}
            >
              <div>
                <label className="block text-sm font-medium text-foreground mb-2 flex items-center gap-2">
                  <span>🔑</span>
                  Brokerage API Key
                </label>

                {/* 🔥 updated input */}
                <input
                  type="password"
                  name="brokerage_api_key"
                  value={formData.brokerage_api_key}
                  onChange={handleChange}
                  placeholder="Enter your API key"
                  className="input w-full"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-foreground mb-2 flex items-center gap-2">
                  <span>🔐</span>
                  Brokerage Secret Key
                </label>

                {/* 🔥 updated input */}
                <input
                  type="password"
                  name="brokerage_secret_key"
                  value={formData.brokerage_secret_key}
                  onChange={handleChange}
                  placeholder="Enter your secret key"
                  className="input w-full"
                  required
                />
              </div>

              <div className="bg-accent/5 border border-accent/20 rounded-lg p-4 text-sm text-foreground/70">
                <p className="font-medium text-foreground mb-2">
                  How to find your credentials:
                </p>
                <ul className="space-y-1 text-xs">
                  <li>1. Log in to your brokerage platform</li>
                  <li>2. Go to Settings → API & Applications</li>
                  <li>3. Generate new API credentials</li>
                  <li>4. Copy and paste them here</li>
                </ul>
              </div>
            </div>
          )}

          <div className="flex gap-4">
            {step > 1 && (
              <button
                type="button"
                onClick={() => setStep(step - 1)}
                className="flex-1 py-2 rounded-lg font-medium text-foreground border-2 transition-all border-primary/20 hover:bg-primary/5"
              >
                Back
              </button>
            )}

            {step === 1 ? (
              <button
                type="button"
                onClick={() => setStep(2)}
                disabled={!formData.account_nickname}
                className="flex-1 py-2 bg-primary text-white rounded-lg font-medium hover:bg-primary/90 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
                style={{
                  boxShadow:
                    "0 0 20px rgba(10, 124, 255, 0.3), 0 0 40px rgba(10, 124, 255, 0.15), inset 0 0 20px rgba(10, 124, 255, 0.05)",
                }}
              >
                Next
              </button>
            ) : (
              <button
                type="submit"
                disabled={isLoading || !formData.brokerage_api_key}
                className="flex-1 py-2 bg-primary text-white rounded-lg font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-all flex items-center justify-center gap-2"
                style={{
                  boxShadow:
                    "0 0 20px rgba(10, 124, 255, 0.3), 0 0 40px rgba(10, 124, 255, 0.15), inset 0 0 20px rgba(10, 124, 255, 0.05)",
                }}
              >
                {isLoading ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    <span>Setting up...</span>
                  </>
                ) : (
                  "Complete Setup"
                )}
              </button>
            )}
          </div>
        </form>

        <div className="mt-6 text-center">
          <button
            onClick={() => router.push("/")}
            className="text-foreground/50 hover:text-foreground/70 text-sm transition-colors"
          >
            Skip for now
          </button>
        </div>
      </div>
    </div>
  )
}
