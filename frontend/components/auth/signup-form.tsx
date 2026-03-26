"use client"

import { useState } from "react"
import { useAuth } from "@/hooks/use-auth"
import { Loader2 } from "lucide-react"

interface SignupFormProps {
  onLoginClick: () => void
}

export default function SignupForm({ onLoginClick }: SignupFormProps) {
  const { signup, isLoading } = useAuth()
  const [name, setName] = useState("")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    await signup(name, email, password)
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-foreground mb-2">
          Full Name
        </label>
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="input w-full"
          placeholder="John Doe"
          required
          disabled={isLoading}
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-foreground mb-2">
          Email
        </label>
        <input
          type="email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          className="input w-full"
          placeholder="your@email.com"
          required
          disabled={isLoading}
        />
      </div>
      <div>
        <label className="block text-sm font-medium text-foreground mb-2">
          Password
        </label>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          className="input w-full"
          placeholder="••••••••"
          required
          disabled={isLoading}
        />
      </div>

      <button
        type="submit"
        disabled={isLoading}
        className="w-full py-2 bg-primary text-white rounded-lg font-semibold hover:bg-primary/90 disabled:opacity-50 transition-all flex items-center justify-center gap-2"
        style={{
          boxShadow: '0 0 20px rgba(10, 124, 255, 0.3)',
        }}
      >
        {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : "Sign Up"}
      </button>

      <div className="text-center text-sm text-foreground/60">
        Already have an account?{" "}
        <button
          type="button"
          onClick={onLoginClick}
          className="text-accent hover:text-accent/80 font-medium transition-colors"
        >
          Login
        </button>
      </div>
    </form>
  )
}