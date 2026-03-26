"use client"

import { useState } from "react"
import LoginForm from "./login-form"
import SignupForm from "./signup-form"

interface AuthModalProps {
  isOpen: boolean
  mode: "login" | "signup"
  onClose: () => void
  onSwitchMode: (mode: "login" | "signup") => void
}

export default function AuthModal({
  isOpen,
  mode,
  onClose,
  onSwitchMode,
}: AuthModalProps) {
  if (!isOpen) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm">
      <div className="w-full max-w-md">
        <div 
          className="rounded-2xl p-8 relative border-2"
          style={{
            background: 'rgba(10, 124, 255, 0.04)',
            backdropFilter: 'blur(20px)',
            borderColor: 'rgba(10, 124, 255, 0.4)',
          }}
        >
          {/* Close button */}
          <button
            onClick={onClose}
            className="absolute top-4 right-4 text-foreground/50 hover:text-foreground transition-colors"
          >
            ✕
          </button>

          {/* Logo */}
          <div className="flex items-center gap-2 mb-8 justify-center">
            <div className="w-8 h-8 bg-gradient-to-br from-primary to-accent rounded-lg flex items-center justify-center">
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
              TradeMind
            </span>
          </div>

          {/* Form */}
          {mode === "login" ? (
            <LoginForm onSignupClick={() => onSwitchMode("signup")} />
          ) : (
            <SignupForm onLoginClick={() => onSwitchMode("login")} />
          )}
        </div>
      </div>
    </div>
  )
}
