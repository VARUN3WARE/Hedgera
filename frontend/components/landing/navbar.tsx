"use client"

import { useEffect, useState } from "react"

interface NavbarProps {
  onAuthClick: (mode: "login" | "signup") => void
}

export default function Navbar({ onAuthClick }: NavbarProps) {
  const [isScrolled, setIsScrolled] = useState(false)

  useEffect(() => {
    const handleScroll = () => setIsScrolled(window.scrollY > 10)
    window.addEventListener("scroll", handleScroll)
    return () => window.removeEventListener("scroll", handleScroll)
  }, [])

  return (
    <nav
      className={`
        fixed top-4 left-1/2 -translate-x-1/2 z-50 
        transition-all duration-500
        rounded-2xl px-6 py-3 
        w-[92%] max-w-6xl
        ${
          isScrolled 
            ? "shadow-[0_8px_50px_rgba(10,124,255,0.15)]" 
            : "shadow-none"
        }
      `}
      style={{
        background: isScrolled
          ? "rgba(255, 255, 255, 0.10)"
          : "rgba(255, 255, 255, 0.06)",
        backdropFilter: "blur(22px)",
        border: "1.5px solid rgba(255,255,255,0.22)",
        boxShadow:
          isScrolled
            ? "inset 0 1px 4px rgba(255,255,255,0.25), 0 12px 40px rgba(0,0,0,0.35)"
            : "inset 0 1px 4px rgba(255,255,255,0.15)",
      }}
    >
      <div className="flex items-center justify-between w-full">
        
        {/* Logo */}
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-primary to-accent flex items-center justify-center shadow-md shadow-primary/40">
            <span className="text-white font-bold text-lg">✦</span>
          </div>

          <span
            className="text-xl font-extrabold tracking-tight select-none"
          >
            TradeAI
          </span>
        </div>

        {/* Navigation */}
        <div className="hidden md:flex items-center gap-10">
          <a className="nav-link" href="#features">Features</a>
          <a className="nav-link" href="#how-it-works">How It Works</a>
        </div>

        {/* Auth */}
        <div className="flex items-center gap-4">
          <button
            onClick={() => onAuthClick("login")}
            className="text-foreground/70 hover:text-foreground transition-all"
          >
            Login
          </button>

          <button
            onClick={() => onAuthClick("signup")}
            className="
              px-6 py-2 rounded-full font-semibold 
              border transition-all
            "
            style={{
              background: "rgba(10, 124, 255, 0.06)",
              backdropFilter: "blur(20px)",
              borderColor: "rgba(10, 124, 255, 0.4)",
              boxShadow:
                isScrolled
                  ? "0 0 25px rgba(10,124,255,0.3)"
                  : "0 0 10px rgba(10,124,255,0.15)",
              color: "#0A7CFF",
            }}
          >
            Sign Up
          </button>
        </div>
      </div>
    </nav>
  )
}
