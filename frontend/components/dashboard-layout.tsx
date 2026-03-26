"use client"

import type React from "react"
import { BarChart3, TrendingUp, History, Settings, LogOut,Activity } from 'lucide-react'
import { ThemeToggle } from "./theme-toggle"
import { useAuth } from "@/hooks/use-auth"

interface DashboardLayoutProps {
  children: React.ReactNode
  currentPage: string
  onPageChange: (page: "portfolio" | "history" | "settings" | "pipeline") => void
}

export default function DashboardLayout({ children, currentPage, onPageChange }: DashboardLayoutProps) {
  // Use the auth hook for logout logic
  const { logout } = useAuth()
  
  const navItems = [
    { id: "pipeline", label: "Live Ops", icon: Activity },
    { id: "portfolio", label: "Portfolio", icon: BarChart3 },
    { id: "history", label: "History", icon: History },
    { id: "settings", label: "Settings", icon: Settings },
  ]

  const handleLogout = async () => {
    await logout()
  }

  return (
    <div className="flex h-screen bg-background text-foreground">
      {/* Sidebar */}
      <div 
        className="w-64 border-r border-primary/20 flex flex-col"
        style={{
          background: 'rgba(10, 124, 255, 0.06)',
          backdropFilter: 'blur(25px)',
          boxShadow: '0 0 40px rgba(10, 124, 255, 0.15), inset 0 1px 0 rgba(255, 255, 255, 0.1)',
        }}
      >
        {/* Logo section */}
        <div className="p-6 border-b border-primary/20">
          <div className="flex items-center gap-3">
            <div 
              className="w-10 h-10 rounded-lg bg-gradient-to-br from-primary to-blue-500 flex items-center justify-center"
              style={{
                boxShadow: '0 0 20px rgba(10, 124, 255, 0.3), 0 0 40px rgba(10, 124, 255, 0.15), inset 0 0 20px rgba(10, 124, 255, 0.05)',
              }}
            >
              <TrendingUp className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 
                className="text-xl font-bold"
                style={{
                  background: 'linear-gradient(to right, #0A7CFF, #1f87ff, #0A7CFF)',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent',
                  backgroundClip: 'text',
                }}
              >
                TradeAI
              </h1>
              <p className="text-xs text-foreground/50">Premium Trading</p>
            </div>
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 p-4 space-y-2">
          {navItems.map((item) => {
            const Icon = item.icon
            const isActive = currentPage === item.id
            return (
              <button
                key={item.id}
                onClick={() => onPageChange(item.id as any)}
                className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all text-sm font-medium border-2 ${
                  isActive
                    ? "text-primary"
                    : "text-foreground/60 hover:text-foreground"
                }`}
                style={isActive ? {
                  background: 'rgba(10, 124, 255, 0.2)',
                  borderColor: 'rgba(10, 124, 255, 0.4)',
                  boxShadow: '0 0 20px rgba(10, 124, 255, 0.3), 0 0 40px rgba(10, 124, 255, 0.15), inset 0 0 20px rgba(10, 124, 255, 0.05)',
                } : {
                  background: 'transparent',
                  borderColor: 'transparent',
                }}
              >
                <Icon className="w-5 h-5" />
                {item.label}
              </button>
            )
          })}
        </nav>

        <div className="p-4 border-t border-primary/20">
          <button 
            onClick={handleLogout}
            className="w-full flex items-center gap-3 px-4 py-3 rounded-lg text-foreground/60 hover:text-destructive transition-all text-sm font-medium hover:bg-destructive/10"
          >
            <LogOut className="w-5 h-5" />
            Logout
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-auto">
        {/* Header */}
        <div 
          className="border-b border-primary/20 px-8 py-6"
          style={{
            background: 'rgba(10, 124, 255, 0.06)',
            backdropFilter: 'blur(25px)',
          }}
        >
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-3xl font-bold text-foreground">Trading Dashboard</h2>
              <p className="text-sm text-foreground/60 mt-2">AI-Powered Trading with Complete Transparency</p>
            </div>
            <div className="flex items-center gap-6">
              <ThemeToggle />
              <div className="text-right">
                <p className="text-xs text-foreground/60">Portfolio Value</p>
                <p 
                  className="text-3xl font-bold"
                  style={{
                    background: 'linear-gradient(to right, #0A7CFF, #1f87ff, #0A7CFF)',
                    WebkitBackgroundClip: 'text',
                    WebkitTextFillColor: 'transparent',
                    backgroundClip: 'text',
                  }}
                >
                  $145,230.50
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Page content */}
        <div className="p-8">{children}</div>
      </div>
    </div>
  )
}