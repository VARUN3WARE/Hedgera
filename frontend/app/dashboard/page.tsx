"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Loader2 } from "lucide-react"
import DashboardLayout from "@/components/dashboard-layout"
import PortfolioPage from "@/components/pages/portfolio-page"
import HistoryPage from "@/components/pages/history-page"
import SettingsPage from "@/components/pages/settings-page"
import PipelinePage from "@/components/pages/pipeline-page"
import { useAuth } from "@/hooks/use-auth"

type Page = "portfolio" | "history" | "settings" | "pipeline"

export default function DashboardRoute() {
  const { isAuthenticated, isChecking, user } = useAuth()
  const router = useRouter()
  const [currentPage, setCurrentPage] = useState<Page>("pipeline")

  useEffect(() => {
    if (!isChecking) {
      if (!isAuthenticated) {
        console.log(user);
        router.push("/")
      } else if (user && !user.isOnboarded) {
        console.log(user);
        router.push("/onboarding")
      }
    }
  }, [isChecking, isAuthenticated, user, router])

  if (isChecking || !isAuthenticated || (user && !user.isOnboarded)) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    )
  }

  const renderPage = () => {
    switch (currentPage) {
      case "pipeline": return <PipelinePage />
      case "portfolio": return <PortfolioPage />
      case "history": return <HistoryPage />
      case "settings": return <SettingsPage />
      default: return <PortfolioPage />
    }
  }

  return (
    <DashboardLayout currentPage={currentPage} onPageChange={setCurrentPage}>
      {renderPage()}
    </DashboardLayout>
  )
}