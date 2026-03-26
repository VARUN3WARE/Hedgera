"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { Loader2 } from "lucide-react"
import LandingPage from "@/components/pages/landing-page"
import { useAuth } from "@/hooks/use-auth"

export default function Home() {
  const { isAuthenticated, isChecking, user } = useAuth()
  const router = useRouter()

  useEffect(() => {
    if (!isChecking && isAuthenticated && user) {
      if (user.isOnboarded) {
        console.log("User onboarded, redirecting to dashboard")
        router.push("/dashboard")
      } else {
        console.log("User not onboarded, redirecting to onboarding")
        router.push("/onboarding")
      }
    }
  }, [isChecking, isAuthenticated, user, router])

  if (isChecking) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    )
  }

  // If we are here, user is either not authenticated or waiting for redirect
  if (!isAuthenticated) {
    console.log("User not authenticated, showing landing page")
    return <LandingPage />
  }

  // Return loader while redirecting
  return (
    <div className="min-h-screen flex items-center justify-center bg-background">
      <Loader2 className="w-8 h-8 animate-spin text-primary" />
    </div>
  )
}