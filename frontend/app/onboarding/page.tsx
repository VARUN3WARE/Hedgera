// "use client"

// import { useState } from "react"
// import { useRouter } from 'next/navigation'
// import OnboardingPage from "@/components/pages/onboarding-page"

// export default function OnboardingRoute() {
//   return <OnboardingPage />
// }
"use client"

import { useEffect } from "react"
import { useRouter } from 'next/navigation'
import OnboardingPage from "@/components/pages/onboarding-page"
import { useAuth } from "@/hooks/use-auth"
import { Loader2 } from "lucide-react"

export default function OnboardingRoute() {
  const { isAuthenticated, isChecking, user } = useAuth()
  const router = useRouter()

  useEffect(() => {
    if (!isChecking) {
      if (!isAuthenticated) {
        console.log(user);
        router.push("/")
      } else if (user && user.isOnboarded) {
        router.push("/dashboard")
      }
      console.log(user);
      console.log(user?.isOnboarded);
      console.log(isAuthenticated);
      console.log(isChecking);
      console.log("Onboarding Route Loaded");
    }
  }, [isChecking, isAuthenticated, user, router])

  if (isChecking || !isAuthenticated || (user && user.isOnboarded)) {
     return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    )
  }

  return <OnboardingPage />
}