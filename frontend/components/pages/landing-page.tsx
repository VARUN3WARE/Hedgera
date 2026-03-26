"use client"

import { useState } from "react"
import Navbar from "@/components/landing/navbar"
import HeroSection from "@/components/landing/hero-section"
import FeaturesSection from "@/components/landing/features-section"
import HowItWorksSection from "@/components/landing/how-it-works-section"
import CtaSection from "@/components/landing/cta-section"
import Footer from "@/components/landing/footer"
import AuthModal from "@/components/auth/auth-modal"

export default function LandingPage() {
  const [authModal, setAuthModal] = useState<"closed" | "login" | "signup">(
    "closed"
  )

  return (
    <div className="bg-background min-h-screen">
      <Navbar onAuthClick={setAuthModal} />
      <main className="relative">
        <HeroSection onSignUp={() => setAuthModal("signup")} />
        <FeaturesSection />
        <HowItWorksSection />
        <CtaSection onSignUp={() => setAuthModal("signup")} />
      </main>
      <Footer />

      <AuthModal
        isOpen={authModal !== "closed"}
        mode={authModal as "login" | "signup"}
        onClose={() => setAuthModal("closed")}
        onSwitchMode={(mode) => setAuthModal(mode)}
      />
    </div>
  )
}
