import React from "react"
import { BarChart3, Bot, Scale, Target } from "lucide-react"

interface Feature {
  title: string
  description: string
  icon: React.ReactNode
}

const features: Feature[] = [
  {
    title: "Explainable Trades",
    description:
      "Simple, human-readable reports for every trade. We show you the Pros and Cons from our AI's analysis — full transparency.",
    icon: <BarChart3 className="w-10 h-10 text-accent" />,
  },
  {
    title: "Hybrid AI Consensus",
    description:
      "Two independent models work together: Deep RL price-action model and Agentic qualitative news/sentiment model.",
    icon: <Bot className="w-10 h-10 text-accent" />,
  },
  {
    title: "The AI Debate Team",
    description:
      "A Bull and Bear AI agent debate every potential investment, surfacing hidden risks and opportunities.",
    icon: <Scale className="w-10 h-10 text-accent" />,
  },
  {
    title: "Your Risk, Your Rules",
    description:
      "Set your risk tolerance, max drawdown, and profit goals. Trades only execute if they match your rules.",
    icon: <Target className="w-10 h-10 text-accent" />,
  },
]

export default function FeaturesSection() {
  return (
    <section
      id="features"
      className="py-24 px-4 sm:px-6 lg:px-8 relative overflow-hidden"
    >
      {/* Soft background glow */}
      <div className="absolute inset-0 -z-10">
        <div className="absolute -top-10 left-0 w-[450px] h-[450px] bg-primary/10 rounded-full blur-3xl opacity-30"></div>
        <div className="absolute bottom-0 right-0 w-[500px] h-[500px] bg-blue-500/10 rounded-full blur-[120px] opacity-20"></div>
      </div>

      <div className="max-w-6xl mx-auto">
        {/* Heading */}
        <div className="text-center mb-16">
          <h2 className="text-4xl md:text-5xl font-bold mb-4">
            Powerful Features for{" "}
            <span
              className="bg-gradient-to-r from-[#0A7CFF] via-[#1f87ff] to-[#0A7CFF] bg-clip-text text-transparent"
            >
              Smart Trading
            </span>
          </h2>
          <p className="text-lg text-foreground/60 max-w-2xl mx-auto">
            Everything you need for transparent, AI-powered automated trading
          </p>
        </div>

        {/* Glass Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {features.map((feature, index) => (
            <div
  key={index}
  className="
    relative p-8 rounded-3xl cursor-pointer
    transition-all duration-500

    /* True Glass Background */
    bg-gradient-to-br from-white/10 via-white/5 to-white/0
    backdrop-blur-3xl

    /* Borders */
    border border-white/20
    [box-shadow:inset_0_0_0_1px_rgba(255,255,255,0.05)]

    /* Smooth Glow */
    shadow-[0_8px_40px_rgba(0,0,0,0.35)]

    /* Hover Glow & Lift */
    hover:shadow-[0_12px_60px_rgba(0,150,255,0.45)]
    hover:-translate-y-1
  "
>
  {/* Subtle glass highlight */}
  <div className="absolute inset-0 rounded-3xl pointer-events-none 
    bg-gradient-to-br from-white/10 to-transparent opacity-50">
  </div>

  {/* Soft vignette like image */}
  <div className="absolute inset-0 rounded-3xl pointer-events-none 
    bg-gradient-to-b from-transparent via-black/10 to-black/20">
  </div>

  <div className="relative mb-5 opacity-90 group-hover:opacity-100 transition">
    {feature.icon}
  </div>

  <h3 className="relative text-xl font-semibold mb-3 text-white">
    {feature.title}
  </h3>

  <p className="relative text-white/70 leading-relaxed">
    {feature.description}
  </p>
</div>

          ))}
        </div>
      </div>
    </section>
  )
}
