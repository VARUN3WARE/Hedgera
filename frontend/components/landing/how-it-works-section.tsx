import { Cog, Radio, BrainCircuit, CheckCircle } from "lucide-react"

interface Step {
  number: number
  title: string
  description: string
  icon: React.ReactNode
}


const steps: Step[] = [
  {
    number: 1,
    title: "Set Your Strategy",
    description:
      "Connect your brokerage account and tell the AI your goals and risk tolerance.",
    icon: <Cog className="w-10 h-10 text-accent" />,
  },
  {
    number: 2,
    title: "Real-Time Data Ingestion",
    description:
      "Our engine scans the market 24/7, ingesting prices, news, discussions, and sentiment scores.",
    icon: <Radio className="w-10 h-10 text-accent" />,
  },
  {
    number: 3,
    title: "Hybrid-Consensus Analysis",
    description:
      "Quantitative and qualitative models analyze data, then Bull vs. Bear agents debate the trade.",
    icon: <BrainCircuit className="w-10 h-10 text-accent" />,
  },
  {
    number: 4,
    title: "Execute & Explain",
    description:
      "Trade executes if consensus is reached and matches your rules. You get instant pros and cons.",
    icon: <CheckCircle className="w-10 h-10 text-accent" />,
  },
]


export default function HowItWorksSection() {
  return (
    <section
      id="how-it-works"
      className="py-20 px-4 sm:px-6 lg:px-8 relative overflow-hidden"
    >
      <div className="absolute inset-0 -z-10">
        <div className="absolute bottom-0 right-0 w-96 h-96 bg-accent/10 rounded-full blur-3xl opacity-20"></div>
      </div>

      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-16">
          <h2 className="text-4xl md:text-5xl font-bold mb-4 text-balance">
            How It{" "}
            <span
              style={{
                background: 'linear-gradient(to right, #0A7CFF, #1f87ff, #0A7CFF)',
                WebkitBackgroundClip: 'text',
                WebkitTextFillColor: 'transparent',
                backgroundClip: 'text',
              }}
            >
              Works
            </span>
          </h2>
          <p className="text-lg text-foreground/60 max-w-2xl mx-auto">
            A seamless process from strategy setup to intelligent execution
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 md:gap-2">
          {steps.map((step, index) => (
           <div
  key={index}
  className="
    relative p-8 rounded-3xl text-center cursor-pointer
    transition-all duration-500

    /* Glass Base */
    bg-gradient-to-br from-white/10 via-white/5 to-white/0
    backdrop-blur-3xl

    /* Borders */
    border border-white/20
    [box-shadow:inset_0_0_0_1px_rgba(255,255,255,0.05)]

    /* Depth Shadow */
    shadow-[0_8px_40px_rgba(0,0,0,0.35)]

    /* Hover Glow */
    hover:shadow-[0_12px_60px_rgba(0,150,255,0.45)]
    hover:-translate-y-1
  "
>
  {/* Top glass shine */}
  <div className="absolute inset-0 rounded-3xl pointer-events-none 
    bg-gradient-to-br from-white/15 to-transparent opacity-50"></div>

  {/* Bottom vignette */}
  <div className="absolute inset-0 rounded-3xl pointer-events-none 
    bg-gradient-to-b from-transparent via-black/10 to-black/20"></div>

  {/* Content */}
  <div className="relative flex justify-center mb-4">
    {step.icon}
  </div>

  <div className="relative inline-block mb-3 px-3 py-1 rounded-full 
      bg-white/10 border border-white/20 text-white text-sm font-semibold 
      backdrop-blur-xl">
    Step {step.number}
  </div>

  <h3 className="relative text-lg font-semibold mb-2 text-white">
    {step.title}
  </h3>

  <p className="relative text-sm text-white/70 leading-relaxed">
    {step.description}
  </p>
</div>

          ))}
        </div>
      </div>
    </section>
  )
}
