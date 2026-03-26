interface HeroSectionProps {
  onSignUp: () => void
}

export default function HeroSection({ onSignUp }: HeroSectionProps) {
  return (
    <section className="relative pt-28 pb-20 px-6 lg:px-12 overflow-hidden">
      {/* Subtle background glow */}
      <div className="absolute inset-0 bg-blue-500/5">
        <div
          className="absolute top-1/3 left-1/2 w-[900px] h-[900px] -translate-x-1/2 rounded-full opacity-20 blur-3xl"
          style={{
            background:
              "radial-gradient(circle, rgba(10,124,255,0.25) 0%, rgba(10,124,255,0) 70%)",
          }}
        />
      </div>

      <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-16 items-center">
        {/* LEFT SECTION — TEXT */}
        <div className="text-left max-w-xl">
        

          {/* Headline */}
          <h1 className="text-5xl lg:text-6xl font-bold mb-6 leading-tight text-foreground">
            Automated Trading,
            <br />
            <span
              style={{
                background:
                  "linear-gradient(to right, #0A7CFF, #1f87ff, #0A7CFF)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
              }}
            >
              With 100% Transparency
            </span>
          </h1>

          {/* Subheading */}
          <p className="text-lg text-foreground/70 mb-10 leading-relaxed max-w-md">
            Stop guessing. Our AI co-pilot explains every trade using
            quant-models + real-time news intelligence.
          </p>

          {/* CTA Buttons */}
          <div className="flex gap-4">
            <button
              onClick={onSignUp}
              className="px-8 py-4 bg-primary text-white rounded-lg font-semibold hover:bg-primary/90 shadow-lg transition-all"
              style={{
                boxShadow:
                  "0 0 25px rgba(10,124,255,0.4), inset 0 0 15px rgba(10,124,255,0.1)",
              }}
            >
              Login
            </button>

            <button
              className="px-8 py-4 rounded-lg font-semibold text-foreground border transition-all"
              style={{
                background: "rgba(10,124,255,0.05)",
                borderColor: "rgba(10,124,255,0.3)",
                backdropFilter: "blur(20px)",
              }}
            >
              Watch Demo
            </button>
          </div>
        </div>

        {/* RIGHT SECTION — LOOPING VIDEO */}
        <div className="relative ml-20 ">
          <div
            className="absolute inset-0 rounded-3xl blur-3xl opacity-30"
            style={{
              background:
                "radial-gradient(circle, rgba(10,124,255,0.25) 0%, rgba(10,124,255,0) 70%)",
            }}
          />
          <div className="w-130 h-130">
            <img 
               src="/landing.png" 
               alt="Trading demo" 
               className="w-full h-full object-cover"
            />
          </div>
        </div>
      </div>
    </section>
  )
}
