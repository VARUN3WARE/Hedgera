interface CtaSectionProps {
  onSignUp: () => void
}

export default function CtaSection({ onSignUp }: CtaSectionProps) {
  return (
    <section className="py-20 px-4 sm:px-6 lg:px-8">
      <div className="max-w-4xl mx-auto">
        <div 
          className="rounded-2xl p-12 md:p-16 relative overflow-hidden group"
          style={{
            background: 'linear-gradient(to bottom right, rgba(10, 124, 255, 0.1), rgba(10, 124, 255, 0.05), rgba(10, 124, 255, 0.05))',
            backdropFilter: 'blur(20px)',
            border: '1px solid rgba(10, 124, 255, 0.2)',
          }}
        >
          {/* Glow effect */}
          <div className="absolute inset-0 -z-10 opacity-0 group-hover:opacity-100 transition-opacity duration-300">
            <div className="absolute top-0 right-0 w-96 h-96 bg-accent/20 rounded-full blur-3xl"></div>
          </div>

          <div className="relative z-10 text-center">
            <h2 className="text-4xl md:text-5xl font-bold mb-4 text-balance">
              Stop Trading in the{" "}
              <span
                style={{
                  background: 'linear-gradient(to right, #0A7CFF, #1f87ff, #0A7CFF)',
                  WebkitBackgroundClip: 'text',
                  WebkitTextFillColor: 'transparent',
                  backgroundClip: 'text',
                }}
              >
                Dark
              </span>
            </h2>
            <p className="text-lg md:text-xl text-foreground/70 mb-8 max-w-2xl mx-auto leading-relaxed">
              Join the new era of transparent, automated investing. Sign up for
              your free trial and see what our AI co-pilot can do for your
              portfolio.
            </p>
            <button
              onClick={onSignUp}
              className="px-8 py-4 bg-primary text-white rounded-lg font-semibold hover:bg-primary/90 shadow-lg hover:shadow-xl transition-all text-lg"
              style={{
                boxShadow: '0 0 30px rgba(10, 124, 255, 0.4), 0 0 60px rgba(10, 124, 255, 0.2), inset 0 0 30px rgba(10, 124, 255, 0.08)',
              }}
            >
              Sign Up and Start Your Free Trial
            </button>
          </div>
        </div>
      </div>
    </section>
  )
}
