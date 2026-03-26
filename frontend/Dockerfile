# ═══════════════════════════════════════════════════════════════
# AEGIS Trading System - Frontend Dockerfile
# Next.js 14 with TypeScript and Tailwind CSS
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# Stage 1: Dependencies
# ═══════════════════════════════════════════════════════════════
FROM node:20-alpine AS deps

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy package files
COPY package.json pnpm-lock.yaml* ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# ═══════════════════════════════════════════════════════════════
# Stage 2: Builder
# ═══════════════════════════════════════════════════════════════
FROM node:20-alpine AS builder

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy dependencies from deps stage
COPY --from=deps /app/node_modules ./node_modules

# Copy all source code
COPY . .

# Build environment variables
ENV NEXT_PUBLIC_API_URL=http://localhost:8000
ENV NEXT_TELEMETRY_DISABLED=1

# Build Next.js application
RUN pnpm build

# ═══════════════════════════════════════════════════════════════
# Stage 3: Runner (Production)
# ═══════════════════════════════════════════════════════════════
FROM node:20-alpine AS runner

WORKDIR /app

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

# Create non-root user
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy package files and install production dependencies only
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/pnpm-lock.yaml* ./pnpm-lock.yaml
RUN pnpm install --prod --frozen-lockfile

# Copy built application
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/next.config.mjs ./next.config.mjs

# Set ownership
RUN chown -R nextjs:nodejs /app

# Switch to non-root user
USER nextjs

# Expose port
EXPOSE 3000

# Environment variables
ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

# Start Next.js
CMD ["pnpm", "start"]
