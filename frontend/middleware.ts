import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

export function middleware(request: NextRequest) {
  const token = request.cookies.get('auth_token')
  
  // Protect routes that require authentication
  if (!token) {
    if (request.nextUrl.pathname.startsWith('/onboarding') || 
        request.nextUrl.pathname.startsWith('/dashboard')) {
      return NextResponse.redirect(new URL('/', request.url))
    }
  }

  return NextResponse.next()
}

export const config = {
  matcher: [
    '/onboarding/:path*',
    '/dashboard/:path*', // Add dashboard to matcher
  ],
}