import { cookies } from "next/headers";
import { NextResponse } from "next/server";

export async function POST(request: Request) {
  try {
    const body = await request.json();

    // 1. Forward the login request to your ACTUAL backend
    const backendRes = await fetch(`${process.env.BACKEND_URL}/api/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const data = await backendRes.json();

    if (!backendRes.ok) {
      return NextResponse.json(
        { message: data.message || "Login failed" },
        { status: backendRes.status }
      );
    }

    // 2. Take the token from your backend and set it as an HTTP-Only cookie
    // This keeps it safe from hackers (XSS attacks)
    const cookieStore = await cookies();
    cookieStore.set("auth_token", data.token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
      maxAge: 60 * 60 * 24 * 7, // 1 week
      path: "/",
    });

    return NextResponse.json({ success: true, user: data.user });
  } catch (error) {
    return NextResponse.json(
      { message: "Internal server error" },
      { status: 500 }
    );
  }
}