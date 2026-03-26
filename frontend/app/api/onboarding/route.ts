import { cookies } from "next/headers";
import { NextResponse } from "next/server";

export async function POST(request: Request) {
  const cookieStore = await cookies();
  const token = cookieStore.get("auth_token");

  if (!token) {
    return NextResponse.json({ message: "Unauthorized" }, { status: 401 });
  }

  try {
    const body = await request.json();

    // Forward data to your backend's /api/onboarding endpoint
    const backendRes = await fetch(`${process.env.BACKEND_URL}/api/onboarding`, {
      method: "POST",
      headers: { 
        "Content-Type": "application/json",
        Authorization: `Bearer ${token.value}`
      },
      body: JSON.stringify(body),
    });

    if (!backendRes.ok) {
      const error = await backendRes.json();
      return NextResponse.json(
        { message: error.message || "Failed to save onboarding data" },
        { status: backendRes.status }
      );
    }

    const data = await backendRes.json();
    return NextResponse.json(data);
  } catch (error) {
    return NextResponse.json(
      { message: "Internal server error" },
      { status: 500 }
    );
  }
}