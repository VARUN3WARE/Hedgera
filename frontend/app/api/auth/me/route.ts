import { cookies } from "next/headers";
import { NextResponse } from "next/server";

export async function GET() {
  const cookieStore = await cookies();
  const token = cookieStore.get("auth_token");

  if (!token) {
    return NextResponse.json({ message: "Unauthorized" }, { status: 401 });
  }

  // Verify the session with your backend
  try {
    const backendRes = await fetch(`${process.env.BACKEND_URL}/api/me`, {
      headers: {
        // Pass the token in the header so your backend can verify it
        Authorization: `Bearer ${token.value}`,
      },
    });

    if (!backendRes.ok) {
      return NextResponse.json(null, { status: 401 });
    }

    const data = await backendRes.json();
    return NextResponse.json(data);
  } catch (error) {
    return NextResponse.json(null, { status: 500 });
  }
}