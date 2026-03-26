import { cookies } from "next/headers";
import { NextResponse } from "next/server";

export async function POST() {
  const cookieStore = await cookies();
  
  // Simply delete the secure cookie to log out
  cookieStore.delete("auth_token");

  return NextResponse.json({ success: true });
}