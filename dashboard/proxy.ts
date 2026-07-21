import { NextResponse, type NextRequest } from "next/server";
import { authorized, authChallenge } from "@/lib/auth";

export function proxy(request: NextRequest) {
  if (!authorized(request)) return authChallenge();
  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
