import { NextResponse } from "next/server";
import { getRecentTrades } from "@/lib/queries";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const limit = Number(url.searchParams.get("limit") || 200);
  const trades = await getRecentTrades(limit);
  return NextResponse.json({ trades });
}
