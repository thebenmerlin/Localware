import { NextResponse } from "next/server";
import { getRiskLatest, getRiskHistory } from "@/lib/queries";

export async function GET() {
  const [latest, history] = await Promise.all([getRiskLatest(), getRiskHistory(252)]);
  return NextResponse.json({ latest, history });
}
