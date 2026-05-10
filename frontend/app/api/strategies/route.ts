import { NextResponse } from "next/server";
import { getStrategies, getStrategyContribution, getStrategySignals } from "@/lib/queries";

export async function GET() {
  const strategies = await getStrategies();
  const contribution = await getStrategyContribution();
  const signals: Record<number, unknown> = {};
  for (const s of strategies) {
    signals[s.id] = await getStrategySignals(s.id, 50);
  }
  return NextResponse.json({ strategies, contribution, signals });
}
