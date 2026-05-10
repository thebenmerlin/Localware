import { NextResponse } from "next/server";
import { getEquityCurve, getAllMetrics, getDrawdownSeries, getMonthlyReturns, getRollingSharpe } from "@/lib/queries";

export async function GET() {
  const [equity, metrics, drawdown, monthly, rollingSharpe] = await Promise.all([
    getEquityCurve(),
    getAllMetrics(),
    getDrawdownSeries(),
    getMonthlyReturns(),
    getRollingSharpe(63),
  ]);
  return NextResponse.json({ equity, metrics, drawdown, monthly, rollingSharpe });
}
