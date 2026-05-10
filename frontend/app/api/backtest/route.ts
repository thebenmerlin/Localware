import { NextResponse } from "next/server";
import { getBacktests, getBacktest } from "@/lib/queries";

export async function GET(req: Request) {
  const url = new URL(req.url);
  const id = url.searchParams.get("id");
  if (id) {
    const bt = await getBacktest(Number(id));
    if (!bt) return NextResponse.json({ error: "not found" }, { status: 404 });
    return NextResponse.json({ backtest: bt });
  }
  const list = await getBacktests();
  return NextResponse.json({ backtests: list });
}
