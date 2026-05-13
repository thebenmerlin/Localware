import { NextResponse } from "next/server";
import { getTickerPrices } from "@/lib/queries";

export const revalidate = 300;

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ ticker: string }> },
) {
  const { ticker } = await params;
  const rows = await getTickerPrices(ticker, 30);
  const prices = rows
    .map((r) => ({ date: String(r.date).slice(0, 10), close: Number(r.close) }))
    .reverse();
  return NextResponse.json({ ticker: ticker.toUpperCase(), prices });
}
