import { getTickerPrices } from "@/lib/queries";

// Backs the positions-page hover sparkline (see HoldingsTable.tsx) — fetched
// lazily per ticker on hover rather than bundled into the page's initial
// load, since most holdings are never hovered in a given visit.
export async function GET(_req: Request, { params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;
  const rows = await getTickerPrices(ticker, 30);
  return Response.json(rows);
}
