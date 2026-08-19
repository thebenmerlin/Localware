import { getTickerPrices } from "@/lib/queries";

// Backs the positions-page hover sparkline and stock detail modal (see
// PositionsTreemap.tsx / StockDetailModal.tsx) — fetched lazily per ticker
// rather than bundled into the page's initial load, since most holdings
// are never hovered or opened in a given visit. `?days=` widens the window
// for the detail modal's full chart (default 30, used by the hover card).
export async function GET(req: Request, { params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;
  const daysParam = new URL(req.url).searchParams.get("days");
  const days = Math.min(1825, Math.max(1, Number(daysParam) || 30));
  const rows = await getTickerPrices(ticker, days);
  return Response.json(rows);
}
