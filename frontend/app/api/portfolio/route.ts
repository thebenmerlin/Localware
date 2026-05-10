import { NextResponse } from "next/server";
import { getLatestNav, getCurrentPositions, getSectorExposure } from "@/lib/queries";

export async function GET() {
  const [nav, positions, sectors] = await Promise.all([
    getLatestNav(),
    getCurrentPositions(),
    getSectorExposure(),
  ]);
  return NextResponse.json({ nav, positions, sectors });
}
