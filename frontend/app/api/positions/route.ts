import { NextResponse } from "next/server";
import { getCurrentPositions, getSectorExposure } from "@/lib/queries";

export async function GET() {
  const [positions, sectors] = await Promise.all([getCurrentPositions(), getSectorExposure()]);
  return NextResponse.json({ positions, sectors });
}
