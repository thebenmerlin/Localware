type Direction = "up" | "down" | "flat";

/**
 * A small stemmed arrow glyph — a blocky chevron head sitting on a vertical
 * stick, built from square <rects> (not a smooth path, not a pixel font).
 * Sits inline next to a serif numeral as a restrained "this moved" accent.
 * Color is inherited via currentColor from the parent (see BigStat's tone).
 */
export function PixelArrow({ direction, className }: { direction: Direction; className?: string }) {
  if (direction === "flat") {
    return (
      <svg viewBox="0 0 15 15" className={className} shapeRendering="crispEdges" aria-hidden="true">
        <rect x="1" y="6" width="13" height="3" fill="currentColor" />
      </svg>
    );
  }

  // Chevron head (3-step staircase, apex centered) on top of a stick —
  // reads as "arrow" at a glance while staying built from square blocks.
  const head = [
    { x: 0, y: 6, w: 3, h: 3 },
    { x: 3, y: 3, w: 3, h: 3 },
    { x: 6, y: 0, w: 3, h: 3 },
    { x: 9, y: 3, w: 3, h: 3 },
    { x: 12, y: 6, w: 3, h: 3 },
  ];
  const stick = { x: 6, y: 9, w: 3, h: 6 };

  return (
    <svg
      viewBox="0 0 15 15"
      className={className}
      shapeRendering="crispEdges"
      aria-hidden="true"
      style={direction === "down" ? { transform: "scaleY(-1)" } : undefined}
    >
      {head.map((s, i) => (
        <rect key={i} x={s.x} y={s.y} width={s.w} height={s.h} fill="currentColor" />
      ))}
      <rect x={stick.x} y={stick.y} width={stick.w} height={stick.h} fill="currentColor" />
    </svg>
  );
}
