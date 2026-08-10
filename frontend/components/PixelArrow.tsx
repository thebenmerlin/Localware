type Direction = "up" | "down" | "flat";

/**
 * A small stepped/blocky arrow glyph — built from square <rects> stacked
 * into a staircase silhouette, not a smooth triangle or a full pixel font.
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

  // Ascending 4-step staircase, apex in the center — read bottom to top.
  // Mirrored vertically for "down" via a transform rather than a second path.
  const steps = [
    { x: 0, y: 12, w: 3, h: 3 },
    { x: 3, y: 9, w: 3, h: 3 },
    { x: 6, y: 6, w: 3, h: 3 },
    { x: 9, y: 9, w: 3, h: 3 },
    { x: 12, y: 12, w: 3, h: 3 },
  ];

  return (
    <svg
      viewBox="0 0 15 15"
      className={className}
      shapeRendering="crispEdges"
      aria-hidden="true"
      style={direction === "down" ? { transform: "scaleY(-1)" } : undefined}
    >
      {steps.map((s, i) => (
        <rect key={i} x={s.x} y={s.y} width={s.w} height={s.h} fill="currentColor" />
      ))}
    </svg>
  );
}
