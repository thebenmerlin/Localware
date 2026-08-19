export interface TreemapItem {
  id: string;
  value: number;
}

export interface TreemapRect extends TreemapItem {
  x: number;
  y: number;
  w: number;
  h: number;
}

/** Worst aspect ratio among a row of `areas` laid along a strip of `length`. */
function worstRatio(areas: number[], length: number): number {
  const sum = areas.reduce((a, b) => a + b, 0);
  const max = Math.max(...areas);
  const min = Math.min(...areas);
  const lenSq = length * length;
  return Math.max((lenSq * max) / (sum * sum), (sum * sum) / (lenSq * min));
}

/**
 * Squarified treemap (Bruls, Huizing, van Wijk 1999): lays `items` into a
 * `w`x`h` rect at (`x`,`y`), area-proportional to `value`, preferring
 * near-square tiles over long slivers. Coordinates are in the same units as
 * `x`/`y`/`w`/`h` — callers normalize to percentages when rendering, and can
 * feed a child rect straight back in as the container for a nested level.
 */
export function squarify(items: TreemapItem[], x: number, y: number, w: number, h: number): TreemapRect[] {
  const sorted = items.filter((i) => i.value > 0).sort((a, b) => b.value - a.value);
  if (sorted.length === 0 || w <= 0 || h <= 0) return [];

  const total = sorted.reduce((s, i) => s + i.value, 0);
  const areaScale = (w * h) / total;
  const scaled = sorted.map((i) => ({ ...i, area: i.value * areaScale }));

  const result: TreemapRect[] = [];
  let remaining = scaled;
  let rx = x;
  let ry = y;
  let rw = w;
  let rh = h;

  while (remaining.length > 0) {
    const length = Math.min(rw, rh);
    let row: typeof scaled = [];
    let i = 0;
    while (i < remaining.length) {
      const candidate = [...row, remaining[i]];
      if (row.length === 0 || worstRatio(candidate.map((r) => r.area), length) <= worstRatio(row.map((r) => r.area), length)) {
        row = candidate;
        i++;
      } else {
        break;
      }
    }

    const rowArea = row.reduce((s, r) => s + r.area, 0);
    const layoutWide = rw >= rh;
    if (layoutWide) {
      const rowW = rowArea / rh;
      let cy = ry;
      for (const r of row) {
        const itemH = r.area / rowW;
        result.push({ id: r.id, value: r.value, x: rx, y: cy, w: rowW, h: itemH });
        cy += itemH;
      }
      rx += rowW;
      rw -= rowW;
    } else {
      const rowH = rowArea / rw;
      let cx = rx;
      for (const r of row) {
        const itemW = r.area / rowH;
        result.push({ id: r.id, value: r.value, x: cx, y: ry, w: itemW, h: rowH });
        cx += itemW;
      }
      ry += rowH;
      rh -= rowH;
    }
    remaining = remaining.slice(row.length);
  }

  return result;
}
