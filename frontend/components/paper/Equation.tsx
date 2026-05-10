"use client";

import { useEffect, useRef } from "react";
import katex from "katex";

export function Equation({
  tex,
  number,
  display = true,
}: {
  tex: string;
  number?: string | number;
  display?: boolean;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    if (ref.current) {
      katex.render(tex, ref.current, {
        displayMode: display,
        throwOnError: false,
        strict: "ignore",
      });
    }
  }, [tex, display]);
  if (!display) {
    return <span ref={ref} />;
  }
  return (
    <div className="equation-block">
      <span ref={ref} />
      {number && <span className="eq-num">({number})</span>}
    </div>
  );
}

export function InlineEq({ tex }: { tex: string }) {
  return <Equation tex={tex} display={false} />;
}
