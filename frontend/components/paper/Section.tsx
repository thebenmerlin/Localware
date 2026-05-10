import { ReactNode } from "react";

export function Abstract({ children }: { children: ReactNode }) {
  return (
    <div className="my-6 mx-auto max-w-[34rem] text-center">
      <div className="smallcaps mb-1">Abstract</div>
      <div className="text-[0.95rem] leading-[1.5] text-ink/85 italic">{children}</div>
    </div>
  );
}

export function Lede({ children }: { children: ReactNode }) {
  return (
    <p className="text-[1.15rem] leading-[1.55] mt-2 mb-6 text-ink/85" style={{ fontStyle: "italic" }}>
      {children}
    </p>
  );
}
