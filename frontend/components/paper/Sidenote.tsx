import { ReactNode } from "react";

export function Sidenote({ children, num }: { children: ReactNode; num?: number }) {
  return (
    <aside className="sidenote my-2 lg:absolute lg:right-[-14rem] lg:w-[12rem] lg:my-0 lg:not-italic lg:text-[0.75rem]">
      {num !== undefined && <sup className="not-italic font-mono mr-1">{num}</sup>}
      {children}
    </aside>
  );
}
