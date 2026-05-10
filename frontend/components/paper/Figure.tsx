import { ReactNode } from "react";

export function Figure({
  number,
  title,
  caption,
  children,
}: {
  number: string | number;
  title?: string;
  caption: ReactNode;
  children: ReactNode;
}) {
  return (
    <figure className="my-6">
      <div className="border-t border-b border-ink/20 py-3">{children}</div>
      <figcaption className="caption mt-1.5">
        <span className="figure-num text-ink not-italic">Figure&nbsp;{number}.</span>
        {title ? <span className="not-italic font-medium text-ink">&nbsp;{title}.</span> : null}
        &nbsp;{caption}
      </figcaption>
    </figure>
  );
}
