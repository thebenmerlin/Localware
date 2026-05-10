import { ReactNode } from "react";

export function PaperTable({
  number,
  title,
  caption,
  children,
}: {
  number?: string | number;
  title?: string;
  caption?: ReactNode;
  children: ReactNode;
}) {
  return (
    <figure className="my-6">
      {(number !== undefined || title) && (
        <div className="caption mb-1 not-italic">
          {number !== undefined && <span className="table-num text-ink">Table&nbsp;{number}.</span>}
          {title && <span className="text-ink font-medium">&nbsp;{title}</span>}
        </div>
      )}
      <table className="paper">{children}</table>
      {caption && <figcaption className="caption">{caption}</figcaption>}
    </figure>
  );
}
