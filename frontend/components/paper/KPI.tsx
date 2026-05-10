import { ReactNode } from "react";
import clsx from "clsx";

export function KPI({
  label,
  value,
  sub,
  tone,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  tone?: "neutral" | "positive" | "negative";
}) {
  return (
    <div className="kpi-card">
      <div className="label">{label}</div>
      <div
        className={clsx(
          "value",
          tone === "positive" && "text-positive",
          tone === "negative" && "text-negative",
        )}
      >
        {value}
      </div>
      {sub && <div className="sub">{sub}</div>}
    </div>
  );
}
