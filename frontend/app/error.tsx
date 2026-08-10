"use client";

export default function Error({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <div>
      <div className="font-display text-2xl">Couldn't load latest figures.</div>
      <button
        onClick={() => reset()}
        className="mt-6 font-mono text-xs uppercase tracking-wide text-[var(--muted)] underline"
      >
        retry
      </button>
    </div>
  );
}
