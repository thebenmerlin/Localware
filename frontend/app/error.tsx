"use client";

export default function Error({ reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <main className="min-h-screen flex flex-col items-center justify-center px-6 py-24 text-center">
      <div className="font-display text-2xl">Couldn't load latest figures.</div>
      <button
        onClick={() => reset()}
        className="mt-6 font-mono text-xs uppercase tracking-wide text-[var(--muted)] underline"
      >
        retry
      </button>
    </main>
  );
}
