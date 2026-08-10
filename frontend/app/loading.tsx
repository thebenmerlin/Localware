export default function Loading() {
  return (
    <main className="min-h-screen flex flex-col items-center justify-center px-6 py-24">
      <div className="w-full max-w-dashboard flex flex-col items-center animate-pulse">
        <div className="h-4 w-16 rounded bg-[var(--faint)] opacity-30" />
        <div className="mt-4 h-24 md:h-32 w-2/3 rounded bg-[var(--faint)] opacity-20" />
        <div className="mt-6 h-5 w-40 rounded bg-[var(--faint)] opacity-20" />

        <div className="mt-20 md:mt-24 grid grid-cols-1 md:grid-cols-3 gap-12 md:gap-16 w-full">
          {[0, 1, 2].map((i) => (
            <div key={i} className="flex flex-col items-center">
              <div className="h-3 w-20 rounded bg-[var(--faint)] opacity-30" />
              <div className="mt-2 h-10 w-24 rounded bg-[var(--faint)] opacity-20" />
            </div>
          ))}
        </div>
      </div>
    </main>
  );
}
