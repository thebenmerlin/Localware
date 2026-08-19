"use client";

import { useEffect, useRef } from "react";

interface Particle {
  x: number;
  y: number;
  r: number;
  vx: number;
  vy: number;
  o: number;
}

/**
 * A sparse field of slow-drifting dots behind the page content, fixed to
 * the viewport. Color is read from `--faint` so it stays in gamut across
 * light/dark/terminal without per-theme branching. Static (single paint,
 * no rAF loop) under prefers-reduced-motion.
 */
export function Particles() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const ctx = canvas?.getContext("2d");
    if (!canvas || !ctx) return;

    const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    let width = (canvas.width = window.innerWidth);
    let height = (canvas.height = window.innerHeight);
    let color = getComputedStyle(document.documentElement).getPropertyValue("--faint").trim() || "#a8a79e";

    function resize() {
      width = canvas!.width = window.innerWidth;
      height = canvas!.height = window.innerHeight;
    }
    window.addEventListener("resize", resize);

    // Re-read the color when the theme toggle flips data-theme.
    const observer = new MutationObserver(() => {
      color = getComputedStyle(document.documentElement).getPropertyValue("--faint").trim() || color;
      if (reduceMotion) paint();
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });

    const count = Math.max(20, Math.min(70, Math.floor((width * height) / 26000)));
    const particles: Particle[] = Array.from({ length: count }, () => ({
      x: Math.random() * width,
      y: Math.random() * height,
      r: Math.random() * 1.3 + 0.4,
      vx: (Math.random() - 0.5) * 0.05,
      vy: -(Math.random() * 0.12 + 0.03),
      o: Math.random() * 0.35 + 0.08,
    }));

    function paint() {
      ctx!.clearRect(0, 0, width, height);
      for (const p of particles) {
        ctx!.beginPath();
        ctx!.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx!.fillStyle = color;
        ctx!.globalAlpha = p.o;
        ctx!.fill();
      }
    }

    let raf = 0;
    function tick() {
      for (const p of particles) {
        p.x += p.vx;
        p.y += p.vy;
        if (p.y < -10) p.y = height + 10;
        if (p.x < -10) p.x = width + 10;
        if (p.x > width + 10) p.x = -10;
      }
      paint();
      raf = requestAnimationFrame(tick);
    }

    if (reduceMotion) {
      paint();
    } else {
      tick();
    }

    return () => {
      window.removeEventListener("resize", resize);
      observer.disconnect();
      cancelAnimationFrame(raf);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      aria-hidden="true"
      className="fixed inset-0 z-0 pointer-events-none"
    />
  );
}
