// Inlined into <head> as a non-async script so the theme is applied before
// the first paint, eliminating the white-flash on a dark-mode reload.
export function ThemeScript() {
  const code = `
    try {
      var t = localStorage.getItem("theme");
      if (!t) {
        t = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
      }
      document.documentElement.setAttribute("data-theme", t);
    } catch (e) {}
  `;
  return <script dangerouslySetInnerHTML={{ __html: code }} />;
}
