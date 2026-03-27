import * as si from "simple-icons";
import type { EngineDef } from "./engine-catalog";

interface EngineIconProps {
  engine: EngineDef;
  /** Icon container size in px (default 32) */
  size?: number;
}

/**
 * Renders the official SVG logo for an engine (via simple-icons),
 * or a branded circle-with-initials fallback when no icon exists.
 */
export function EngineIcon({ engine, size = 32 }: EngineIconProps) {
  const iconSvgSize = Math.round(size * 0.55);

  if (engine.simpleIconSlug) {
    const icon = (si as Record<string, si.SimpleIcon>)[engine.simpleIconSlug];
    if (icon) {
      // Determine if the brand color is light (needs dark container) or dark (needs light container)
      const r = parseInt(engine.hex.slice(0, 2), 16);
      const g = parseInt(engine.hex.slice(2, 4), 16);
      const b = parseInt(engine.hex.slice(4, 6), 16);
      const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
      const isLight = luminance > 0.65;

      return (
        <div
          className="rounded-xl flex items-center justify-center flex-shrink-0"
          style={{
            width: size,
            height: size,
            backgroundColor: `#${engine.hex}${isLight ? "" : "18"}`,
          }}
        >
          <svg
            role="img"
            viewBox="0 0 24 24"
            width={iconSvgSize}
            height={iconSvgSize}
            fill={isLight ? "#1e293b" : `#${engine.hex}`}
          >
            <path d={icon.path} />
          </svg>
        </div>
      );
    }
  }

  // Fallback: colored circle with engine initials
  const initials = engine.name
    .replace(/^Apache\s+/i, "")
    .split(/\s+/)
    .slice(0, 2)
    .map((w) => w[0].toUpperCase())
    .join("");

  return (
    <div
      className="rounded-xl flex items-center justify-center flex-shrink-0 text-white font-bold"
      style={{
        width: size,
        height: size,
        backgroundColor: `#${engine.hex}`,
        fontSize: size * 0.3,
        letterSpacing: "-0.02em",
      }}
    >
      {initials}
    </div>
  );
}
