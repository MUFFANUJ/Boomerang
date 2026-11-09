// Boomerang logo mark: a bent two-armed boomerang with a return arc.
export function LogoMark({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true">
      <rect width="32" height="32" rx="8" fill="var(--accent-soft)" />
      <path
        d="M7 24.5C8.6 16 11.6 9.6 15.6 8.6c3.4-.8 6.8 1.4 9.6 5.2"
        fill="none"
        stroke="var(--accent)"
        strokeWidth="4.2"
        strokeLinecap="round"
      />
      <path
        d="M25.6 19.5c-1.6 3.2-4.6 5.2-8.2 5.6"
        fill="none"
        stroke="var(--accent)"
        strokeOpacity="0.45"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeDasharray="0.1 3"
      />
    </svg>
  );
}
