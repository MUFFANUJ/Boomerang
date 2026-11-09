// Small inline icon set (16px, stroke = currentColor).
import type { SVGProps } from "react";

type P = SVGProps<SVGSVGElement> & { size?: number };

function Svg({ size = 16, children, ...rest }: P) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...rest}
    >
      {children}
    </svg>
  );
}

export const IconOverview = (p: P) => (
  <Svg {...p}>
    <rect x="2" y="2" width="5" height="6" rx="1.2" />
    <rect x="9" y="2" width="5" height="4" rx="1.2" />
    <rect x="2" y="10" width="5" height="4" rx="1.2" />
    <rect x="9" y="8" width="5" height="6" rx="1.2" />
  </Svg>
);
export const IconFlow = (p: P) => (
  <Svg {...p}>
    <circle cx="3" cy="8" r="1.6" />
    <circle cx="13" cy="4" r="1.6" />
    <circle cx="13" cy="12" r="1.6" />
    <path d="M4.6 8h3.2M7.8 8c1.6 0 2-4 3.6-4M7.8 8c1.6 0 2 4 3.6 4" />
  </Svg>
);
export const IconTraffic = (p: P) => (
  <Svg {...p}>
    <path d="M2 12l3.5-4 3 2.5L14 4" />
    <path d="M10.5 4H14v3.5" />
  </Svg>
);
export const IconChaos = (p: P) => (
  <Svg {...p}>
    <path d="M9 1.5L3.5 9h4L7 14.5 12.5 7h-4L9 1.5z" />
  </Svg>
);
export const IconEndpoints = (p: P) => (
  <Svg {...p}>
    <circle cx="8" cy="8" r="6" />
    <path d="M2 8h12M8 2c1.8 1.8 2.6 3.8 2.6 6S9.8 12.2 8 14M8 2C6.2 3.8 5.4 5.8 5.4 8s.8 4.2 2.6 6" />
  </Svg>
);
export const IconEvents = (p: P) => (
  <Svg {...p}>
    <circle cx="7" cy="7" r="4.5" />
    <path d="M10.4 10.4L14 14" />
  </Svg>
);
export const IconInfra = (p: P) => (
  <Svg {...p}>
    <rect x="2" y="2.5" width="12" height="4.5" rx="1.2" />
    <rect x="2" y="9" width="12" height="4.5" rx="1.2" />
    <path d="M4.5 4.75h.01M4.5 11.25h.01" strokeWidth={2} />
  </Svg>
);
export const IconSettings = (p: P) => (
  <Svg {...p}>
    <circle cx="8" cy="8" r="2" />
    <path d="M8 1.5v2M8 12.5v2M1.5 8h2M12.5 8h2M3.4 3.4l1.4 1.4M11.2 11.2l1.4 1.4M3.4 12.6l1.4-1.4M11.2 4.8l1.4-1.4" />
  </Svg>
);
export const IconCheck = (p: P) => (
  <Svg {...p}>
    <path d="M3 8.5l3 3 7-7" />
  </Svg>
);
export const IconX = (p: P) => (
  <Svg {...p}>
    <path d="M4 4l8 8M12 4l-8 8" />
  </Svg>
);
export const IconAlert = (p: P) => (
  <Svg {...p}>
    <path d="M8 1.8L14.6 13.5H1.4L8 1.8z" />
    <path d="M8 6.2v3.3M8 11.6h.01" />
  </Svg>
);
export const IconInfo = (p: P) => (
  <Svg {...p}>
    <circle cx="8" cy="8" r="6.2" />
    <path d="M8 7.2v4M8 4.9h.01" />
  </Svg>
);
export const IconCopy = (p: P) => (
  <Svg {...p}>
    <rect x="5" y="5" width="8.5" height="8.5" rx="1.5" />
    <path d="M11 5V3.5A1.5 1.5 0 009.5 2h-6A1.5 1.5 0 002 3.5v6A1.5 1.5 0 003.5 11H5" />
  </Svg>
);
export const IconEye = (p: P) => (
  <Svg {...p}>
    <path d="M1.5 8S4 3.5 8 3.5 14.5 8 14.5 8 12 12.5 8 12.5 1.5 8 1.5 8z" />
    <circle cx="8" cy="8" r="2" />
  </Svg>
);
export const IconEyeOff = (p: P) => (
  <Svg {...p}>
    <path d="M2 2l12 12M6.6 6.6a2 2 0 002.8 2.8M4.2 4.4C2.5 5.6 1.5 8 1.5 8S4 12.5 8 12.5c1.3 0 2.4-.4 3.4-1M7 3.6c.3 0 .7-.1 1-.1 4 0 6.5 4.5 6.5 4.5s-.5 1-1.5 2.1" />
  </Svg>
);
export const IconRefresh = (p: P) => (
  <Svg {...p}>
    <path d="M13.5 8a5.5 5.5 0 11-1.6-3.9" />
    <path d="M13.5 2.5v3h-3" />
  </Svg>
);
export const IconReplay = (p: P) => (
  <Svg {...p}>
    <path d="M2.5 8a5.5 5.5 0 105.5-5.5c-1.8 0-3.3.8-4.4 2.1" />
    <path d="M3 2v3h3" />
  </Svg>
);
export const IconPlus = (p: P) => (
  <Svg {...p}>
    <path d="M8 3v10M3 8h10" />
  </Svg>
);
export const IconMinus = (p: P) => (
  <Svg {...p}>
    <path d="M3 8h10" />
  </Svg>
);
export const IconTrash = (p: P) => (
  <Svg {...p}>
    <path d="M2.5 4h11M6 4V2.5h4V4M4 4l.7 9.5h6.6L12 4" />
  </Svg>
);
export const IconEdit = (p: P) => (
  <Svg {...p}>
    <path d="M10.5 2.5l3 3L6 13H3v-3l7.5-7.5z" />
  </Svg>
);
export const IconPause = (p: P) => (
  <Svg {...p}>
    <path d="M5.5 3v10M10.5 3v10" />
  </Svg>
);
export const IconPlay = (p: P) => (
  <Svg {...p}>
    <path d="M4.5 2.8l9 5.2-9 5.2V2.8z" />
  </Svg>
);
export const IconStop = (p: P) => (
  <Svg {...p}>
    <rect x="3.5" y="3.5" width="9" height="9" rx="1.5" />
  </Svg>
);
export const IconKey = (p: P) => (
  <Svg {...p}>
    <circle cx="5" cy="11" r="2.8" />
    <path d="M7 9l6.5-6.5M11 5l2 2M9.5 6.5l1.5 1.5" />
  </Svg>
);
export const IconRotate = (p: P) => (
  <Svg {...p}>
    <path d="M2.5 6.5A5.5 5.5 0 0113 5M13.5 9.5A5.5 5.5 0 013 11" />
    <path d="M13 1.8V5h-3.2M3 14.2V11h3.2" />
  </Svg>
);
export const IconSkull = (p: P) => (
  <Svg {...p}>
    <path d="M8 1.8c-3.2 0-5.5 2.2-5.5 5.2 0 1.8.9 3.1 2 3.9V13h7v-2.1c1.1-.8 2-2.1 2-3.9 0-3-2.3-5.2-5.5-5.2z" />
    <circle cx="5.9" cy="7.2" r="1" />
    <circle cx="10.1" cy="7.2" r="1" />
    <path d="M7 13v-1.5M9 13v-1.5" />
  </Svg>
);
export const IconBolt = IconChaos;
export const IconArrowLeft = (p: P) => (
  <Svg {...p}>
    <path d="M13 8H3M7 4L3 8l4 4" />
  </Svg>
);
export const IconExternal = (p: P) => (
  <Svg {...p}>
    <path d="M9 2.5h4.5V7M13.5 2.5L7 9M11.5 9.5v3a1 1 0 01-1 1h-7a1 1 0 01-1-1v-7a1 1 0 011-1h3" />
  </Svg>
);
export const IconShield = (p: P) => (
  <Svg {...p}>
    <path d="M8 1.5l5.5 2v4.2c0 3.2-2.3 5.8-5.5 6.8-3.2-1-5.5-3.6-5.5-6.8V3.5l5.5-2z" />
    <path d="M5.5 8l1.8 1.8L10.8 6.3" />
  </Svg>
);
export const IconInbox = (p: P) => (
  <Svg {...p}>
    <path d="M1.8 9.5L3.8 3h8.4l2 6.5V13H1.8V9.5z" />
    <path d="M1.8 9.5h3.7l.8 1.6h3.4l.8-1.6h3.7" />
  </Svg>
);
export const IconChevronRight = (p: P) => (
  <Svg {...p}>
    <path d="M6 3.5L10.5 8 6 12.5" />
  </Svg>
);
export const IconSearch = IconEvents;
