import React, { useEffect, useRef, useState } from "react";

type SidebarInfoProps = {
  text: string;
  label?: string;
};

export function SidebarInfo({ text, label = "More information" }: SidebarInfoProps) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLSpanElement | null>(null);
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const [bubbleStyle, setBubbleStyle] = useState<React.CSSProperties>({});

  useEffect(() => {
    if (!open) return;
    const updateBubblePosition = () => {
      if (!triggerRef.current) return;
      const rect = triggerRef.current.getBoundingClientRect();
      const margin = 12;
      const maxWidthPx = Math.max(120, Math.min(220, window.innerWidth - margin * 2));
      const leftPx = Math.max(margin, Math.min(rect.left, window.innerWidth - maxWidthPx - margin));
      const topPx = Math.max(margin, Math.min(rect.bottom + 6, window.innerHeight - margin));
      setBubbleStyle({
        left: `${leftPx}px`,
        top: `${topPx}px`,
        maxWidth: `${Math.max(120, window.innerWidth - margin * 2)}px`,
      });
    };

    updateBubblePosition();

    const onDocMouseDown = (ev: MouseEvent) => {
      if (!rootRef.current?.contains(ev.target as Node)) {
        setOpen(false);
      }
    };
    const onDocKeyDown = (ev: KeyboardEvent) => {
      if (ev.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDocMouseDown);
    document.addEventListener("keydown", onDocKeyDown);
    window.addEventListener("resize", updateBubblePosition);
    window.addEventListener("scroll", updateBubblePosition, true);
    return () => {
      document.removeEventListener("mousedown", onDocMouseDown);
      document.removeEventListener("keydown", onDocKeyDown);
      window.removeEventListener("resize", updateBubblePosition);
      window.removeEventListener("scroll", updateBubblePosition, true);
    };
  }, [open]);

  return (
    <span className="sidebar-info" ref={rootRef}>
      <button
        ref={triggerRef}
        type="button"
        className="sidebar-info-trigger"
        aria-label={label}
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        i
      </button>
      {open && (
        <span role="note" className="sidebar-info-bubble" style={bubbleStyle}>
          {text}
        </span>
      )}
    </span>
  );
}
