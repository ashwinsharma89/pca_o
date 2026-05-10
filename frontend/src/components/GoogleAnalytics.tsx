"use client";

import { Suspense, useEffect } from "react";
import { usePathname, useSearchParams } from "next/navigation";

declare global {
  interface Window {
    gtag?: (...args: unknown[]) => void;
    dataLayer?: unknown[];
  }
}

interface RouteChangeTrackerProps {
  measurementId: string;
}

function Tracker({ measurementId }: RouteChangeTrackerProps) {
  const pathname = usePathname();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.gtag !== "function") {
      return;
    }

    const queryString = searchParams?.toString();
    const fullPath = queryString ? `${pathname}?${queryString}` : pathname;

    window.gtag("event", "page_view", {
      page_path: fullPath,
      page_location: window.location.href,
      page_title: document.title,
      send_to: measurementId,
    });
  }, [pathname, searchParams, measurementId]);

  return null;
}

/**
 * Fires a GA4 `page_view` event on every Next.js client-side route change.
 * Wrap in <Suspense> because useSearchParams suspends during prerender.
 */
export function RouteChangeTracker({ measurementId }: RouteChangeTrackerProps) {
  return (
    <Suspense fallback={null}>
      <Tracker measurementId={measurementId} />
    </Suspense>
  );
}
