"use client";

import { useEffect } from "react";

/**
 * Next.js Global Error Page
 * 
 * This catches errors that occur in the root layout.
 * For errors in other components, use ErrorBoundary.
 */
export default function GlobalError({
    error,
    reset,
}: {
    error: Error & { digest?: string };
    reset: () => void;
}) {
    useEffect(() => {
        // Log the error to console
        console.error("Global error:", error);

        // Report to backend
        try {
            fetch("/api/v1/errors/report", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    error: {
                        message: error.message,
                        stack: error.stack,
                        name: error.name,
                        digest: error.digest,
                    },
                    type: "global_error",
                    timestamp: new Date().toISOString(),
                    url: typeof window !== "undefined" ? window.location.href : "",
                }),
            }).catch(() => { });
        } catch {
            // Silently fail
        }
    }, [error]);

    return (
        <html lang="en">
            <body className="bg-gray-100 dark:bg-gray-900 min-h-screen flex items-center justify-center p-4">
                <div className="max-w-lg w-full bg-white dark:bg-gray-800 rounded-xl shadow-lg p-8">
                    <div className="text-center">
                        {/* Error Icon */}
                        <div className="mx-auto w-16 h-16 bg-red-100 dark:bg-red-900/30 rounded-full flex items-center justify-center mb-6">
                            <svg
                                className="w-8 h-8 text-red-500"
                                fill="none"
                                stroke="currentColor"
                                viewBox="0 0 24 24"
                            >
                                <path
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                    strokeWidth={2}
                                    d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                                />
                            </svg>
                        </div>

                        <h1 className="text-2xl font-bold text-gray-900 dark:text-white mb-2">
                            Something went wrong!
                        </h1>

                        <p className="text-gray-600 dark:text-gray-400 mb-6">
                            An unexpected error occurred. Our team has been notified.
                        </p>

                        {/* Error Details (Development Only) */}
                        {process.env.NODE_ENV === "development" && (
                            <details className="mb-6 text-left">
                                <summary className="cursor-pointer text-sm text-red-600 dark:text-red-400 hover:underline">
                                    Error details (dev only)
                                </summary>
                                <pre className="mt-2 p-4 bg-gray-100 dark:bg-gray-900 rounded-lg text-xs overflow-auto max-h-48 text-red-600 dark:text-red-400">
                                    {error.message}
                                    {"\n\n"}
                                    {error.stack}
                                </pre>
                            </details>
                        )}

                        {/* Actions */}
                        <div className="flex flex-col sm:flex-row gap-3 justify-center">
                            <button
                                onClick={reset}
                                className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium"
                            >
                                Try Again
                            </button>
                            <button
                                onClick={() => (window.location.href = "/")}
                                className="px-6 py-3 bg-gray-200 dark:bg-gray-700 text-gray-800 dark:text-gray-200 rounded-lg hover:bg-gray-300 dark:hover:bg-gray-600 transition-colors font-medium"
                            >
                                Go Home
                            </button>
                        </div>
                    </div>
                </div>
            </body>
        </html>
    );
}
