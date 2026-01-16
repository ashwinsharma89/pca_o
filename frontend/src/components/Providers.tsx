"use client";

import { AuthProvider } from "@/context/AuthContext";
import { ThemeProvider } from "@/context/ThemeContext";

import { SessionExpiredToast } from "@/components/SessionExpiredToast";
import { Toaster } from "@/components/ui/toaster";

import { AnalysisProvider } from "@/context/AnalysisContext";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

export function Providers({ children }: { children: React.ReactNode }) {
    const [queryClient] = useState(() => new QueryClient({
        defaultOptions: {
            queries: {
                staleTime: 60 * 1000, // 1 minute
                refetchOnWindowFocus: false,
            },
        },
    }));

    return (
        <QueryClientProvider client={queryClient}>
            <ThemeProvider>
                <AuthProvider>
                    <AnalysisProvider>
                        <SessionExpiredToast />
                        {children}
                        <Toaster />
                    </AnalysisProvider>
                </AuthProvider>
            </ThemeProvider>
        </QueryClientProvider>
    );
}
