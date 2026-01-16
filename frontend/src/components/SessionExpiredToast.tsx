"use client";

import { useEffect } from "react";
import { useAuth } from "@/context/AuthContext";
import { useRouter } from "next/navigation";
import { useToast } from "@/hooks/use-toast";

export function SessionExpiredToast() {
    const { sessionExpired, clearSessionExpired } = useAuth();
    const router = useRouter();
    const { toast } = useToast();

    useEffect(() => {
        if (sessionExpired) {
            toast({
                title: "Session Expired",
                description: "Your session has expired. Please log in again.",
                variant: "destructive",
            });
            clearSessionExpired();
            router.push('/login');
        }
    }, [sessionExpired, clearSessionExpired, router, toast]);

    return null;
}
