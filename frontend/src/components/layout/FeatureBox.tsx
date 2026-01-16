'use client';

import React from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { LucideIcon, Info } from "lucide-react";
import { motion } from "framer-motion";

interface FeatureItemProps {
    label: string;
    value?: string | number;
    icon?: LucideIcon;
    status?: 'active' | 'inactive' | 'warning';
    description?: string;
}

export function FeatureItem({ label, value, icon: Icon, status, description }: FeatureItemProps) {
    return (
        <div className="flex flex-col gap-1 p-2 rounded-lg hover:bg-white/5 transition-colors group">
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    {Icon && <Icon className="h-4 w-4 text-primary/70 group-hover:text-primary transition-colors" />}
                    <span className="text-sm font-medium text-muted-foreground group-hover:text-foreground transition-colors">{label}</span>
                </div>
                {status && (
                    <Badge variant={status === 'active' ? 'default' : status === 'warning' ? 'secondary' : 'outline'} className="h-4 px-1 text-[10px]">
                        {status}
                    </Badge>
                )}
            </div>
            {value !== undefined && (
                <div className="text-lg font-bold pl-6">
                    {value}
                </div>
            )}
            {description && (
                <p className="text-[10px] text-muted-foreground pl-6 leading-tight">
                    {description}
                </p>
            )}
        </div>
    );
}

interface FeatureBoxProps {
    title: string;
    description?: string;
    icon?: LucideIcon;
    children: React.ReactNode;
    className?: string;
}

export function FeatureBox({ title, description, icon: Icon, children, className }: FeatureBoxProps) {
    return (
        <motion.div
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.5 }}
            className={className}
        >
            <Card className="border-white/10 bg-black/20 backdrop-blur-sm sticky top-24">
                <CardHeader className="pb-3 border-b border-white/5">
                    <CardTitle className="text-lg flex items-center gap-2">
                        {Icon && <Icon className="h-5 w-5 text-primary" />}
                        {title}
                    </CardTitle>
                    {description && <CardDescription className="text-xs">{description}</CardDescription>}
                </CardHeader>
                <CardContent className="pt-4 space-y-4">
                    {children}
                </CardContent>
            </Card>
        </motion.div>
    );
}
