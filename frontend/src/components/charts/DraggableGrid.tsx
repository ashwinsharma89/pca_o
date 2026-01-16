"use client";

import { ReactNode, useState } from 'react';
import { motion } from 'framer-motion';
import { GripVertical, Maximize2, Minimize2 } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';

export interface DashboardWidget {
    id: string;
    title: string;
    component: ReactNode;
    gridSpan?: 1 | 2; // 1 = half width, 2 = full width
}

interface DraggableGridProps {
    widgets: DashboardWidget[];
    editable?: boolean;
}

export function DraggableGrid({ widgets, editable = false }: DraggableGridProps) {
    const [expandedWidget, setExpandedWidget] = useState<string | null>(null);

    const toggleExpand = (widgetId: string) => {
        setExpandedWidget(expandedWidget === widgetId ? null : widgetId);
    };

    // Expanded view
    if (expandedWidget) {
        const widget = widgets.find(w => w.id === expandedWidget);
        if (widget) {
            return (
                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    className="fixed inset-4 z-50 bg-background"
                >
                    <Card className="h-full">
                        <CardHeader className="flex flex-row items-center justify-between py-3">
                            <CardTitle>{widget.title}</CardTitle>
                            <Button variant="ghost" size="icon" onClick={() => setExpandedWidget(null)}>
                                <Minimize2 className="h-4 w-4" />
                            </Button>
                        </CardHeader>
                        <CardContent className="h-[calc(100%-60px)]">
                            {widget.component}
                        </CardContent>
                    </Card>
                </motion.div>
            );
        }
    }

    return (
        <div className="grid gap-4 md:grid-cols-2">
            {widgets.map((widget, idx) => (
                <motion.div
                    key={widget.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: idx * 0.05 }}
                    className={widget.gridSpan === 2 ? 'md:col-span-2' : ''}
                >
                    <Card className="h-full min-h-[300px]">
                        <CardHeader className="flex flex-row items-center justify-between py-2 px-4">
                            <div className="flex items-center gap-2">
                                {editable && (
                                    <div className="drag-handle cursor-move">
                                        <GripVertical className="h-4 w-4 text-muted-foreground" />
                                    </div>
                                )}
                                <CardTitle className="text-sm font-medium">{widget.title}</CardTitle>
                            </div>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="h-6 w-6"
                                onClick={() => toggleExpand(widget.id)}
                            >
                                <Maximize2 className="h-3 w-3" />
                            </Button>
                        </CardHeader>
                        <CardContent className="p-4 pt-0 h-[calc(100%-52px)]">
                            {widget.component}
                        </CardContent>
                    </Card>
                </motion.div>
            ))}
        </div>
    );
}
