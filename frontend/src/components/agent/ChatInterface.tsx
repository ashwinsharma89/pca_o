import React, { useState, useRef, useEffect } from 'react';
import { Send, Bot, User as UserIcon, Loader2 } from 'lucide-react';
import { api } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { cn } from '@/lib/utils';

interface ChatInterfaceProps {
    campaignId: string;
}

interface Message {
    id: string;
    role: 'user' | 'assistant';
    content: string;
    timestamp: Date;
    data?: any[];
    chart?: {
        type: string;
        title: string;
        labels: string[];
        values: number[];
        label_key: string;
        value_key: string;
    };
}

import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { FunnelChart } from "@/components/charts/FunnelChart";

export function ChatInterface({ campaignId }: ChatInterfaceProps) {
    const [messages, setMessages] = useState<Message[]>([
        {
            id: 'welcome',
            role: 'assistant',
            content: 'Hello! I am your Campaign Analyst AI. Ask me anything about this campaign\'s performance, ROAS, or best converting channels.',
            timestamp: new Date(),
        }
    ]);
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const messagesEndRef = useRef<HTMLDivElement>(null);

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    const handleSend = async () => {
        if (!input.trim() || isLoading) return;

        const userMessage: Message = {
            id: Date.now().toString(),
            role: 'user',
            content: input,
            timestamp: new Date(),
        };

        setMessages((prev: Message[]) => [...prev, userMessage]);
        setInput('');
        setIsLoading(true);

        try {
            const response: any = await api.chatWithCampaign(campaignId, userMessage.content);

            const botMessage: Message = {
                id: (Date.now() + 1).toString(),
                role: 'assistant',
                content: response.answer || "I couldn't generate an answer. Please check the logs.",
                timestamp: new Date(),
                data: response.data,
                chart: response.chart
            };

            setMessages((prev: Message[]) => [...prev, botMessage]);
        } catch (error) {
            console.error('Chat error:', error);
            const errorMessage: Message = {
                id: (Date.now() + 1).toString(),
                role: 'assistant',
                content: 'Sorry, I encountered an error comparing your request. Please try again.',
                timestamp: new Date(),
            };
            setMessages((prev: Message[]) => [...prev, errorMessage]);
        } finally {
            setIsLoading(false);
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    const renderData = (data: any[]) => {
        if (!data || data.length === 0) return null;
        const columns = Object.keys(data[0]);

        return (
            <div className="mt-4 border rounded-lg overflow-hidden bg-white dark:bg-slate-900 shadow-sm overflow-x-auto">
                <Table>
                    <TableHeader className="bg-slate-50 dark:bg-slate-800">
                        <TableRow>
                            {columns.map(col => (
                                <TableHead key={col} className="text-[10px] uppercase font-bold text-slate-500 whitespace-nowrap px-3 h-8">
                                    {col.replace(/_/g, ' ')}
                                </TableHead>
                            ))}
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {data.slice(0, 10).map((row, i) => (
                            <TableRow key={i} className="hover:bg-slate-50 dark:hover:bg-slate-800/50">
                                {columns.map(col => (
                                    <TableCell key={col} className="text-[11px] py-1 px-3 border-b border-slate-100 dark:border-slate-800">
                                        {typeof row[col] === 'number' ?
                                            (row[col] > 1000 ? row[col].toLocaleString() : row[col].toFixed(2)) :
                                            row[col]}
                                    </TableCell>
                                ))}
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
                {data.length > 10 && (
                    <div className="p-1 text-[9px] text-center text-slate-400 bg-slate-50 dark:bg-slate-900 border-t italic">
                        Showing first 10 of {data.length} rows
                    </div>
                )}
            </div>
        );
    };

    const renderChart = (chart: Message['chart']) => {
        if (!chart) return null;

        if (chart.type === 'funnel') {
            const funnelData = chart.labels.map((label, i) => ({
                stage: label,
                value: chart.values[i]
            }));

            return (
                <div className="mt-4 p-4 border rounded-xl bg-white dark:bg-slate-900 shadow-sm">
                    <h4 className="text-xs font-bold mb-4 text-slate-600 dark:text-slate-400 uppercase tracking-wider">{chart.title}</h4>
                    <div className="h-[300px]">
                        <FunnelChart data={funnelData} />
                    </div>
                </div>
            );
        }

        return (
            <div className="mt-4 p-3 border rounded-lg bg-slate-50 dark:bg-slate-900 text-[10px] text-slate-500 italic text-center">
                Visualizing "{chart.title}" as {chart.type} chart...
            </div>
        );
    };

    return (
        <Card className="flex flex-col h-[650px] shadow-2xl border-slate-200 dark:border-slate-800 overflow-hidden">
            <CardHeader className="border-b bg-white dark:bg-slate-900 py-3">
                <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full bg-indigo-50 dark:bg-indigo-900/30 flex items-center justify-center">
                        <Bot className="w-6 h-6 text-indigo-600 dark:text-indigo-400" />
                    </div>
                    <div>
                        <CardTitle className="text-lg font-bold bg-clip-text text-transparent bg-gradient-to-r from-indigo-600 to-purple-600">AI Campaign Analyst</CardTitle>
                        <CardDescription className="text-xs">Next-gen analytical insights powered by Gemini</CardDescription>
                    </div>
                </div>
            </CardHeader>

            <CardContent className="flex-1 overflow-y-auto p-4 space-y-6 scrollbar-thin scrollbar-thumb-slate-200 dark:scrollbar-thumb-slate-800">
                {messages.map((message: Message) => (
                    <div
                        key={message.id}
                        className={cn(
                            "flex gap-4",
                            message.role === 'user' ? "flex-row-reverse" : "flex-row"
                        )}
                    >
                        <div className={cn(
                            "w-9 h-9 rounded-full flex items-center justify-center shrink-0 shadow-sm",
                            message.role === 'user'
                                ? "bg-gradient-to-br from-indigo-500 to-purple-600 text-white"
                                : "bg-white dark:bg-slate-800 text-slate-700 dark:text-slate-300 border border-slate-200 dark:border-slate-700"
                        )}>
                            {message.role === 'user' ? <UserIcon size={18} /> : <Bot size={18} />}
                        </div>

                        <div className={cn(
                            "group relative max-w-[85%]",
                            message.role === 'user' ? "text-right" : "text-left"
                        )}>
                            <div className={cn(
                                "p-4 rounded-2xl text-[13px] leading-relaxed shadow-sm transition-all duration-200",
                                message.role === 'user'
                                    ? "bg-indigo-600 text-white rounded-tr-none hover:bg-indigo-700"
                                    : "bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-200 border border-slate-100 dark:border-slate-700 rounded-tl-none shadow-indigo-100/20"
                            )}>
                                {message.content.split('\n').map((line: string, i: number) => (
                                    <p key={i} className={line.trim() ? "mb-2 last:mb-0" : "h-2"}>{line}</p>
                                ))}

                                {message.chart && renderChart(message.chart)}
                                {message.data && renderData(message.data)}

                                <div className={cn(
                                    "text-[10px] opacity-50 mt-2 font-medium",
                                    message.role === 'user' ? "text-indigo-100" : "text-slate-500"
                                )}>
                                    {message.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                </div>
                            </div>
                        </div>
                    </div>
                ))}
                {isLoading && (
                    <div className="flex gap-4">
                        <div className="w-9 h-9 rounded-full bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 flex items-center justify-center shrink-0">
                            <Bot size={18} className="text-indigo-500 animate-pulse" />
                        </div>
                        <div className="bg-white dark:bg-slate-800 border border-slate-100 dark:border-slate-700 p-4 rounded-2xl rounded-tl-none flex items-center shadow-sm">
                            <Loader2 className="w-4 h-4 animate-spin text-indigo-500" />
                            <span className="text-xs text-slate-500 ml-3 font-medium">Analyzing campaign data...</span>
                        </div>
                    </div>
                )}
                <div ref={messagesEndRef} />
            </CardContent>

            <div className="p-4 border-t bg-slate-50/30 dark:bg-slate-900/30">
                <div className="flex gap-2">
                    <Input
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyDown={handleKeyDown}
                        placeholder="E.g., What is our CPA for Google Ads?"
                        className="rounded-full border-slate-300 focus:ring-indigo-500"
                        disabled={isLoading}
                    />
                    <Button
                        onClick={handleSend}
                        disabled={isLoading || !input.trim()}
                        size="icon"
                        className="rounded-full w-10 h-10 shrink-0 bg-indigo-600 hover:bg-indigo-700"
                    >
                        <Send size={18} />
                    </Button>
                </div>
            </div>
        </Card>
    );
}
