"use client";

import { useState, useRef, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
    Loader2, Send, Database, Zap, Search, TrendingUp,
    Target, BarChart3, Clock, CheckCircle, AlertCircle,
    Sparkles, BookOpen, ChevronDown, Network
} from "lucide-react";
import { api } from "@/lib/api";
import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
} from "@/components/ui/accordion";

interface QueryResult {
    success: boolean;
    data: any[];
    summary?: {
        count: number;
        total_spend?: number;
        total_impressions?: number;
        total_clicks?: number;
        total_conversions?: number;
        avg_ctr?: number;
        avg_cpc?: number;
        avg_roas?: number;
    };
    metadata: {
        query: string;
        intent: string;
        confidence: number;
        routing: string;
        cypher?: string;
        execution_time_ms?: number;
    };
    error?: string;
}

interface GraphHealth {
    status: string;
    neo4j_connected: boolean;
    neo4j_uri: string;
    node_count: number;
    relationship_count: number;
}

interface Template {
    intent: string;
    examples: string[];
}

export default function RAGPage() {
    const [query, setQuery] = useState("");
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState<QueryResult | null>(null);
    const [health, setHealth] = useState<GraphHealth | null>(null);
    const [templates, setTemplates] = useState<Template[]>([]);
    const [queryHistory, setQueryHistory] = useState<string[]>([]);
    const inputRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        loadHealth();
        loadTemplates();
    }, []);

    const loadHealth = async () => {
        try {
            const data = await api.get('/kg/health') as GraphHealth;
            setHealth(data);
        } catch (error) {
            console.error("Health check failed", error);
        }
    };

    const loadTemplates = async () => {
        try {
            const data = await api.get('/kg/templates') as { templates: Template[] };
            setTemplates(data.templates || []);
        } catch (error) {
            console.error("Templates load failed", error);
        }
    };

    const runQuery = async (queryText?: string) => {
        const q = queryText || query;
        if (!q.trim()) return;

        setLoading(true);
        setResult(null);

        try {
            const data = await api.post('/kg/query', {
                query: q,
                limit: 20
            }) as QueryResult;

            setResult(data);

            // Add to history
            if (!queryHistory.includes(q)) {
                setQueryHistory(prev => [q, ...prev.slice(0, 9)]);
            }
        } catch (error) {
            console.error("Query failed", error);
            setResult({
                success: false,
                data: [],
                metadata: {
                    query: q,
                    intent: "error",
                    confidence: 0,
                    routing: "error"
                },
                error: String(error)
            });
        } finally {
            setLoading(false);
        }
    };

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        runQuery();
    };

    const formatNumber = (num: number | null | undefined): string => {
        if (num == null || typeof num !== 'number') return "0";
        if (num >= 1000000) return `${(num / 1000000).toFixed(2)}M`;
        if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
        return num.toLocaleString();
    };

    const formatCurrency = (num: number): string => {
        if (typeof num !== 'number') return '$0.00';
        if (num >= 1000000) return `$${(num / 1000000).toFixed(2)}M`;
        if (num >= 1000) return `$${(num / 1000).toFixed(1)}K`;
        return `$${num.toFixed(2)}`;
    };

    const getIntentIcon = (intent: string) => {
        switch (intent) {
            case 'platform': return <Database className="h-4 w-4" />;
            case 'temporal': return <Clock className="h-4 w-4" />;
            case 'ranking': return <BarChart3 className="h-4 w-4" />;
            case 'cross_channel': return <Network className="h-4 w-4" />;
            case 'targeting': return <Target className="h-4 w-4" />;
            default: return <Search className="h-4 w-4" />;
        }
    };

    const exampleQueries = [
        "Meta ads performance",
        "Daily spend trend",
        "Top 5 campaigns by spend",
        "Compare Search vs Social",
        "Device breakdown",
    ];

    return (
        <div className="container mx-auto py-10 px-6 space-y-8 max-w-7xl animate-in fade-in duration-700">
            {/* Header */}
            <div className="flex items-center justify-between border-b pb-6">
                <div>
                    <h1 className="text-4xl font-extrabold tracking-tight bg-gradient-to-r from-cyan-500 to-blue-600 bg-clip-text text-transparent flex items-center gap-3">
                        <Sparkles className="h-10 w-10 text-cyan-500" />
                        Knowledge Graph RAG
                    </h1>
                    <p className="text-muted-foreground mt-2 text-lg">
                        Query campaign data with natural language powered by Neo4j.
                    </p>
                </div>
                {health && (
                    <Badge
                        variant="outline"
                        className={`px-4 py-2 text-sm ${health.neo4j_connected ? 'border-green-500/30 text-green-500 bg-green-500/5' : 'border-red-500/30 text-red-500 bg-red-500/5'}`}
                    >
                        <div className={`w-2 h-2 rounded-full mr-2 ${health.neo4j_connected ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
                        {health.neo4j_connected ? `${formatNumber(health.node_count)} nodes` : 'Disconnected'}
                    </Badge>
                )}
            </div>

            {/* Graph Stats */}
            {health && health.neo4j_connected && (
                <div className="grid gap-4 sm:grid-cols-3">
                    <Card className="bg-gradient-to-br from-cyan-500/5 to-blue-500/5 border-cyan-500/20">
                        <CardContent className="p-6 flex items-center gap-4">
                            <div className="bg-cyan-500/10 p-3 rounded-xl">
                                <Database className="h-6 w-6 text-cyan-500" />
                            </div>
                            <div>
                                <p className="text-xs text-muted-foreground uppercase tracking-wide font-medium">Nodes</p>
                                <p className="text-2xl font-bold">{formatNumber(health.node_count)}</p>
                            </div>
                        </CardContent>
                    </Card>
                    <Card className="bg-gradient-to-br from-purple-500/5 to-violet-500/5 border-purple-500/20">
                        <CardContent className="p-6 flex items-center gap-4">
                            <div className="bg-purple-500/10 p-3 rounded-xl">
                                <Network className="h-6 w-6 text-purple-500" />
                            </div>
                            <div>
                                <p className="text-xs text-muted-foreground uppercase tracking-wide font-medium">Relationships</p>
                                <p className="text-2xl font-bold">{formatNumber(health.relationship_count)}</p>
                            </div>
                        </CardContent>
                    </Card>
                    <Card className="bg-gradient-to-br from-green-500/5 to-emerald-500/5 border-green-500/20">
                        <CardContent className="p-6 flex items-center gap-4">
                            <div className="bg-green-500/10 p-3 rounded-xl">
                                <BookOpen className="h-6 w-6 text-green-500" />
                            </div>
                            <div>
                                <p className="text-xs text-muted-foreground uppercase tracking-wide font-medium">Templates</p>
                                <p className="text-2xl font-bold">{templates.length}</p>
                            </div>
                        </CardContent>
                    </Card>
                </div>
            )}

            {/* Query Input */}
            <Card className="border-cyan-500/20 bg-cyan-500/5 shadow-xl shadow-cyan-500/5">
                <CardContent className="p-6">
                    <form onSubmit={handleSubmit} className="flex gap-4">
                        <div className="relative flex-1">
                            <Search className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                            <Input
                                ref={inputRef}
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                                placeholder="Ask about your campaigns... e.g. 'Meta ads performance' or 'Daily spend trend'"
                                className="pl-12 h-14 text-lg border-cyan-500/20 focus:border-cyan-500 focus:ring-cyan-500 bg-background"
                                disabled={loading}
                            />
                        </div>
                        <Button
                            type="submit"
                            disabled={loading || !query.trim()}
                            className="h-14 px-8 bg-cyan-600 hover:bg-cyan-700 text-white font-bold rounded-xl shadow-lg shadow-cyan-600/20"
                        >
                            {loading ? (
                                <>
                                    <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                                    Querying...
                                </>
                            ) : (
                                <>
                                    <Send className="mr-2 h-5 w-5" />
                                    Query
                                </>
                            )}
                        </Button>
                    </form>

                    {/* Example Queries */}
                    <div className="mt-4 flex flex-wrap gap-2">
                        <span className="text-sm text-muted-foreground">Try:</span>
                        {exampleQueries.map((q, i) => (
                            <button
                                key={i}
                                onClick={() => {
                                    setQuery(q);
                                    runQuery(q);
                                }}
                                className="text-sm px-3 py-1 rounded-full bg-muted/50 hover:bg-cyan-500/10 hover:text-cyan-500 transition-colors"
                            >
                                {q}
                            </button>
                        ))}
                    </div>
                </CardContent>
            </Card>

            {/* Results */}
            {result && (
                <div className="space-y-6 animate-in slide-in-from-bottom-4 duration-500">
                    {/* Metadata */}
                    <Card className={`border-2 ${result.success ? 'border-green-500/20 bg-green-500/5' : 'border-red-500/20 bg-red-500/5'}`}>
                        <CardContent className="p-6">
                            <div className="flex flex-wrap items-center gap-4">
                                <div className="flex items-center gap-2">
                                    {result.success ? (
                                        <CheckCircle className="h-5 w-5 text-green-500" />
                                    ) : (
                                        <AlertCircle className="h-5 w-5 text-red-500" />
                                    )}
                                    <span className="font-semibold">
                                        {result.success ? 'Query Successful' : 'Query Failed'}
                                    </span>
                                </div>

                                <Badge variant="outline" className="border-cyan-500/30 text-cyan-500 bg-cyan-500/5">
                                    {getIntentIcon(result.metadata.intent)}
                                    <span className="ml-2">{result.metadata.intent}</span>
                                </Badge>

                                <Badge variant="outline" className={`${result.metadata.confidence > 0.8 ? 'border-green-500/30 text-green-500' : 'border-yellow-500/30 text-yellow-500'}`}>
                                    {(result.metadata.confidence * 100).toFixed(0)}% confidence
                                </Badge>

                                <Badge variant="outline">
                                    {result.metadata.routing === 'template' ? (
                                        <>
                                            <Zap className="h-3 w-3 mr-1" />
                                            Template
                                        </>
                                    ) : (
                                        <>
                                            <Sparkles className="h-3 w-3 mr-1" />
                                            LLM
                                        </>
                                    )}
                                </Badge>

                                {result.metadata.execution_time_ms && (
                                    <Badge variant="secondary">
                                        <Clock className="h-3 w-3 mr-1" />
                                        {result.metadata.execution_time_ms.toFixed(0)}ms
                                    </Badge>
                                )}

                                <span className="text-sm text-muted-foreground">
                                    {result.data.length} results
                                </span>
                            </div>

                            {result.error && (
                                <div className="mt-4 p-4 bg-red-500/10 rounded-lg text-red-500 text-sm">
                                    {result.error}
                                </div>
                            )}
                        </CardContent>
                    </Card>

                    {/* Summary Stats */}
                    {result.summary && result.summary.count > 0 && (
                        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
                            {result.summary.total_spend != null && (
                                <Card className="hover:border-cyan-500/30 transition-colors">
                                    <CardHeader className="pb-2">
                                        <CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Total Spend</CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <p className="text-2xl font-bold">{formatCurrency(result.summary.total_spend)}</p>
                                    </CardContent>
                                </Card>
                            )}
                            {result.summary.avg_roas != null && (
                                <Card className="hover:border-cyan-500/30 transition-colors">
                                    <CardHeader className="pb-2">
                                        <CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Avg ROAS</CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <p className="text-2xl font-bold">{typeof result.summary.avg_roas === 'number' ? result.summary.avg_roas.toFixed(2) : '0.00'}x</p>
                                    </CardContent>
                                </Card>
                            )}
                            {result.summary.avg_ctr != null && (
                                <Card className="hover:border-cyan-500/30 transition-colors">
                                    <CardHeader className="pb-2">
                                        <CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Avg CTR</CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <p className="text-2xl font-bold">{typeof result.summary.avg_ctr === 'number' ? result.summary.avg_ctr.toFixed(2) : '0.00'}%</p>
                                    </CardContent>
                                </Card>
                            )}
                            {result.summary.avg_cpc != null && (
                                <Card className="hover:border-cyan-500/30 transition-colors">
                                    <CardHeader className="pb-2">
                                        <CardTitle className="text-xs uppercase tracking-wide text-muted-foreground">Avg CPC</CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <p className="text-2xl font-bold">{formatCurrency(result.summary.avg_cpc)}</p>
                                    </CardContent>
                                </Card>
                            )}
                        </div>
                    )}

                    {/* No Results State */}
                    {result.success && result.data.length === 0 && (
                        <div className="flex flex-col items-center justify-center py-12 text-center border-2 border-dashed rounded-xl border-border/50 bg-muted/10">
                            <div className="bg-muted/20 rounded-full p-4 mb-4">
                                <Search className="h-8 w-8 text-muted-foreground" />
                            </div>
                            <h3 className="text-lg font-semibold mb-1">No Results Found</h3>
                            <p className="text-muted-foreground text-sm max-w-sm">
                                Your query was understood, but matched no data in the Knowledge Graph.
                            </p>
                        </div>
                    )}

                    {/* Data Table */}
                    {result.success && result.data.length > 0 && (
                        <Card>
                            <CardHeader>
                                <div className="flex items-center gap-3">
                                    <div className="bg-cyan-500/10 rounded-lg p-2">
                                        <BarChart3 className="h-5 w-5 text-cyan-500" />
                                    </div>
                                    <div>
                                        <CardTitle>Results</CardTitle>
                                        <CardDescription>{result.data.length} rows returned</CardDescription>
                                    </div>
                                </div>
                            </CardHeader>
                            <CardContent>
                                <div className="overflow-x-auto">
                                    <table className="w-full text-sm">
                                        <thead>
                                            <tr className="border-b border-border/60">
                                                {Object.keys(result.data[0]).map((key) => (
                                                    <th key={key} className="text-left py-3 px-4 font-semibold text-muted-foreground uppercase tracking-wide text-xs">
                                                        {key.replace(/_/g, ' ')}
                                                    </th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {result.data.slice(0, 20).map((row, i) => (
                                                <tr key={i} className="border-b border-border/40 hover:bg-muted/30 transition-colors">
                                                    {Object.values(row).map((val: any, j) => (
                                                        <td key={j} className="py-3 px-4">
                                                            {typeof val === 'number'
                                                                ? val.toLocaleString(undefined, { maximumFractionDigits: 2 })
                                                                : String(val ?? '-')}
                                                        </td>
                                                    ))}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </CardContent>
                        </Card>
                    )}

                    {/* Cypher Query */}
                    {result.metadata.cypher && (
                        <Accordion type="single" collapsible>
                            <AccordionItem value="cypher" className="border border-border/60 rounded-xl overflow-hidden">
                                <AccordionTrigger className="px-6 py-4 hover:no-underline hover:bg-muted/30">
                                    <div className="flex items-center gap-2 text-sm">
                                        <Database className="h-4 w-4 text-muted-foreground" />
                                        View Cypher Query
                                    </div>
                                </AccordionTrigger>
                                <AccordionContent className="p-6 bg-muted/20">
                                    <pre className="text-sm font-mono whitespace-pre-wrap overflow-x-auto text-cyan-500">
                                        {result.metadata.cypher}
                                    </pre>
                                </AccordionContent>
                            </AccordionItem>
                        </Accordion>
                    )}
                </div>
            )}

            {/* Empty State */}
            {!result && !loading && (
                <div className="flex flex-col items-center justify-center py-20 text-center border-2 border-dashed rounded-3xl border-border/50 bg-muted/20">
                    <div className="bg-cyan-500/10 rounded-full p-6 mb-6">
                        <Sparkles className="h-12 w-12 text-cyan-500" />
                    </div>
                    <h3 className="text-xl font-bold text-foreground mb-2">Query Your Knowledge Graph</h3>
                    <p className="text-muted-foreground max-w-md mx-auto mb-6">
                        Ask questions about your campaigns in natural language. The system will analyze your query and retrieve data from Neo4j.
                    </p>

                    {/* Template Categories */}
                    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 max-w-3xl">
                        {templates.map((t, i) => (
                            <Card key={i} className="text-left hover:border-cyan-500/30 transition-colors cursor-pointer" onClick={() => {
                                setQuery(t.examples[0]);
                                inputRef.current?.focus();
                            }}>
                                <CardContent className="p-4">
                                    <div className="flex items-center gap-2 mb-2">
                                        {getIntentIcon(t.intent)}
                                        <span className="font-semibold capitalize">{t.intent.replace(/_/g, ' ')}</span>
                                    </div>
                                    <p className="text-xs text-muted-foreground">
                                        {t.examples.join(', ')}
                                    </p>
                                </CardContent>
                            </Card>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
