/**
 * Intelligence Studio - AI-Powered Natural Language Visualization
 * 
 * Features:
 * - Natural language query input
 * - AI-powered chart type selection
 * - Automatic visualization generation
 * - Conversational insights
 * - Follow-up question suggestions
 * 
 * Usage:
 * Place in: frontend/src/app/intelligence-studio/page.tsx
 * 
 * Dependencies:
 * - @tanstack/react-query (for data fetching)
 * - recharts (for charts)
 * - lucide-react (for icons)
 */

"use client";

import { useState, useRef, useEffect } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  Sparkles, Send, Loader2, TrendingUp, TrendingDown,
  BarChart3, LineChart as LineChartIcon, PieChart as PieChartIcon,
  Lightbulb, MessageSquare, Mic, Download, Share2, RefreshCw
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import dynamic from 'next/dynamic';

// Dynamic imports for charts
const ResponsiveContainer = dynamic(() => import('recharts').then(mod => mod.ResponsiveContainer), { ssr: false });
const BarChart = dynamic(() => import('recharts').then(mod => mod.BarChart), { ssr: false });
const Bar = dynamic(() => import('recharts').then(mod => mod.Bar), { ssr: false });
const LineChart = dynamic(() => import('recharts').then(mod => mod.LineChart), { ssr: false });
const Line = dynamic(() => import('recharts').then(mod => mod.Line), { ssr: false });
const PieChart = dynamic(() => import('recharts').then(mod => mod.PieChart), { ssr: false });
const Pie = dynamic(() => import('recharts').then(mod => mod.Pie), { ssr: false });
const Cell = dynamic(() => import('recharts').then(mod => mod.Cell), { ssr: false });
const XAxis = dynamic(() => import('recharts').then(mod => mod.XAxis), { ssr: false });
const YAxis = dynamic(() => import('recharts').then(mod => mod.YAxis), { ssr: false });
const CartesianGrid = dynamic(() => import('recharts').then(mod => mod.CartesianGrid), { ssr: false });
const Tooltip = dynamic(() => import('recharts').then(mod => mod.Tooltip), { ssr: false });
const Legend = dynamic(() => import('recharts').then(mod => mod.Legend), { ssr: false });

// ============================================================================
// TYPESCRIPT INTERFACES
// ============================================================================

interface QueryResponse {
  query: string;
  chart_type: 'bar' | 'line' | 'pie' | 'area';
  data: any[];
  insight: string;
  recommendations: string[];
  follow_up_questions: string[];
  sql_query?: string;
  execution_time_ms: number;
}

interface ConversationMessage {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  chart_data?: QueryResponse;
}

// ============================================================================
// MOCK DATA (for testing without API)
// ============================================================================

const MOCK_RESPONSE: QueryResponse = {
  query: "Show me Google Ads spend vs Meta for Q4",
  chart_type: "bar",
  data: [
    { platform: "Google Ads", spend: 125000, conversions: 1250 },
    { platform: "Meta Ads", spend: 98000, conversions: 1420 },
    { platform: "LinkedIn", spend: 45000, conversions: 340 },
    { platform: "TikTok", spend: 32000, conversions: 890 }
  ],
  insight: "Meta Ads achieved 15% better conversion efficiency (CPA: $69) compared to Google Ads (CPA: $100), despite 28% lower spend. TikTok shows strong engagement with lowest CPA at $36.",
  recommendations: [
    "Shift 15-20% of Google Ads budget to Meta for better ROAS",
    "Scale TikTok campaigns - strong CPA performance indicates opportunity",
    "Analyze Google Ads quality score to improve conversion efficiency"
  ],
  follow_up_questions: [
    "Show me spend breakdown by age group",
    "What's the trend over the last 6 months?",
    "Compare CTR across these platforms"
  ],
  sql_query: "SELECT platform, SUM(spend) as spend, SUM(conversions) as conversions FROM campaigns WHERE date >= '2024-10-01' GROUP BY platform",
  execution_time_ms: 247
};

const SUGGESTED_QUERIES = [
  "Show top 10 campaigns by ROAS",
  "Compare Google Ads vs Meta performance this month",
  "What caused the CPA spike last week?",
  "Show conversion funnel breakdown",
  "Which campaigns are underperforming?",
  "Analyze spend by device type"
];

const CHART_COLORS = ['#6366f1', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#06b6d4'];

// ============================================================================
// API INTEGRATION HOOKS
// ============================================================================


/**
 * Hook to send natural language query to AI backend
 * 
 * API Endpoint: POST /api/v1/intelligence/query
 * Request: { query: string }
 * Response: QueryResponse
 */
const useIntelligenceQuery = () => {
  return useMutation({
    mutationFn: async (query: string): Promise<QueryResponse> => {
      const token = localStorage.getItem('token');
      const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';

      const response = await fetch(`${API_URL}/intelligence/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
          'X-CSRF-Token': 'v2-token-generation-pca'
        },
        body: JSON.stringify({ query })
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Failed to process query');
      }

      return response.json();
    }
  });
};


// ============================================================================
// CHART RENDERING COMPONENT
// ============================================================================

interface ChartRendererProps {
  chartType: QueryResponse['chart_type'];
  data: any[];
  height?: number;
}

const ChartRenderer = ({ chartType, data, height = 300 }: ChartRendererProps) => {
  if (!data || data.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        No data to display
      </div>
    );
  }

  const dataKeys = Object.keys(data[0]).filter(key => typeof data[0][key] === 'number');
  const categoryKey = Object.keys(data[0]).find(key => typeof data[0][key] === 'string') || 'name';

  switch (chartType) {
    case 'bar':
      return (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
            <XAxis dataKey={categoryKey} />
            <YAxis />
            <Tooltip />
            <Legend />
            {dataKeys.map((key, idx) => (
              <Bar key={key} dataKey={key} fill={CHART_COLORS[idx % CHART_COLORS.length]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      );

    case 'line':
      return (
        <ResponsiveContainer width="100%" height={height}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
            <XAxis dataKey={categoryKey} />
            <YAxis />
            <Tooltip />
            <Legend />
            {dataKeys.map((key, idx) => (
              <Line
                key={key}
                type="monotone"
                dataKey={key}
                stroke={CHART_COLORS[idx % CHART_COLORS.length]}
                strokeWidth={2}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      );

    case 'pie':
      const pieData = data.map((item, idx) => ({
        name: item[categoryKey],
        value: item[dataKeys[0]],
        fill: CHART_COLORS[idx % CHART_COLORS.length]
      }));

      return (
        <ResponsiveContainer width="100%" height={height}>
          <PieChart>
            <Pie
              data={pieData}
              cx="50%"
              cy="50%"
              labelLine={false}
              label={({ name, percent }) => `${name}: ${((percent || 0) * 100).toFixed(0)}%`}
              outerRadius={100}
              dataKey="value"
            >
              {pieData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.fill} />
              ))}
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>
      );

    default:
      return (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
            <XAxis dataKey={categoryKey} />
            <YAxis />
            <Tooltip />
            <Legend />
            {dataKeys.map((key, idx) => (
              <Bar key={key} dataKey={key} fill={CHART_COLORS[idx % CHART_COLORS.length]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      );
  }
};

// ============================================================================
// MAIN COMPONENT
// ============================================================================

export default function IntelligenceStudioPage() {
  const [query, setQuery] = useState("");
  const [conversation, setConversation] = useState<ConversationMessage[]>([]);
  const [isListening, setIsListening] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const recognitionRef = useRef<any>(null);

  const intelligenceQuery = useIntelligenceQuery();

  // Initialize Web Speech API
  useEffect(() => {
    if (typeof window !== 'undefined' && ('webkitSpeechRecognition' in window || 'SpeechRecognition' in window)) {
      const SpeechRecognition = (window as any).webkitSpeechRecognition || (window as any).SpeechRecognition;
      recognitionRef.current = new SpeechRecognition();
      recognitionRef.current.continuous = false;
      recognitionRef.current.interimResults = false;
      recognitionRef.current.lang = 'en-US';

      recognitionRef.current.onresult = (event: any) => {
        const transcript = event.results[0][0].transcript;
        setQuery(transcript);
        setIsListening(false);
      };

      recognitionRef.current.onerror = (event: any) => {
        console.error('Speech recognition error:', event.error);
        setIsListening(false);
      };

      recognitionRef.current.onend = () => {
        setIsListening(false);
      };
    }
  }, []);

  // Auto-scroll to bottom of conversation
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [conversation]);

  // Handle query submission
  const handleSubmit = async (queryText: string) => {
    if (!queryText.trim()) return;

    // Add user message
    const userMessage: ConversationMessage = {
      id: `user-${Date.now()}`,
      type: 'user',
      content: queryText,
      timestamp: new Date()
    };
    setConversation(prev => [...prev, userMessage]);
    setQuery("");

    // Send to AI
    try {
      const response = await intelligenceQuery.mutateAsync(queryText);

      // Add assistant response
      const assistantMessage: ConversationMessage = {
        id: `assistant-${Date.now()}`,
        type: 'assistant',
        content: response.insight,
        timestamp: new Date(),
        chart_data: response
      };
      setConversation(prev => [...prev, assistantMessage]);
    } catch (error) {
      console.error("Query failed:", error);
    }
  };

  // Handle voice input with Web Speech API
  const handleVoiceInput = () => {
    if (!recognitionRef.current) {
      alert('Voice input is not supported in your browser. Please use Chrome, Edge, or Safari.');
      return;
    }

    if (isListening) {
      recognitionRef.current.stop();
      setIsListening(false);
    } else {
      try {
        recognitionRef.current.start();
        setIsListening(true);
      } catch (error) {
        console.error('Failed to start voice recognition:', error);
        setIsListening(false);
      }
    }
  };

  // Get chart type icon
  const getChartIcon = (type: string) => {
    switch (type) {
      case 'bar': return <BarChart3 className="h-4 w-4" />;
      case 'line': return <LineChartIcon className="h-4 w-4" />;
      case 'pie': return <PieChartIcon className="h-4 w-4" />;
      default: return <BarChart3 className="h-4 w-4" />;
    }
  };

  return (
    <div className="flex flex-col h-screen bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-950 dark:to-slate-900">
      {/* Header */}
      <header className="border-b bg-white/80 dark:bg-slate-900/80 backdrop-blur-sm sticky top-0 z-10">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-lg">
                <Sparkles className="h-6 w-6 text-white" />
              </div>
              <div>
                <h1 className="text-2xl font-bold bg-gradient-to-r from-indigo-600 to-purple-600 bg-clip-text text-transparent">
                  Intelligence Studio
                </h1>
                <p className="text-sm text-muted-foreground">
                  Ask anything about your campaigns
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm">
                <Share2 className="h-4 w-4 mr-2" />
                Share
              </Button>
              <Button variant="outline" size="sm">
                <Download className="h-4 w-4 mr-2" />
                Export
              </Button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden flex flex-col">
        {/* Conversation Area */}
        <div className="flex-1 overflow-y-auto px-4 py-6">
          <div className="container mx-auto max-w-5xl space-y-6">

            {/* Welcome Message */}
            {conversation.length === 0 && (
              <div className="text-center space-y-6 py-12">
                <div className="inline-flex p-4 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-2xl">
                  <Sparkles className="h-12 w-12 text-white" />
                </div>
                <div className="space-y-2">
                  <h2 className="text-3xl font-bold">Ask me anything</h2>
                  <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
                    I'll analyze your campaign data and create instant visualizations with AI-powered insights
                  </p>
                </div>

                {/* Suggested Queries */}
                <div className="space-y-3">
                  <p className="text-sm font-medium text-muted-foreground">Try asking:</p>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3 max-w-3xl mx-auto">
                    {SUGGESTED_QUERIES.map((suggestion, idx) => (
                      <Button
                        key={idx}
                        variant="outline"
                        className="justify-start text-left h-auto py-3 px-4"
                        onClick={() => handleSubmit(suggestion)}
                      >
                        <MessageSquare className="h-4 w-4 mr-2 flex-shrink-0" />
                        <span className="text-sm">{suggestion}</span>
                      </Button>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Conversation Messages */}
            {conversation.map((message) => (
              <div
                key={message.id}
                className={`flex ${message.type === 'user' ? 'justify-end' : 'justify-start'}`}
              >
                <div className={`max-w-4xl ${message.type === 'user' ? 'w-auto' : 'w-full'}`}>

                  {/* User Message */}
                  {message.type === 'user' && (
                    <div className="bg-indigo-600 text-white rounded-2xl px-4 py-3 inline-block">
                      <p className="text-sm">{message.content}</p>
                    </div>
                  )}

                  {/* Assistant Message */}
                  {message.type === 'assistant' && message.chart_data && (
                    <Card className="border-2">
                      <CardHeader>
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            {getChartIcon(message.chart_data.chart_type)}
                            <CardTitle className="text-lg">
                              {message.chart_data.query}
                            </CardTitle>
                          </div>
                          <Badge variant="secondary" className="text-xs">
                            {message.chart_data.execution_time_ms}ms
                          </Badge>
                        </div>
                      </CardHeader>
                      <CardContent className="space-y-4">

                        {/* Chart */}
                        <div className="bg-slate-50 dark:bg-slate-900 rounded-lg p-4">
                          <ChartRenderer
                            chartType={message.chart_data.chart_type}
                            data={message.chart_data.data}
                            height={300}
                          />
                        </div>

                        {/* AI Insight */}
                        <div className="bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
                          <div className="flex items-start gap-3">
                            <Lightbulb className="h-5 w-5 text-blue-600 dark:text-blue-400 flex-shrink-0 mt-0.5" />
                            <div className="space-y-2">
                              <p className="font-medium text-sm text-blue-900 dark:text-blue-100">
                                AI Insight
                              </p>
                              <p className="text-sm text-blue-800 dark:text-blue-200">
                                {message.chart_data.insight}
                              </p>
                            </div>
                          </div>
                        </div>

                        {/* Recommendations */}
                        {message.chart_data.recommendations.length > 0 && (
                          <div className="space-y-2">
                            <p className="text-sm font-medium flex items-center gap-2">
                              <TrendingUp className="h-4 w-4 text-green-600" />
                              Recommended Actions
                            </p>
                            <ul className="space-y-2">
                              {message.chart_data.recommendations.map((rec, idx) => (
                                <li key={idx} className="flex items-start gap-2 text-sm">
                                  <span className="text-green-600 mt-0.5">•</span>
                                  <span>{rec}</span>
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}

                        {/* Follow-up Questions */}
                        {message.chart_data.follow_up_questions.length > 0 && (
                          <div className="space-y-2">
                            <p className="text-sm font-medium text-muted-foreground">
                              Ask me more:
                            </p>
                            <div className="flex flex-wrap gap-2">
                              {message.chart_data.follow_up_questions.map((question, idx) => (
                                <Button
                                  key={idx}
                                  variant="outline"
                                  size="sm"
                                  onClick={() => handleSubmit(question)}
                                  className="text-xs"
                                >
                                  {question}
                                </Button>
                              ))}
                            </div>
                          </div>
                        )}

                      </CardContent>
                    </Card>
                  )}
                </div>
              </div>
            ))}

            {/* Loading State */}
            {intelligenceQuery.isPending && (
              <div className="flex justify-start">
                <Card className="max-w-4xl w-full border-2">
                  <CardContent className="py-8">
                    <div className="flex items-center justify-center gap-3">
                      <Loader2 className="h-6 w-6 animate-spin text-indigo-600" />
                      <p className="text-sm text-muted-foreground">
                        Analyzing your data...
                      </p>
                    </div>
                  </CardContent>
                </Card>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>
        </div>

        {/* Input Area */}
        <div className="border-t bg-white/80 dark:bg-slate-900/80 backdrop-blur-sm">
          <div className="container mx-auto max-w-5xl px-4 py-4">
            <div className="flex items-center gap-2">
              <div className="flex-1 relative">
                <Input
                  ref={inputRef}
                  type="text"
                  placeholder="Ask anything about your campaigns..."
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey) {
                      e.preventDefault();
                      handleSubmit(query);
                    }
                  }}
                  className="pr-12 h-12 text-base"
                  disabled={intelligenceQuery.isPending}
                />
                <Button
                  size="icon"
                  variant="ghost"
                  className="absolute right-1 top-1 h-10 w-10"
                  onClick={handleVoiceInput}
                >
                  <Mic className={`h-4 w-4 ${isListening ? 'text-red-500' : ''}`} />
                </Button>
              </div>
              <Button
                size="lg"
                onClick={() => handleSubmit(query)}
                disabled={!query.trim() || intelligenceQuery.isPending}
                className="h-12 px-6"
              >
                {intelligenceQuery.isPending ? (
                  <Loader2 className="h-5 w-5 animate-spin" />
                ) : (
                  <>
                    <Send className="h-5 w-5 mr-2" />
                    Send
                  </>
                )}
              </Button>
            </div>
            <p className="text-xs text-muted-foreground mt-2 text-center">
              Powered by AI • Ask in natural language • Get instant insights
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
