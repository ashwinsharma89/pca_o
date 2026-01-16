#!/usr/bin/env python3
"""
Script to fix Visualizations-2 charts:
1. Reorder charts
2. Add Clicks-CTR chart
3. Rename Cost/Conv to CPA
"""

import re

# Read the file
with open('frontend/src/app/visualizations-2/page.tsx', 'r') as f:
    content = f.read()

# Replace "Cost/Conv" with "CPA" in the chart labels
content = content.replace('Cost/Conv', 'CPA')

# Find the section with monthly performance charts (around line 1836-1964)
# We need to reorder the charts in this section

# The new order should be:
# 1. Spend-CTR
# 2. Spend-Conversions  
# 3. Spend-ROAS
# 4. Conversions-CPA
# 5. Clicks-CTR (NEW)
# 6. Clicks-CPC
# 7. Clicks-Conversions (NEW)
# 8. Impressions-Clicks (NEW)
# 9. Impressions-Conversions (NEW)

# Define the new charts section
new_charts_section = '''                                        <CardContent className="space-y-4">
                                            {/* Chart 1: Spend with CTR overlay */}
                                            {dataAvailability.metrics.spend && dataAvailability.metrics.ctr && (
                                                <div className="h-[150px]" data-testid="chart-spend-ctr">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Spend</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-teal-500 rounded-sm"></span> Spend</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-orange-400 rounded-sm"></span> CTR</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `$${v >= 1000 ? (v / 1000).toFixed(0) + 'k' : v}`} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `${v.toFixed(2)}%`} domain={['dataMin - 0.05', 'dataMax + 0.05']} />
                                                            <Bar yAxisId="left" dataKey="spend" fill="#14b8a6" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="ctr" stroke="#fb923c" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 2: Spend with Conversions overlay */}
                                            {dataAvailability.metrics.spend && dataAvailability.metrics.conversions && (
                                                <div className="h-[150px]" data-testid="chart-spend-conversions">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Spend</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-blue-500 rounded-sm"></span> Spend</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-purple-400 rounded-sm"></span> Conversions</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `$${v >= 1000 ? (v / 1000).toFixed(0) + 'k' : v}`} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} domain={['dataMin * 0.95', 'dataMax * 1.05']} />
                                                            <Bar yAxisId="left" dataKey="spend" fill="#3b82f6" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="conversions" stroke="#a78bfa" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 3: Spend with ROAS overlay - only show if ROAS can be calculated */}
                                            {dataAvailability.metrics.spend && dataAvailability.metrics.roas && (
                                                <div className="h-[150px]" data-testid="chart-spend-roas">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Spend</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-indigo-500 rounded-sm"></span> Spend</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-yellow-400 rounded-sm"></span> ROAS</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `$${v >= 1000 ? (v / 1000).toFixed(0) + 'k' : v}`} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `${v.toFixed(1)}x`} domain={['dataMin * 0.9', 'dataMax * 1.1']} />
                                                            <Bar yAxisId="left" dataKey="spend" fill="#6366f1" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="roas" stroke="#facc15" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 4: Conversions with CPA overlay */}
                                            {dataAvailability.metrics.conversions && dataAvailability.metrics.cpa && (
                                                <div className="h-[150px]" data-testid="chart-conversions-cpa">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Conv</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-pink-500 rounded-sm"></span> Conversions</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-emerald-400 rounded-sm"></span> CPA</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `$${v.toFixed(0)}`} domain={['dataMin * 0.9', 'dataMax * 1.1']} />
                                                            <Bar yAxisId="left" dataKey="conversions" fill="#ec4899" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="costPerConv" stroke="#34d399" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 5: Clicks with CTR overlay */}
                                            {dataAvailability.metrics.clicks && dataAvailability.metrics.ctr && (
                                                <div className="h-[150px]" data-testid="chart-clicks-ctr">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Clicks</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-amber-500 rounded-sm"></span> Clicks</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-cyan-400 rounded-sm"></span> CTR</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `${v.toFixed(2)}%`} domain={['dataMin - 0.05', 'dataMax + 0.05']} />
                                                            <Bar yAxisId="left" dataKey="clicks" fill="#f59e0b" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="ctr" stroke="#22d3ee" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 6: Clicks with CPC overlay */}
                                            {dataAvailability.metrics.clicks && dataAvailability.metrics.cpc && (
                                                <div className="h-[150px]" data-testid="chart-clicks-cpc">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Clicks</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-amber-500 rounded-sm"></span> Clicks</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-cyan-400 rounded-sm"></span> CPC</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => `$${v.toFixed(2)}`} domain={['dataMin * 0.9', 'dataMax * 1.1']} />
                                                            <Bar yAxisId="left" dataKey="clicks" fill="#f59e0b" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="cpc" stroke="#22d3ee" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 7: Clicks with Conversions overlay */}
                                            {dataAvailability.metrics.clicks && dataAvailability.metrics.conversions && (
                                                <div className="h-[150px]" data-testid="chart-clicks-conversions">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Clicks</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-amber-500 rounded-sm"></span> Clicks</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-purple-400 rounded-sm"></span> Conversions</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <Bar yAxisId="left" dataKey="clicks" fill="#f59e0b" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="conversions" stroke="#a78bfa" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 8: Impressions with Clicks overlay */}
                                            {dataAvailability.metrics.impressions && dataAvailability.metrics.clicks && (
                                                <div className="h-[150px]" data-testid="chart-impressions-clicks">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Impr</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-slate-500 rounded-sm"></span> Impressions</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-amber-400 rounded-sm"></span> Clicks</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <Bar yAxisId="left" dataKey="impressions" fill="#64748b" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="clicks" stroke="#fbbf24" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}

                                            {/* Chart 9: Impressions with Conversions overlay */}
                                            {dataAvailability.metrics.impressions && dataAvailability.metrics.conversions && (
                                                <div className="h-[150px]" data-testid="chart-impressions-conversions">
                                                    <div className="flex items-center gap-2 mb-1">
                                                        <span className="text-xs font-medium text-muted-foreground w-16">Impr</span>
                                                        <div className="flex gap-3 text-xs">
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-slate-500 rounded-sm"></span> Impressions</span>
                                                            <span className="flex items-center gap-1"><span className="w-3 h-3 bg-purple-400 rounded-sm"></span> Conversions</span>
                                                        </div>
                                                    </div>
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <ComposedChart data={monthlyTrendData}>
                                                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.3} />
                                                            <XAxis dataKey="date" tick={{ fill: '#9ca3af', fontSize: 9 }} angle={-45} textAnchor="end" height={50} />
                                                            <YAxis yAxisId="left" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <YAxis yAxisId="right" orientation="right" tick={{ fill: '#9ca3af', fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                                                            <Bar yAxisId="left" dataKey="impressions" fill="#64748b" radius={[2, 2, 0, 0]} />
                                                            <Line yAxisId="right" type="monotone" dataKey="conversions" stroke="#a78bfa" strokeWidth={2} dot={false} />
                                                        </ComposedChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            )}
                                        </CardContent>'''

# Find and replace the charts section
# Pattern to match from <CardContent className="space-y-4"> to </CardContent> in the Monthly Performance Trends section
pattern = r'(<CardHeader>\s*<CardTitle className="flex items-center gap-2">\s*<TrendingUp className="h-5 w-5" />\s*Monthly Performance Trends\s*</CardTitle>\s*<CardDescription>Hover for more details</CardDescription>\s*</CardHeader>\s*)<CardContent className="space-y-4">.*?</CardContent>'

replacement = r'\1' + new_charts_section

content = re.sub(pattern, replacement, content, flags=re.DOTALL)

# Write the file back
with open('frontend/src/app/visualizations-2/page.tsx', 'w') as f:
    f.write(content)

print("✅ Successfully updated visualizations-2/page.tsx")
print("   - Renamed 'Cost/Conv' to 'CPA'")
print("   - Reordered charts as requested")
print("   - Added new charts: Clicks-CTR, Clicks-Conversions, Impressions-Clicks, Impressions-Conversions")
