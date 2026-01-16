#!/usr/bin/env python3
"""
Script to fix chart line visibility by adding cumulative sums
"""

# Read the file
with open('frontend/src/app/visualizations-2/page.tsx', 'r') as f:
    lines = f.readlines()

# Find and update the monthlyTrendData section
in_monthly_trend = False
monthly_trend_start = -1
monthly_trend_end = -1

for i, line in enumerate(lines):
    if '// Aggregate trend data to monthly level for cleaner charts' in line:
        in_monthly_trend = True
        monthly_trend_start = i
    elif in_monthly_trend and '}, [trendData]);' in line:
        monthly_trend_end = i
        break

# Replace the monthlyTrendData section
if monthly_trend_start != -1 and monthly_trend_end != -1:
    new_section = '''    // Aggregate trend data to monthly level for cleaner charts
    const monthlyTrendData = useMemo(() => {
        if (!trendData || trendData.length === 0) return [];

        const monthlyAgg: { [key: string]: any } = {};

        trendData.forEach((d: any) => {
            const date = new Date(d.date);
            const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
            const monthLabel = date.toLocaleDateString('en-US', { month: 'short' }) + ' ' + String(date.getFullYear()).slice(-2);

            if (!monthlyAgg[monthKey]) {
                monthlyAgg[monthKey] = {
                    date: monthLabel,
                    monthKey: monthKey,
                    spend: 0,
                    clicks: 0,
                    conversions: 0,
                    impressions: 0,
                    revenue: 0
                };
            }

            monthlyAgg[monthKey].spend += d.spend || 0;
            monthlyAgg[monthKey].clicks += d.clicks || 0;
            monthlyAgg[monthKey].conversions += d.conversions || 0;
            monthlyAgg[monthKey].impressions += d.impressions || 0;
            monthlyAgg[monthKey].revenue += d.revenue || 0;
        });

        // Convert to array and sort by month
        const sortedData = Object.values(monthlyAgg)
            .sort((a: any, b: any) => a.monthKey.localeCompare(b.monthKey));

        // Calculate cumulative sums and derived metrics
        let cumulativeClicks = 0;
        let cumulativeConversions = 0;
        let cumulativeImpressions = 0;

        return sortedData.map((m: any) => {
            cumulativeClicks += m.clicks;
            cumulativeConversions += m.conversions;
            cumulativeImpressions += m.impressions;

            return {
                ...m,
                cpc: m.clicks > 0 ? m.spend / m.clicks : 0,
                ctr: m.impressions > 0 ? (m.clicks / m.impressions) * 100 : 0,
                roas: m.spend > 0 ? (m.revenue || 0) / m.spend : 0,
                costPerConv: m.conversions > 0 ? m.spend / m.conversions : 0,
                // Cumulative sums for better line visibility
                cumulativeClicks,
                cumulativeConversions,
                cumulativeImpressions
            };
        });
    }, [trendData]);
'''
    lines[monthly_trend_start:monthly_trend_end+1] = [new_section]

# Now update the chart labels and dataKeys
output_lines = []
for line in lines:
    # Update Clicks-Conversions chart
    if 'Chart 7: Clicks with Conversions overlay' in line:
        line = line.replace('Clicks with Conversions overlay', 'Clicks with Cumulative Conversions overlay')
    elif 'chart-clicks-conversions' in line and 'Conversions</span>' in line and 'Σ' not in line:
        line = line.replace('Conversions</span>', 'Σ Conversions</span>')
    elif 'chart-clicks-conversions' in line and 'dataKey="conversions"' in line:
        line = line.replace('dataKey="conversions"', 'dataKey="cumulativeConversions"')
    
    # Update Impressions-Clicks chart
    elif 'Chart 8: Impressions with Clicks overlay' in line:
        line = line.replace('Impressions with Clicks overlay', 'Impressions with Cumulative Clicks overlay')
    elif 'chart-impressions-clicks' in line and 'Clicks</span>' in line and 'Σ' not in line:
        line = line.replace('Clicks</span>', 'Σ Clicks</span>')
    elif 'chart-impressions-clicks' in line and 'dataKey="clicks"' in line and 'Line' in line:
        line = line.replace('dataKey="clicks"', 'dataKey="cumulativeClicks"')
    
    # Update Impressions-Conversions chart
    elif 'Chart 9: Impressions with Conversions overlay' in line:
        line = line.replace('Impressions with Conversions overlay', 'Impressions with Cumulative Conversions overlay')
    elif 'chart-impressions-conversions' in line and 'Conversions</span>' in line and 'Σ' not in line:
        line = line.replace('Conversions</span>', 'Σ Conversions</span>')
    elif 'chart-impressions-conversions' in line and 'dataKey="conversions"' in line:
        line = line.replace('dataKey="conversions"', 'dataKey="cumulativeConversions"')
    
    output_lines.append(line)

# Write the file back
with open('frontend/src/app/visualizations-2/page.tsx', 'w') as f:
    f.writelines(output_lines)

print("✅ Successfully updated chart line visibility")
print("   - Added cumulative sums to monthlyTrendData")
print("   - Clicks-Conversions: Now shows cumulative conversions (Σ Conversions)")
print("   - Impressions-Clicks: Now shows cumulative clicks (Σ Clicks)")
print("   - Impressions-Conversions: Now shows cumulative conversions (Σ Conversions)")
print("   - Lines will now be more visible with upward trends")
