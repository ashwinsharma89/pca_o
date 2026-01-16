# In-Depth Analysis Page Improvements

## Changes Required

### 1. Time Series Analysis - Weekly Line Chart ✅
**Current:** Daily data with dual-axis chart
**New:** Weekly aggregation with line chart

**Implementation:**
- Aggregate data by week using `pd.Grouper(freq='W')`
- Change from bar/area to line chart
- Keep dual-axis functionality

### 2. Smart Filters Position ⬇️
**Current:** At top of page
**New:** Move down below Custom Chart Builder

**Implementation:**
- Reorder sections in `render_deep_dive_page()`
- Custom Chart Builder → Smart Filters → Analysis

### 3. Custom Chart Builder - Always Open + Move to Top 🔝
**Current:** Collapsed expander at bottom
**New:** Always expanded, at top below header

**Implementation:**
- Change `expanded=False` to `expanded=True`
- Move before Smart Filters section
- Remove expander, make it a regular section

### 4. Custom Chart Builder-2 with KPIs on X-axis 📊
**Current:** Only one custom chart builder
**New:** Add second builder where KPIs can be on X-axis

**Implementation:**
- Create "Custom Chart Builder-2" section
- Allow selecting multiple KPIs for X-axis
- Create grouped bar/line charts

### 5. Spend-Click Analysis by Multiple Dimensions 📈
**Current:** Only by Platform
**New:** By Funnel, Source, Audience, Demographic, Campaign Type

**Implementation:**
- Detect available columns (funnel stages, channels, audience types, demographics)
- Create dropdown to select dimension
- Generate spend-click scatter for selected dimension

## Column Name Mappings

### Funnel Stages:
- `Funnel_Stage`, `Stage`, `Funnel`, `Marketing_Funnel`

### Source/Channel:
- `Source`, `Channel`, `Traffic_Source`, `Medium`, `utm_source`, `utm_medium`

### Audience Type:
- `Audience`, `Audience_Type`, `Audience_Segment`, `Segment`, `Target_Audience`

### Demographics:
- `Age`, `Age_Group`, `Age_Range`, `Gender`, `Location`, `Geography`, `Geo`, `Country`, `Region`

### Campaign Type:
- `Campaign_Type`, `CampaignType`, `Type`, `Objective`, `Campaign_Objective`

## Implementation Order

1. ✅ Create helper function for weekly aggregation
2. ✅ Update time series chart to weekly line chart
3. ✅ Move Custom Chart Builder to top (always open)
4. ✅ Move Smart Filters below Custom Chart Builder
5. ✅ Add Custom Chart Builder-2
6. ✅ Add multi-dimensional spend-click analysis

## Code Locations

- **File:** `streamlit_modular.py`
- **Function:** `render_deep_dive_page()` (lines 1067-1350)
- **Time Series:** Lines 1311-1325
- **Custom Chart Builder:** Lines 1352-1400
- **Spend-Click Analysis:** Need to add new section

## Testing Checklist

- [ ] Weekly time series shows correct aggregation
- [ ] Line chart displays properly
- [ ] Custom Chart Builder is at top and always open
- [ ] Smart Filters are below Custom Chart Builder
- [ ] Custom Chart Builder-2 works with KPIs on X-axis
- [ ] Spend-click analysis dropdown shows all available dimensions
- [ ] Each dimension generates correct scatter plot
