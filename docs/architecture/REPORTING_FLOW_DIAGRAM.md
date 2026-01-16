# Reporting Module - Visual Flow Diagram

## Complete Process Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    STEP 1: UPLOAD TEMPLATE                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │   Template File (Excel/CSV/PowerPoint)   │
        │                                          │
        │   Example: campaign_report.xlsx         │
        │   ┌────────────────────────────────┐   │
        │   │ Campaign Report                │   │
        │   │ ========================       │   │
        │   │ Total Spend: {{Total_Spend}}  │   │
        │   │ Total Clicks: {{Total_Clicks}}│   │
        │   │ ROAS: {{ROAS}}                │   │
        │   └────────────────────────────────┘   │
        └──────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │      SYSTEM ANALYZES TEMPLATE            │
        │                                          │
        │  ✓ Scans all sheets/slides               │
        │  ✓ Detects placeholder patterns          │
        │  ✓ Records locations (Sheet1!A5, etc)    │
        │  ✓ Identifies 3 placeholders             │
        └──────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    STEP 2: UPLOAD DATA                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │     Campaign Data (CSV/Excel)            │
        │                                          │
        │   Date       | Spend  | Clicks | ROAS   │
        │   2024-01-01 | 1500   | 450    | 3.2    │
        │   2024-01-02 | 1800   | 520    | 3.5    │
        │   2024-01-03 | 950    | 380    | 2.8    │
        │   2024-01-04 | 1100   | 420    | 3.1    │
        └──────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │      SYSTEM LOADS & VALIDATES DATA       │
        │                                          │
        │  ✓ Reads 4 rows, 4 columns               │
        │  ✓ Detects data types                    │
        │  ✓ Shows preview                         │
        │  ✓ Validates completeness                │
        └──────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    STEP 3: MAP FIELDS                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │       AUTO-MAPPING SUGGESTIONS           │
        │                                          │
        │  {{Total_Spend}}  →  Spend (Sum)        │
        │  {{Total_Clicks}} →  Clicks (Sum)       │
        │  {{ROAS}}         →  ROAS (Average)     │
        │                                          │
        │  [User can adjust mappings]              │
        └──────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │      MAPPING CONFIGURATION SAVED         │
        │                                          │
        │  Placeholder      | Column  | Method    │
        │  {{Total_Spend}}  | Spend   | Sum       │
        │  {{Total_Clicks}} | Clicks  | Sum       │
        │  {{ROAS}}         | ROAS    | Average   │
        └──────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  STEP 4: GENERATE REPORT                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │         DATA AGGREGATION                 │
        │                                          │
        │  Total_Spend  = SUM(1500+1800+950+1100) │
        │               = 5,350                    │
        │                                          │
        │  Total_Clicks = SUM(450+520+380+420)    │
        │               = 1,770                    │
        │                                          │
        │  ROAS         = AVG(3.2+3.5+2.8+3.1)    │
        │               = 3.15                     │
        └──────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │      PLACEHOLDER REPLACEMENT             │
        │                                          │
        │  1. Load template workbook               │
        │  2. Find cell with {{Total_Spend}}       │
        │  3. Replace with 5,350                   │
        │  4. Preserve formatting                  │
        │  5. Repeat for all placeholders          │
        └──────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────┐
        │         FINAL REPORT OUTPUT              │
        │                                          │
        │   ┌────────────────────────────────┐    │
        │   │ Campaign Report                │    │
        │   │ ========================       │    │
        │   │ Total Spend: $5,350.00        │    │
        │   │ Total Clicks: 1,770           │    │
        │   │ ROAS: 3.15x                   │    │
        │   └────────────────────────────────┘    │
        │                                          │
        │   + Sheet "Campaign_Data" with raw data  │
        └──────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  DOWNLOAD REPORT │
                    └──────────────────┘
```

---

## Technical Implementation Details

### Excel Report Generation

```
INPUT:
  - Template: campaign_report.xlsx
  - Data: campaign_data.csv
  - Mapping: {{{Total_Spend}} → Spend (Sum)}

PROCESS:
  1. openpyxl.load_workbook(template)
  2. For each placeholder in mapping:
     a. Parse location (e.g., "Sheet1!B5")
     b. Get data column (e.g., "Spend")
     c. Calculate aggregation:
        - Sum: df['Spend'].sum() = 5350
     d. Replace cell value:
        - sheet['B5'].value = 5350
     e. Preserve cell formatting
  3. Add "Campaign_Data" sheet with raw data
  4. Save to BytesIO buffer

OUTPUT:
  - Populated Excel file (bytes)
  - All formatting preserved
  - Formulas intact
  - Charts preserved
```

### PowerPoint Report Generation

```
INPUT:
  - Template: monthly_deck.pptx
  - Data: campaign_data.csv
  - Mapping: {{{Total_Spend}} → Spend (Sum)}

PROCESS:
  1. python-pptx.Presentation(template)
  2. For each slide:
     a. For each shape (text box, table):
        - Get text content
        - Search for placeholders
        - Replace with aggregated values
        - Format numbers (e.g., $5.3K)
     b. Preserve all formatting
  3. Save to BytesIO buffer

OUTPUT:
  - Populated PowerPoint file (bytes)
  - All layouts preserved
  - Images intact
  - Charts preserved
```

---

## Data Aggregation Methods Explained

### SUM Example
```
Data Column: [1500, 1800, 950, 1100]
Calculation: 1500 + 1800 + 950 + 1100
Result: 5,350
Formatted: $5,350.00
```

### AVERAGE Example
```
Data Column: [3.2, 3.5, 2.8, 3.1]
Calculation: (3.2 + 3.5 + 2.8 + 3.1) / 4
Result: 3.15
Formatted: 3.15x
```

### LATEST Example
```
Data Column: [1500, 1800, 950, 1100]
Calculation: Get last value
Result: 1100
Formatted: $1,100.00
```

### ALL ROWS Example
```
Data Column: ['Google', 'Facebook', 'LinkedIn']
Calculation: Join all values
Result: "Google, Facebook, LinkedIn"
Formatted: Google, Facebook, LinkedIn
```

---

## Error Handling Flow

```
┌─────────────────────────────────────┐
│   Placeholder: {{Total_Spend}}      │
└─────────────────────────────────────┘
              │
              ▼
    ┌──────────────────────┐
    │ Column exists?       │
    └──────────────────────┘
         │           │
        YES         NO
         │           │
         ▼           ▼
    ┌─────────┐  ┌──────────────┐
    │Calculate│  │ Show "N/A"   │
    │ Value   │  │ Log warning  │
    └─────────┘  └──────────────┘
         │
         ▼
    ┌──────────────────────┐
    │ Value is numeric?    │
    └──────────────────────┘
         │           │
        YES         NO
         │           │
         ▼           ▼
    ┌─────────┐  ┌──────────────┐
    │ Format  │  │ Use as-is    │
    │ Number  │  │              │
    └─────────┘  └──────────────┘
         │
         ▼
    ┌──────────────────────┐
    │ Replace placeholder  │
    └──────────────────────┘
```

---

## Performance Considerations

### Small Reports (< 1MB, < 10 placeholders)
- Processing time: < 2 seconds
- Memory usage: < 50MB
- Suitable for: Real-time generation

### Medium Reports (1-10MB, 10-50 placeholders)
- Processing time: 2-10 seconds
- Memory usage: 50-200MB
- Suitable for: On-demand generation

### Large Reports (> 10MB, > 50 placeholders)
- Processing time: 10-30 seconds
- Memory usage: 200-500MB
- Suitable for: Batch processing

---

## Security & Privacy

```
┌─────────────────────────────────────┐
│  File Upload                        │
│  ↓                                  │
│  Temporary Storage (session only)   │
│  ↓                                  │
│  Processing (in-memory)             │
│  ↓                                  │
│  Report Generation                  │
│  ↓                                  │
│  Download (to user's device)        │
│  ↓                                  │
│  Cleanup (delete from server)       │
└─────────────────────────────────────┘

✓ No permanent storage
✓ Session-based isolation
✓ Automatic cleanup
✓ No data logging
```

---

## Future Enhancements

1. **Conditional Formatting**
   - Apply colors based on thresholds
   - Highlight top/bottom performers

2. **Advanced Filters**
   - Filter data before aggregation
   - Multiple conditions support

3. **Chart Generation**
   - Auto-generate charts from data
   - Insert into template

4. **Template Library**
   - Save mapping configurations
   - Reuse for similar reports

5. **Scheduled Reports**
   - Automatic generation
   - Email delivery

6. **API Access**
   - Programmatic generation
   - Webhook integration
