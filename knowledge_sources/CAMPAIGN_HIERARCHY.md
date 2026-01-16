# Campaign Hierarchy & Structure - Reference Sources

## Overview
Understanding campaign hierarchy and structure across different advertising platforms is crucial for proper organization, reporting, and optimization.

## Campaign Hierarchy Sources

### Reporting Hierarchy
- **Reporting Hierarchy** - CentraHub CRM
  - URL: https://www.centrahubcrm.com/support-center/reporting-hierarchy
  - Topics: Hierarchical reporting structure, metric rollup, organizational levels

### Google Ads Hierarchy
- **Google Ads Reporting** - AdNabu
  - URL: https://blog.adnabu.com/google-ads/google-ads-reporting/
  - Topics: Account → Campaign → Ad Group → Ad → Keyword structure

### DV360 Hierarchy
- **DV360 Hierarchy** - 365 Lessons
  - URL: https://365lessons.in/dv360-hierarchy/
  - Topics: Partner → Advertiser → Campaign → Insertion Order → Line Item → Creative

- **DV360 Campaign Structure Best Practices** - Analytics Liv
  - URL: https://analyticsliv.com/blogs/dv360-campaign-structure-best-practices
  - Topics: Structure optimization, naming conventions, organization

### Campaign Manager 360 Hierarchy
- **Campaign Manager 360 Reporting** - Google Support
  - URL: https://support.google.com/campaignmanager/answer/2709362?hl=en
  - Topics: Advertiser → Campaign → Site → Placement → Creative structure

### Meta Ads Hierarchy
- **Meta Ads Report Explained** - Agorapulse
  - URL: https://support.agorapulse.com/en/articles/10137532-meta-ads-report-explained
  - Topics: Campaign → Ad Set → Ad structure

- **Understanding Meta Ads Reporting: A Complete Guide** - Magic Brief
  - URL: https://magicbrief.com/post/understanding-meta-ads-reporting-a-complete-guide
  - Topics: Account structure, reporting levels, optimization

### LinkedIn Ads Hierarchy
- **LinkedIn Ads Campaign Structure & Roles** - Sprinklr
  - URL: https://www.sprinklr.com/help/articles/getting-started/linkedin-ads-campaign-structure-roles/645234780d27fc559bbe4b62
  - Topics: Account → Campaign Group → Campaign → Ad structure

## Standard Campaign Hierarchy Models

### Google Ads Structure
```
Account
└── Campaign (Budget, targeting, settings)
    └── Ad Group (Keywords, audiences)
        └── Ads (Creative)
            └── Keywords/Audiences (Targeting)
```

**Levels:**
1. **Account**: Top level, billing, access control
2. **Campaign**: Budget, location, schedule, network
3. **Ad Group**: Keyword themes, audience groups
4. **Ad**: Creative assets, copy, extensions
5. **Keyword**: Match types, bids

**Key Settings by Level:**
- **Account**: Billing, users, linked accounts
- **Campaign**: Daily budget, location, language, bid strategy, networks
- **Ad Group**: Default bids, audience targeting
- **Ad**: Headlines, descriptions, URLs, assets
- **Keyword**: Match type, bid adjustments

### Meta Ads Structure
```
Ad Account
└── Campaign (Objective, budget optimization)
    └── Ad Set (Audience, placement, schedule, budget)
        └── Ad (Creative, copy, CTA)
```

**Levels:**
1. **Ad Account**: Billing, pixel, catalogs
2. **Campaign**: Objective (Awareness, Consideration, Conversion)
3. **Ad Set**: Audience, placements, budget, schedule
4. **Ad**: Creative, copy, format

**Key Settings by Level:**
- **Account**: Payment method, business settings
- **Campaign**: Objective, campaign budget optimization (CBO)
- **Ad Set**: Targeting, placements, optimization, budget (if not CBO)
- **Ad**: Creative, text, call-to-action

### LinkedIn Ads Structure
```
Account
└── Campaign Group (Budget, schedule)
    └── Campaign (Objective, audience)
        └── Ad (Creative, format)
```

**Levels:**
1. **Account**: Company page, billing
2. **Campaign Group**: Total budget, date range
3. **Campaign**: Objective, audience, bid
4. **Ad**: Creative, format, destination

**Key Settings by Level:**
- **Account**: Payment, company association
- **Campaign Group**: Total budget, run dates
- **Campaign**: Objective, targeting, bid strategy, daily budget
- **Ad**: Format, creative, copy, CTA

### DV360 (Programmatic) Structure
```
Partner
└── Advertiser
    └── Campaign
        └── Insertion Order (Budget, pacing)
            └── Line Item (Targeting, inventory)
                └── Creative (Assets)
```

**Levels:**
1. **Partner**: Agency/holding company level
2. **Advertiser**: Client/brand level
3. **Campaign**: Marketing initiative
4. **Insertion Order**: Budget container, pacing
5. **Line Item**: Targeting, inventory, bidding
6. **Creative**: Ad assets

**Key Settings by Level:**
- **Partner**: Billing, users, global settings
- **Advertiser**: Brand settings, pixels, audiences
- **Campaign**: High-level organization
- **Insertion Order**: Budget, pacing, performance goals
- **Line Item**: Targeting, inventory, bid strategy, frequency caps
- **Creative**: Assets, sizes, click tracking

### Campaign Manager 360 Structure
```
Advertiser
└── Campaign
    └── Site (Publisher/placement group)
        └── Placement (Specific ad unit)
            └── Creative (Ad asset)
                └── Creative Assignment (Rotation)
```

**Levels:**
1. **Advertiser**: Client/brand
2. **Campaign**: Marketing initiative
3. **Site**: Publisher or placement group
4. **Placement**: Specific ad unit/position
5. **Creative**: Ad assets
6. **Creative Assignment**: Ad rotation settings

### The Trade Desk Structure
```
Advertiser
└── Campaign
    └── Ad Group
        └── Creative
```

**Levels:**
1. **Advertiser**: Client/brand
2. **Campaign**: Budget, flight dates, goals
3. **Ad Group**: Targeting, inventory, bidding
4. **Creative**: Ad assets, formats

### Snapchat Ads Structure
```
Ad Account
└── Campaign (Objective, budget)
    └── Ad Set (Audience, placement, schedule)
        └── Ad (Creative, format)
```

**Levels:**
1. **Ad Account**: Billing, pixel
2. **Campaign**: Objective, campaign budget
3. **Ad Set**: Targeting, placements, optimization
4. **Ad**: Creative, format, CTA

## Campaign Hierarchy Best Practices

### Naming Conventions

#### Standard Format
```
[Brand]_[Channel]_[Campaign Type]_[Audience]_[Geo]_[Date]

Example:
Nike_Google_Search_Brand_US_Q1-2024
Nike_Meta_Prospecting_Lookalike_UK_Jan2024
```

#### Components
1. **Brand/Product**: Client or product name
2. **Channel**: Platform (Google, Meta, LinkedIn)
3. **Campaign Type**: Search, Display, Video, Prospecting, Retargeting
4. **Audience**: Target audience segment
5. **Geography**: Target location
6. **Date**: Time period or version

#### Ad Group/Ad Set Naming
```
[Campaign Name]_[Specific Target]_[Creative Version]

Example:
Nike_Google_Search_Brand_US_Q1-2024_Running-Shoes_V1
```

### Structure Optimization

#### Campaign Level
**Best Practices:**
- One objective per campaign
- Separate brand vs. non-brand
- Separate prospecting vs. retargeting
- Geographic segmentation when needed
- Budget appropriate to goals

**Common Mistakes:**
- ❌ Mixing objectives in one campaign
- ❌ Too many campaigns (fragmented data)
- ❌ Too few campaigns (lack of control)
- ❌ Inconsistent naming

#### Ad Group/Ad Set Level
**Best Practices:**
- Tight theme per ad group
- 2-3 ads per ad group for testing
- Logical audience grouping
- Appropriate bid strategy per group

**Common Mistakes:**
- ❌ Too broad keyword themes
- ❌ Single ad (no testing)
- ❌ Mixing audience types
- ❌ Inconsistent bidding

#### Ad/Creative Level
**Best Practices:**
- Multiple variations for testing
- Consistent messaging per group
- Proper tracking parameters
- Regular creative refresh

**Common Mistakes:**
- ❌ No creative testing
- ❌ Inconsistent messaging
- ❌ Missing tracking
- ❌ Stale creative

## Hierarchy Comparison Table

| Platform | Levels | Budget Level | Targeting Level | Creative Level |
|----------|--------|--------------|-----------------|----------------|
| Google Ads | 4 | Campaign | Ad Group | Ad |
| Meta Ads | 3 | Campaign/Ad Set | Ad Set | Ad |
| LinkedIn | 3 | Campaign Group/Campaign | Campaign | Ad |
| DV360 | 6 | Insertion Order | Line Item | Creative |
| CM360 | 5 | Campaign | Placement | Creative |
| TTD | 3 | Campaign | Ad Group | Creative |
| Snapchat | 3 | Campaign/Ad Set | Ad Set | Ad |
| Bing Ads | 4 | Campaign | Ad Group | Ad |

## Reporting by Hierarchy Level

### Account Level Reporting
**Metrics:**
- Total spend across all campaigns
- Overall ROAS/ROI
- Account-level conversion tracking
- Cross-campaign performance

**Use Cases:**
- Executive reporting
- Budget allocation decisions
- Platform comparison
- Annual planning

### Campaign Level Reporting
**Metrics:**
- Campaign spend and pacing
- Campaign-level ROAS/CPA
- Objective achievement
- Budget utilization

**Use Cases:**
- Campaign performance review
- Budget reallocation
- Objective tracking
- Stakeholder updates

### Ad Group/Ad Set Level Reporting
**Metrics:**
- Targeting performance
- Audience segment results
- Keyword/placement performance
- Bid efficiency

**Use Cases:**
- Optimization decisions
- Targeting refinement
- Bid adjustments
- A/B test analysis

### Ad/Creative Level Reporting
**Metrics:**
- Creative performance
- Message testing results
- Format comparison
- Asset performance

**Use Cases:**
- Creative optimization
- Message testing
- Format selection
- Asset allocation

## Cross-Platform Hierarchy Mapping

### Mapping for Unified Reporting
```
Universal Hierarchy:
Account → Campaign → Ad Group → Ad

Platform Mappings:
- Google Ads: Direct match
- Meta Ads: Account → Campaign → Ad Set → Ad
- LinkedIn: Account → Campaign → (skip) → Ad
- DV360: Advertiser → IO → Line Item → Creative
- CM360: Advertiser → Campaign → Placement → Creative
```

### Normalization Strategy
1. **Define standard levels** for reporting
2. **Map platform-specific** to standard
3. **Aggregate appropriately** for cross-platform views
4. **Maintain granularity** for platform-specific optimization

## Usage Guidelines

### For Campaign Setup
1. Review platform-specific hierarchy
2. Plan structure before building
3. Apply naming conventions
4. Document structure decisions

### For Reporting
1. Understand hierarchy levels
2. Report at appropriate level for audience
3. Drill down for optimization
4. Roll up for executive views

### For Optimization
1. Analyze at granular levels
2. Make changes at appropriate level
3. Monitor impact across hierarchy
4. Document changes and results

## Quality Notes

- Sources from official platform documentation
- Best practices from industry experts
- Real-world structure examples
- Platform-specific considerations

## Cross-References

- See CHANNEL_SOURCES.md for platform-specific details
- See KPI_METRICS_SOURCES.md for metrics by level
- See CAMPAIGN_ANALYTICS_SOURCES.md for analysis frameworks

---

**Last Updated**: 2025-01-23
**Total Sources**: 7+ hierarchy-specific sources
**Platforms Covered**: Google Ads, Meta, LinkedIn, DV360, CM360, TTD, Snapchat, Bing
**Primary Topics**: Campaign Structure, Hierarchy, Naming Conventions, Best Practices
