# 📚 Media Terminology & Data Hierarchy Guide

## Overview

Complete guide to media dimensions, terminologies, and data hierarchies across all major advertising platforms.

---

## 🎯 Platform Hierarchies

### **Google Ads**
```
Account
  └── Campaign
      └── Ad Group
          └── Ad
              └── Keyword
```

**Levels:**
1. **Account**: Top-level container (MCC or individual account)
2. **Campaign**: Budget, targeting, schedule settings
3. **Ad Group**: Keywords, bids, ad variations
4. **Ad**: Creative (text, responsive, display)
5. **Keyword**: Search terms, match types

### **Meta Ads (Facebook/Instagram)**
```
Account
  └── Campaign
      └── Ad Set
          └── Ad
              └── Creative
```

**Levels:**
1. **Account**: Business Manager account
2. **Campaign**: Objective (awareness, consideration, conversion)
3. **Ad Set**: Audience, placement, budget, schedule
4. **Ad**: Creative variations
5. **Creative**: Images, videos, copy

### **LinkedIn Ads**
```
Account
  └── Campaign Group
      └── Campaign
          └── Ad
              └── Creative
```

**Levels:**
1. **Account**: LinkedIn Campaign Manager account
2. **Campaign Group**: Budget allocation across campaigns
3. **Campaign**: Objective, audience, bid strategy
4. **Ad**: Sponsored content, message ads, dynamic ads
5. **Creative**: Images, videos, carousel

### **DV360 (Display & Video 360)**
```
Advertiser
  └── Insertion Order
      └── Line Item
          └── Creative
              └── Placement
```

**Levels:**
1. **Advertiser**: Top-level entity
2. **Insertion Order**: Budget, pacing, dates
3. **Line Item**: Targeting, bidding, inventory
4. **Creative**: Display, video, native ads
5. **Placement**: Specific sites/apps

### **CM360 (Campaign Manager 360)**
```
Advertiser
  └── Campaign
      └── Placement
          └── Creative
              └── Site
```

**Levels:**
1. **Advertiser**: Client/brand
2. **Campaign**: Marketing initiative
3. **Placement**: Ad slot/position
4. **Creative**: Ad assets
5. **Site**: Publisher/website

### **Snapchat Ads**
```
Account
  └── Campaign
      └── Ad Squad
          └── Ad
              └── Creative
```

**Levels:**
1. **Account**: Snapchat Ads Manager account
2. **Campaign**: Objective, budget
3. **Ad Squad**: Audience, placement, schedule
4. **Ad**: Creative format
5. **Creative**: Snap, story, collection

### **TikTok Ads**
```
Account
  └── Campaign
      └── Ad Group
          └── Ad
              └── Creative
```

**Levels:**
1. **Account**: TikTok Ads Manager
2. **Campaign**: Objective, budget
3. **Ad Group**: Targeting, placement, bid
4. **Ad**: Creative variations
5. **Creative**: Video, image, carousel

### **Twitter Ads**
```
Account
  └── Campaign
      └── Ad Group
          └── Ad
              └── Creative
```

**Levels:**
1. **Account**: Twitter Ads account
2. **Campaign**: Objective, budget
3. **Ad Group**: Targeting, bid
4. **Ad**: Tweet, card
5. **Creative**: Media, copy

### **Pinterest Ads**
```
Account
  └── Campaign
      └── Ad Group
          └── Ad
              └── Pin
```

**Levels:**
1. **Account**: Pinterest Business account
2. **Campaign**: Objective, budget
3. **Ad Group**: Targeting, bid
4. **Ad**: Promoted pin
5. **Pin**: Image, video, carousel

---

## 📊 Standard Dimensions

### **Campaign Dimensions**
- **Campaign Name/ID**: Unique identifier
- **Campaign Type**: Search, Display, Video, Shopping
- **Campaign Status**: Active, Paused, Ended
- **Campaign Objective**: Awareness, Consideration, Conversion
- **Campaign Budget**: Daily, Lifetime
- **Campaign Start/End Date**: Flight dates

### **Ad Group/Ad Set Dimensions**
- **Ad Group Name/ID**: Container for ads
- **Ad Set Name/ID**: (Meta terminology)
- **Ad Squad Name/ID**: (Snapchat terminology)
- **Targeting**: Audience segments
- **Bid Strategy**: Manual, Auto, Target CPA, Target ROAS

### **Ad Dimensions**
- **Ad Name/ID**: Individual ad identifier
- **Ad Type**: Text, Image, Video, Carousel, Collection
- **Ad Status**: Active, Paused, Disapproved
- **Ad Format**: Single image, video, slideshow

### **Creative Dimensions**
- **Creative Name/ID**: Asset identifier
- **Creative Type**: Image, Video, HTML5
- **Creative Size**: 300x250, 728x90, 1920x1080
- **Creative Format**: JPG, PNG, MP4, GIF

### **Placement Dimensions**
- **Placement**: Feed, Stories, Reels, Audience Network
- **Site**: Publisher website/app
- **Position**: Above fold, below fold, sidebar
- **Device**: Desktop, Mobile, Tablet

### **Targeting Dimensions**
- **Location**: Country, State, City, DMA, Zip
- **Age**: 18-24, 25-34, 35-44, 45-54, 55-64, 65+
- **Gender**: Male, Female, Unknown
- **Device**: Desktop, Mobile, Tablet
- **OS**: iOS, Android, Windows
- **Browser**: Chrome, Safari, Firefox, Edge

### **Audience Dimensions**
- **Audience Name**: Custom, Lookalike, Interest
- **Audience Type**: Remarketing, Prospecting
- **Audience Size**: Reach potential
- **Segment**: Demographics, Interests, Behaviors

### **Keyword Dimensions** (Search)
- **Keyword**: Search term
- **Match Type**: Exact, Phrase, Broad, Modified Broad
- **Quality Score**: 1-10 rating
- **Search Term**: Actual user query

### **Time Dimensions**
- **Date**: YYYY-MM-DD
- **Hour**: 0-23
- **Day of Week**: Monday-Sunday
- **Week**: Week number
- **Month**: 1-12
- **Quarter**: Q1-Q4
- **Year**: YYYY

---

## 📈 Standard Metrics

### **Impression Metrics**
- **Impressions**: Number of times ad shown
- **Reach**: Unique users who saw ad
- **Frequency**: Impressions / Reach
- **Share of Voice**: Your impressions / Total impressions

### **Click Metrics**
- **Clicks**: Number of clicks
- **CTR (Click-Through Rate)**: (Clicks / Impressions) × 100
- **CPC (Cost Per Click)**: Spend / Clicks
- **Link Clicks**: Clicks on ad link (vs other clicks)

### **Engagement Metrics**
- **Engagements**: Likes, shares, comments, saves
- **Engagement Rate**: (Engagements / Impressions) × 100
- **Video Views**: Number of video plays
- **Video View Rate**: (Video Views / Impressions) × 100
- **Video Completion Rate**: % who watched to end

### **Conversion Metrics**
- **Conversions**: Desired actions completed
- **Conversion Rate**: (Conversions / Clicks) × 100
- **CPA (Cost Per Acquisition)**: Spend / Conversions
- **ROAS (Return on Ad Spend)**: Revenue / Spend
- **ROI (Return on Investment)**: (Revenue - Spend) / Spend × 100

### **Cost Metrics**
- **Spend**: Total amount spent
- **CPM (Cost Per Mille)**: (Spend / Impressions) × 1000
- **CPC (Cost Per Click)**: Spend / Clicks
- **CPV (Cost Per View)**: Spend / Video Views
- **CPA (Cost Per Acquisition)**: Spend / Conversions

### **Revenue Metrics**
- **Revenue**: Total revenue generated
- **ROAS**: Revenue / Spend
- **AOV (Average Order Value)**: Revenue / Conversions
- **LTV (Lifetime Value)**: Customer lifetime revenue

### **Quality Metrics**
- **Quality Score**: Google Ads quality rating (1-10)
- **Relevance Score**: Meta Ads relevance rating (1-10)
- **Ad Rank**: Position in auction
- **Landing Page Experience**: User experience rating

---

## 🔄 Data Granularities

### **Time Granularities**
1. **Hourly**: Hour-by-hour performance
2. **Daily**: Day-by-day performance
3. **Weekly**: Week-by-week aggregation
4. **Monthly**: Month-by-month aggregation
5. **Quarterly**: Q1, Q2, Q3, Q4
6. **Yearly**: Annual performance

### **Campaign Granularities**
1. **Account Level**: All campaigns combined
2. **Campaign Level**: Individual campaign performance
3. **Ad Group/Set Level**: Within campaign breakdown
4. **Ad Level**: Individual ad performance
5. **Creative Level**: Creative asset performance
6. **Keyword Level**: Keyword-specific metrics

### **Audience Granularities**
1. **Overall**: All audiences combined
2. **Demographic**: Age, gender breakdown
3. **Geographic**: Location-based
4. **Device**: Desktop, mobile, tablet
5. **Audience Segment**: Custom, lookalike, interest

### **Placement Granularities**
1. **Overall**: All placements combined
2. **Platform**: Facebook, Instagram, Audience Network
3. **Position**: Feed, Stories, Reels
4. **Site**: Specific publisher
5. **App**: Mobile app placement

---

## 🎯 Common Grouping Scenarios

### **Campaign + Placement**
```sql
SELECT Campaign, Placement, 
       SUM(Spend), SUM(Conversions), AVG(ROAS)
FROM data
GROUP BY Campaign, Placement
```

### **Platform + Device**
```sql
SELECT Platform, Device,
       SUM(Impressions), SUM(Clicks), AVG(CTR)
FROM data
GROUP BY Platform, Device
```

### **Month + Campaign**
```sql
SELECT Month, Campaign,
       SUM(Spend), SUM(Revenue), AVG(ROAS)
FROM data
GROUP BY Month, Campaign
```

### **Ad Group + Keyword**
```sql
SELECT Ad_Group, Keyword,
       SUM(Clicks), AVG(CPC), AVG(Quality_Score)
FROM data
GROUP BY Ad_Group, Keyword
```

---

## 📅 Time Comparison Patterns

### **Month-over-Month (MoM)**
```
Current Month vs Previous Month
Example: November 2024 vs October 2024
```

### **Week-over-Week (WoW)**
```
Current Week vs Previous Week
Example: Week 46 vs Week 45
```

### **Year-over-Year (YoY)**
```
Current Period vs Same Period Last Year
Example: Q4 2024 vs Q4 2023
```

### **Last N Periods**
```
Last 2 months, Last 4 weeks, Last 7 days
Example: Oct-Nov 2024 vs Aug-Sep 2024
```

### **Quarter Comparison**
```
Q4 2024 vs Q3 2024
Q4 2024 vs Q4 2023
```

---

## 🔍 Seasonality Patterns

### **Monthly Seasonality**
- **Peak Months**: November (Black Friday), December (Holiday)
- **Low Months**: January (post-holiday), August (summer)
- **Back-to-School**: August-September
- **Holiday Season**: November-December

### **Weekly Seasonality**
- **Peak Days**: Tuesday, Wednesday (mid-week)
- **Low Days**: Saturday, Sunday (weekend)
- **Monday**: Week start, lower engagement
- **Friday**: Week end, higher engagement

### **Daily Seasonality**
- **Peak Hours**: 12pm-2pm (lunch), 7pm-9pm (evening)
- **Low Hours**: 2am-6am (overnight)
- **Morning**: 8am-11am (commute, work start)
- **Evening**: 6pm-10pm (after work)

---

## 🎯 Platform-Specific Terminology

### **Google Ads**
- **Search Network**: Google Search results
- **Display Network**: GDN (Google Display Network)
- **Shopping**: Product listing ads
- **Performance Max**: Automated campaign type
- **Smart Bidding**: AI-powered bidding

### **Meta Ads**
- **Advantage+**: Automated campaigns
- **Lookalike Audiences**: Similar to existing customers
- **Custom Audiences**: Uploaded lists, website visitors
- **Dynamic Ads**: Product catalog ads
- **Instant Experience**: Full-screen mobile experience

### **LinkedIn Ads**
- **Sponsored Content**: Native feed ads
- **Message Ads**: Direct InMail
- **Dynamic Ads**: Personalized ads
- **Text Ads**: Sidebar ads
- **Matched Audiences**: Retargeting

### **DV360**
- **Programmatic**: Automated ad buying
- **Private Marketplace (PMP)**: Invitation-only deals
- **Programmatic Guaranteed**: Reserved inventory
- **Open Auction**: Public marketplace
- **Contextual Targeting**: Content-based targeting

---

## 📊 KPI Calculations

### **CTR (Click-Through Rate)**
```
CTR = (Clicks / Impressions) × 100
Example: (1,000 / 50,000) × 100 = 2.0%
```

### **CPC (Cost Per Click)**
```
CPC = Spend / Clicks
Example: $1,000 / 500 = $2.00
```

### **CPM (Cost Per Thousand Impressions)**
```
CPM = (Spend / Impressions) × 1,000
Example: ($1,000 / 50,000) × 1,000 = $20.00
```

### **CPA (Cost Per Acquisition)**
```
CPA = Spend / Conversions
Example: $1,000 / 20 = $50.00
```

### **ROAS (Return on Ad Spend)**
```
ROAS = Revenue / Spend
Example: $5,000 / $1,000 = 5.0x
```

### **Conversion Rate**
```
Conversion Rate = (Conversions / Clicks) × 100
Example: (20 / 500) × 100 = 4.0%
```

### **Frequency**
```
Frequency = Impressions / Reach
Example: 50,000 / 40,000 = 1.25
```

---

**This guide covers all major platforms, dimensions, and terminologies used in digital advertising!** 📚

