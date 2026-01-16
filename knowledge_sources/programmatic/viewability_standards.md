# Programmatic Viewability Standards & Optimization

## Overview
Viewability measures whether an ad had the opportunity to be seen by a user. It's a critical quality metric for programmatic advertising that directly impacts campaign effectiveness and brand safety.

## Industry Standards

### MRC (Media Rating Council) Standards

#### Display Ads
**Viewable Impression Definition**:
- **50% of pixels** in view
- For **1 continuous second** minimum
- Measured by accredited vendors

#### Video Ads
**Viewable Impression Definition**:
- **50% of pixels** in view
- For **2 continuous seconds** minimum
- Must be audible (sound on or user-initiated)

### GroupM Standards (Premium)
**Enhanced Viewability**:
- **100% of pixels** in view
- For **1 second** (display) or **2 seconds** (video)
- Higher quality threshold

## Viewability Benchmarks

### By Format

| Format | Industry Average | Good | Excellent |
|--------|-----------------|------|-----------|
| Display Banner | 50-55% | 65-70% | 75%+ |
| Native Display | 60-65% | 70-75% | 80%+ |
| Video (In-stream) | 65-70% | 75-80% | 85%+ |
| Video (Out-stream) | 55-60% | 65-70% | 75%+ |
| Mobile Display | 45-50% | 60-65% | 70%+ |
| Mobile Video | 60-65% | 70-75% | 80%+ |

### By Placement

| Placement Type | Avg Viewability | Notes |
|---------------|----------------|-------|
| Above the Fold | 70-80% | Premium positioning |
| Below the Fold | 30-40% | Lower visibility |
| In-Content | 65-75% | Natural user engagement |
| Sidebar | 40-50% | Often ignored |
| Interstitial | 85-95% | High visibility, may impact UX |
| In-Feed (Social) | 60-70% | Scroll-dependent |

## Factors Affecting Viewability

### 1. Ad Position
**Impact**: 40-50% of viewability variance
- **Above the Fold**: 2-3x higher viewability
- **First Screen**: Optimal for desktop
- **Scroll Position**: Critical for mobile

### 2. Ad Size
**Impact**: 20-30% of viewability variance

| Size | Viewability Impact |
|------|-------------------|
| 300×250 | Baseline (50-55%) |
| 728×90 | Lower (40-45%) |
| 300×600 | Higher (60-65%) |
| 970×250 | Highest (70-75%) |
| 320×50 (Mobile) | Lowest (35-40%) |

### 3. Page Load Speed
**Impact**: 15-25% of viewability variance
- **< 2 seconds**: Optimal viewability
- **2-4 seconds**: 10-15% viewability drop
- **> 4 seconds**: 25-30% viewability drop

### 4. User Behavior
**Impact**: 10-20% of viewability variance
- **Bounce Rate**: High bounce = low viewability
- **Time on Site**: Longer time = higher viewability
- **Scroll Depth**: Deeper scroll = more viewable inventory

### 5. Device Type
**Impact**: 15-20% of viewability variance
- **Desktop**: 55-60% average
- **Mobile Web**: 45-50% average
- **Mobile App**: 60-65% average (better control)

## Optimization Strategies

### 1. Inventory Selection

#### Premium Publishers
**Characteristics**:
- Viewability > 70%
- Brand-safe content
- Quality user experience
- Transparent reporting

**Examples**:
- Major news sites (NYT, WSJ, CNN)
- Premium content networks
- Verified app inventory

#### Viewability-Optimized Deals
```
PMP Deal Setup:
- Minimum viewability: 70%
- Above-the-fold only
- Desktop + Mobile App
- Exclude below-fold inventory
```

### 2. Targeting Optimization

#### Contextual Targeting
- **High-Engagement Content**: News, entertainment, sports
- **Long-Form Content**: Articles > 500 words
- **Video Content Pages**: Higher video viewability

#### Behavioral Targeting
- **Engaged Users**: Time on site > 2 minutes
- **Return Visitors**: Higher attention
- **Low Bounce Rate Sites**: > 60 seconds average

### 3. Creative Optimization

#### Size Selection
**Priority Order**:
1. 300×600 (Half Page)
2. 970×250 (Billboard)
3. 300×250 (Medium Rectangle)
4. 728×90 (Leaderboard)

#### Format Selection
**Viewability by Format**:
1. **Native Ads**: 70-75% (blend with content)
2. **Video (In-stream)**: 70-75% (user-initiated)
3. **Rich Media**: 65-70% (engaging)
4. **Standard Display**: 50-55% (baseline)

### 4. Technical Optimization

#### Lazy Loading Prevention
- Avoid publishers with aggressive lazy loading
- Verify ad loads before scroll
- Test viewability on sample pages

#### Ad Refresh Management
- Limit refresh to viewable impressions only
- Set minimum viewability threshold (50%+)
- Cap refresh frequency (max 3-4 times)

### 5. Supply Path Optimization (SPO)

#### Direct Publisher Relationships
- **Benefit**: Higher viewability (10-15% lift)
- **Reason**: Better placement control
- **Implementation**: PMP deals, direct buys

#### SSP Selection
**High-Viewability SSPs**:
- Google Ad Manager (65-70% avg)
- Magnite/Rubicon (60-65% avg)
- PubMatic (60-65% avg)
- OpenX (55-60% avg)

**Avoid**:
- Long-tail SSPs with < 50% viewability
- Resellers with poor transparency

## Measurement & Verification

### Viewability Vendors

#### Tier 1 (MRC-Accredited)
1. **IAS (Integral Ad Science)**
   - Comprehensive measurement
   - Real-time optimization
   - Fraud detection integrated

2. **DoubleVerify (DV)**
   - Industry-leading accuracy
   - Brand safety + viewability
   - Pre-bid filtering

3. **MOAT (Oracle)**
   - Attention metrics
   - Cross-platform measurement
   - Creative quality scoring

4. **Comscore**
   - Audience validation
   - Viewability + reach
   - Cross-media measurement

### Measurement Setup

#### Pre-Bid Optimization
```javascript
// DV360 Example
Viewability Targeting:
- Predicted Viewability: > 70%
- Active View: Enabled
- Viewable CPM Bidding: Enabled
```

#### Post-Bid Verification
```javascript
// Tag Implementation
<script src="https://vendor.com/viewability.js">
  config: {
    threshold: 0.5,  // 50% pixels
    duration: 1000,  // 1 second
    continuous: true
  }
</script>
```

### Discrepancy Management
**Acceptable Variance**: ± 10%
**Common Causes**:
- Measurement methodology differences
- Sampling vs. census measurement
- Bot/IVT filtering differences

## Viewability vs. Other Metrics

### Viewability vs. Attention
**Viewability**: Ad had opportunity to be seen
**Attention**: User actually engaged with ad

**Relationship**:
- High viewability ≠ High attention
- Attention requires viewability first
- Attention metrics: Time in view, interaction rate

### Viewability vs. Brand Lift
**Correlation**:
- 70%+ viewability → 15-20% higher brand lift
- 50-70% viewability → 10-15% higher brand lift
- < 50% viewability → Minimal brand impact

### Viewability vs. Conversion
**Impact**:
- Viewable impressions → 2-3x higher conversion rate
- Non-viewable impressions → Wasted spend
- Viewability optimization → 20-30% CPA improvement

## Advanced Strategies

### 1. Viewable CPM (vCPM) Bidding
**How It Works**:
- Pay only for viewable impressions
- Typically 30-50% higher CPM
- Better ROI for brand campaigns

**When to Use**:
- Brand awareness campaigns
- Video campaigns
- Upper-funnel objectives

### 2. Attention-Based Buying
**Next Evolution**:
- Measure actual user attention
- Time in view > 5 seconds
- Active engagement signals

**Platforms**:
- Adelaide (Attention Units)
- Lumen (Eye-tracking)
- Amplified Intelligence

### 3. Contextual + Viewability Combo
**Strategy**:
- Target high-engagement content
- Require 70%+ viewability
- Premium placements only

**Expected Results**:
- 80-85% viewability rates
- 25-30% higher brand lift
- 20-25% better conversion rates

## Common Pitfalls

### 1. Chasing 100% Viewability
**Problem**: Severely limits scale, inflates CPMs
**Solution**: Target 70-80% for optimal balance

### 2. Ignoring Context
**Problem**: High viewability on low-quality sites
**Solution**: Combine viewability with brand safety

### 3. Not Optimizing Creative
**Problem**: Viewable but not engaging
**Solution**: Test creative for attention, not just viewability

### 4. Measurement Discrepancies
**Problem**: Multiple vendors, conflicting data
**Solution**: Standardize on one primary vendor

### 5. Mobile Neglect
**Problem**: Lower mobile viewability accepted
**Solution**: Apply same standards, optimize for mobile

## Reporting & KPIs

### Essential Metrics
1. **Viewability Rate**: % of measured impressions that were viewable
2. **Measurable Rate**: % of impressions that could be measured
3. **Viewable Impressions**: Absolute count of viewable impressions
4. **vCPM**: Cost per 1,000 viewable impressions
5. **Viewability by Placement**: Identify top/bottom performers

### Reporting Frequency
- **Daily**: Monitor viewability trends
- **Weekly**: Optimize low-performing placements
- **Monthly**: Strategic viewability review

### Benchmarking
- Compare to industry standards
- Track month-over-month trends
- Analyze by campaign type

## Action Plan Template

### Week 1: Audit
- [ ] Implement viewability measurement
- [ ] Establish baseline metrics
- [ ] Identify low-viewability sources

### Week 2-3: Optimize
- [ ] Block below-threshold publishers (< 50%)
- [ ] Shift budget to high-viewability inventory
- [ ] Test viewability targeting in DSP

### Week 4+: Scale
- [ ] Expand high-viewability sources
- [ ] Negotiate viewability-guaranteed deals
- [ ] Implement continuous optimization

## Resources
- MRC Viewability Standards
- IAB Viewability Best Practices
- DV360 Viewability Optimization Guide
- IAS/DoubleVerify Measurement Guides
