# Programmatic Brand Safety Guidelines

## Overview
Brand safety ensures your ads don't appear alongside inappropriate, harmful, or brand-damaging content. It's critical for protecting brand reputation and ensuring advertising effectiveness.

## Brand Safety Framework

### The 4 Pillars of Brand Safety

#### 1. Content Verification
**What**: Ensure ads appear on appropriate content
**How**: Pre-bid filtering, post-bid verification
**Tools**: IAS, DoubleVerify, MOAT

#### 2. Fraud Prevention
**What**: Block invalid traffic and bot activity
**How**: IVT detection, traffic quality scoring
**Impact**: 2-10% of programmatic spend at risk

#### 3. Viewability Assurance
**What**: Ads must be viewable to be brand-safe
**How**: MRC-compliant measurement
**Standard**: 70%+ viewability target

#### 4. Transparency & Control
**What**: Know where ads appear, control placement
**How**: Ads.txt, sellers.json, supply path optimization
**Benefit**: Reduce unauthorized reselling

## Content Categories to Avoid

### GARM Brand Safety Framework
**Global Alliance for Responsible Media Standards**

#### High-Risk Categories (Block Always)
1. **Adult Content**
   - Pornography
   - Sexual services
   - Adult dating

2. **Arms & Ammunition**
   - Weapons sales
   - Ammunition
   - Military equipment

3. **Crime & Harmful Acts**
   - Criminal activity
   - Terrorism
   - Violence

4. **Death, Injury, or Military Conflict**
   - Graphic violence
   - War coverage (context-dependent)
   - Accidents/disasters

5. **Hate Speech & Acts of Aggression**
   - Discrimination
   - Extremism
   - Harassment

6. **Illegal Drugs/Tobacco/E-Cigarettes**
   - Drug trafficking
   - Tobacco promotion
   - Vaping content

7. **Online Piracy**
   - Copyright infringement
   - Illegal streaming
   - Counterfeit goods

8. **Spam or Harmful Content**
   - Malware
   - Phishing
   - Deceptive practices

9. **Debated Sensitive Social Issues**
   - Abortion (brand-dependent)
   - Immigration (context matters)
   - LGBTQ+ issues (brand-dependent)

#### Medium-Risk Categories (Context-Dependent)
1. **News & Politics**
   - Partisan content
   - Controversial topics
   - Election coverage

2. **Religion**
   - Religious content
   - Spiritual practices
   - Faith-based organizations

3. **Tragedy & Conflict**
   - Natural disasters
   - Breaking news
   - Crisis situations

## Brand Safety Tiers

### Tier 1: Maximum Safety (Conservative)
**Use Case**: Luxury brands, financial services, healthcare
**Approach**:
- Block all high-risk categories
- Block most medium-risk categories
- Whitelist-only approach
- Premium publishers only

**Expected Reach**: 40-50% of available inventory
**Brand Safety Score**: 95-98%

### Tier 2: Balanced Safety (Moderate)
**Use Case**: Most consumer brands, B2B, retail
**Approach**:
- Block all high-risk categories
- Contextual filtering for medium-risk
- Mix of whitelist + filtered open exchange
- Quality publisher focus

**Expected Reach**: 60-70% of available inventory
**Brand Safety Score**: 90-95%

### Tier 3: Contextual Safety (Aggressive)
**Use Case**: News-related products, advocacy, entertainment
**Approach**:
- Block only extreme high-risk
- Allow news and political content
- Broader publisher base
- Contextual relevance prioritized

**Expected Reach**: 80-90% of available inventory
**Brand Safety Score**: 85-90%

## Implementation Strategies

### 1. Pre-Bid Filtering

#### Keyword Blocking
```javascript
// Example blocklist
blocked_keywords = [
  "violence", "terrorism", "hate", "porn",
  "drugs", "weapons", "crime", "death",
  "scandal", "controversy", "explicit"
]

// Contextual exceptions
allowed_contexts = [
  "news/violence" (if tier 3),
  "health/drugs" (pharmaceutical ads),
  "sports/weapons" (sporting goods)
]
```

#### URL/Domain Blocking
**Blocklist Types**:
1. **Global Blocklist**: Industry-standard bad actors
2. **Category Blocklist**: Content categories to avoid
3. **Custom Blocklist**: Brand-specific exclusions

**Maintenance**:
- Update weekly minimum
- Review monthly for false positives
- Share across campaigns

#### Publisher Whitelisting
**Criteria for Inclusion**:
- Brand safety score > 95%
- Viewability > 70%
- IVT rate < 2%
- Direct relationship or verified PMP

**Example Whitelist**:
```
Tier 1 Publishers:
- WSJ.com
- NYTimes.com
- BBC.com
- CNN.com (news tier 2+)
- ESPN.com
- Weather.com
```

### 2. Post-Bid Verification

#### Real-Time Blocking
**How It Works**:
1. Ad call initiated
2. Verification tag fires
3. Content analyzed in <100ms
4. Ad blocked if unsafe
5. Impression not counted

**Vendors**:
- **IAS**: 98% block accuracy
- **DoubleVerify**: 97% block accuracy
- **MOAT**: 96% block accuracy

#### Reporting & Optimization
**Daily Monitoring**:
- Brand safety violations
- Blocked impression volume
- False positive rate
- Cost of blocking

**Weekly Actions**:
- Update blocklists
- Review new violations
- Optimize filtering rules

### 3. Supply Chain Transparency

#### Ads.txt Verification
**What**: Authorized digital sellers list
**Why**: Prevents unauthorized reselling
**How**: Only buy from ads.txt authorized sellers

**Implementation**:
```
# Check ads.txt compliance
IF seller NOT IN publisher_ads_txt:
  BLOCK impression
```

#### Sellers.json Transparency
**What**: Seller identity disclosure
**Why**: Know who you're buying from
**How**: Verify seller legitimacy

**Red Flags**:
- Missing sellers.json
- Confidential seller IDs
- Multiple intermediaries

#### Supply Path Optimization (SPO)
**Goal**: Shortest path to publisher
**Benefits**:
- Lower fees (15-30% savings)
- Better brand safety
- Improved transparency

**Strategy**:
```
Priority 1: Direct publisher deals
Priority 2: Single SSP path
Priority 3: Verified resellers only
Block: Long-tail, unverified paths
```

## Platform-Specific Guidelines

### DV360 (Google)

#### Built-In Controls
1. **Digital Content Labels**
   - DL-G: General audiences
   - DL-PG: Parental guidance
   - DL-T: Teens and older
   - DL-MA: Mature audiences

2. **Sensitive Categories**
   - Tragedy & conflict
   - Sensitive social issues
   - Sensational & shocking

3. **Inventory Types**
   - Authorized inventory only
   - Open Auction (filtered)
   - Private Marketplace (preferred)

**Recommended Settings**:
```
Content Labels: DL-G, DL-PG only
Sensitive Categories: Block all
Inventory: Authorized + PMP
Brand Safety: Maximum protection
```

### The Trade Desk

#### Brand Safety Tools
1. **OpenSlate Integration**
   - Content-level analysis
   - Custom brand safety scores
   - Real-time filtering

2. **IAS/DV Integration**
   - Pre-bid segments
   - Post-bid verification
   - Comprehensive reporting

**Recommended Setup**:
```
OpenSlate: Tier 1 (95+ score)
IAS/DV: Pre-bid filtering enabled
Blocklists: Global + custom
Whitelist: Premium publishers
```

### Amazon DSP

#### Brand Safety Features
1. **Amazon Publisher Services**
   - Vetted publisher network
   - High brand safety standards
   - Premium inventory

2. **Third-Party Verification**
   - IAS integration
   - DoubleVerify support
   - Custom blocking

**Best Practices**:
- Prioritize Amazon O&O inventory
- Use verified third-party sites
- Enable all safety controls

## Crisis Management

### Real-Time Monitoring

#### Alert Setup
**Immediate Alerts** (within 1 hour):
- Brand safety violations > 1%
- Placement on blocked sites
- Negative brand mentions

**Daily Alerts**:
- Brand safety score trends
- New violation patterns
- Blocklist effectiveness

#### Response Protocol
```
SEVERITY 1 (Critical):
- Pause all campaigns immediately
- Audit placements
- Update blocklists
- Notify stakeholders
- Resume with tighter controls

SEVERITY 2 (High):
- Pause affected line items
- Review and block violating publishers
- Update filtering rules
- Resume within 24 hours

SEVERITY 3 (Medium):
- Add to blocklist
- Monitor for recurrence
- No campaign pause needed
```

### Breaking News Events

#### Sensitivity Periods
**High-Risk Events**:
- Terrorist attacks
- Mass shootings
- Natural disasters
- Political crises
- Celebrity deaths

**Action Plan**:
1. **Immediate** (0-2 hours):
   - Pause news inventory (if tier 1)
   - Enable keyword blocking for event
   - Monitor social sentiment

2. **Short-term** (2-24 hours):
   - Review placement reports
   - Adjust contextual targeting
   - Communicate with stakeholders

3. **Recovery** (24-72 hours):
   - Gradually resume news inventory
   - Monitor brand safety scores
   - Document learnings

## Measurement & Reporting

### Key Metrics

#### Brand Safety Score
```
Brand Safety Score = (Safe Impressions / Total Impressions) × 100

Target: 95%+ (Tier 1), 90%+ (Tier 2), 85%+ (Tier 3)
```

#### Violation Rate
```
Violation Rate = (Unsafe Impressions / Total Impressions) × 100

Acceptable: < 1% (Tier 1), < 2% (Tier 2), < 5% (Tier 3)
```

#### Block Rate
```
Block Rate = (Blocked Impressions / Total Opportunities) × 100

Typical: 20-40% depending on tier
```

### Reporting Dashboard

**Daily Metrics**:
- Brand safety score
- Violation count and examples
- Blocked impression volume
- Top violating publishers

**Weekly Analysis**:
- Trend analysis
- Category breakdown
- Publisher performance
- Cost impact of blocking

**Monthly Review**:
- Overall brand safety health
- Blocklist effectiveness
- Whitelist performance
- Vendor comparison

## Cost Considerations

### Brand Safety Investment

#### Verification Costs
- **IAS/DoubleVerify**: $0.10-$0.25 CPM
- **MOAT**: $0.08-$0.20 CPM
- **OpenSlate**: $0.05-$0.15 CPM

**ROI Calculation**:
```
Prevented Brand Damage Value = 
  (Blocked Unsafe Impressions × Avg CPM × Risk Multiplier)

Risk Multiplier:
- High-risk content: 100x (major brand damage)
- Medium-risk: 10x (moderate damage)
- Low-risk: 2x (minor damage)
```

#### Reach vs. Safety Trade-off
**Conservative Approach** (Tier 1):
- 30-40% reach reduction
- 20-30% CPM increase
- 95%+ brand safety

**Balanced Approach** (Tier 2):
- 15-20% reach reduction
- 10-15% CPM increase
- 90-95% brand safety

## Best Practices Checklist

### Setup Phase
- [ ] Define brand safety tier (1, 2, or 3)
- [ ] Implement verification vendor (IAS/DV/MOAT)
- [ ] Create comprehensive blocklist
- [ ] Build publisher whitelist
- [ ] Enable pre-bid filtering
- [ ] Set up post-bid verification
- [ ] Configure alerts and monitoring

### Ongoing Management
- [ ] Review placement reports weekly
- [ ] Update blocklists monthly
- [ ] Audit whitelist quarterly
- [ ] Test new safety technologies
- [ ] Monitor industry news for risks
- [ ] Document violations and responses
- [ ] Share learnings across teams

### Crisis Preparedness
- [ ] Document response protocols
- [ ] Establish escalation paths
- [ ] Create communication templates
- [ ] Maintain emergency contact list
- [ ] Conduct quarterly drills
- [ ] Review and update playbooks

## Resources
- GARM Brand Safety Framework
- IAB Brand Safety Guidelines
- IAS/DoubleVerify Best Practices
- Ads.txt Specification
- Sellers.json Documentation
