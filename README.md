# Harvey Analytics dbt Project

## 🎯 **Power User Analytics**

This dbt project identifies and analyzes **Power Users** - the most valuable and engaged users of Harvey's AI-powered legal assistant platform.

## 📋 **Data Assumptions**

### **Data Source Assumptions:**
- **users.csv**: **Daily updates** - user data can be refreshed daily with potential role changes, promotions, or new user additions
- **events.csv**: **Daily updates** - event logs arrive daily with real-time user activity, feedback scores, and document processing
- **firms.csv**: **Daily updates** - firm data can be refreshed daily with potential changes to firm size, ARR, mergers, acquisitions, or new firm additions

### **Data Quality Assumptions:**
- User roles may change over time (promotions, role changes, departures)
- Firm attributes (size, ARR, structure) may change daily due to business dynamics
- Events are timestamped accurately and arrive within 24 hours
- No duplicate events for the same user-firm-event combination
- All data sources maintain referential integrity

### **Data Deduplication Logic:**
- **Daily data files may contain duplicate records** for the same user or firm
- **Deduplication strategy**: Always take the latest data based on creation date
- **Primary method**: Use `created` date for deduplication since `updated_at` columns don't exist
- **Fallback method**: Use `ROW_NUMBER()` to ensure only one record per user/firm per day
- **SCD Type 2 tracking**: Maintains historical changes while preventing duplicate current records

### **Business Logic Assumptions:**
- Power users are defined by the scoring formula: `0.4 × Active Days + 0.3 × Document Volume + 0.3 × Feature Breadth`
- Firm health is assessed based on feedback scores and user engagement patterns
- User engagement levels are frequency-based classifications (High/Mid/Low Frequency User)
- Daily processing ensures real-time insights for business decision making

## 🏆 **Power User Definition**

**Power Users** are identified using a comprehensive scoring system that evaluates three key dimensions:

### **Power Score Formula:**
```
Power Score = 0.4 × Active Days Weight + 0.3 × Document Volume Weight + 0.3 × Feature Breadth Weight
```

### **Score Components:**

#### **1. Active Days Weight (40% of total score)**
- **100 points**: 20+ active days per month
- **80 points**: 15-19 active days per month  
- **60 points**: 10-14 active days per month
- **40 points**: 5-9 active days per month
- **20 points**: 2-4 active days per month
- **0 points**: 0-1 active days per month

#### **2. Document Volume Weight (30% of total score)**
- **100 points**: 100+ documents processed per month
- **80 points**: 50-99 documents processed per month
- **60 points**: 25-49 documents processed per month
- **40 points**: 10-24 documents processed per month
- **20 points**: 5-9 documents processed per month
- **0 points**: 0-4 documents processed per month

#### **3. Feature Breadth Weight (30% of total score)**
- **100 points**: Uses all 3 event types (ASSISTANT, VAULT, WORKFLOW)
- **70 points**: Uses 2 event types
- **40 points**: Uses 1 event type
- **0 points**: No event activity

### **Power User Tiers:**

| Score Range | Tier | Description |
|-------------|------|-------------|
| 80-100 | **Power User** | Elite users with maximum engagement |
| 60-79 | **High Value User** | Highly engaged users with strong activity |
| 40-59 | **Medium Value User** | Moderately engaged users |
| 20-39 | **Low Value User** | Light users with minimal engagement |
| 0-19 | **Inactive User** | Users with very low or no activity |

## 📊 **Key Metrics Available**

### **Power User Metrics:**
- `power_user_score` - Overall power score (0-100)
- `power_user_tier` - Classification tier
- `active_days_weight` - Active days component score
- `document_volume_weight` - Document volume component score  
- `feature_breadth_weight` - Feature usage breadth component score

### **Engagement Metrics:**
- `engagement_level` - Frequency-based classification (High/Mid/Low Frequency User)
- `total_queries` - Total queries submitted per month
- `active_days` - Unique days of activity per month
- `total_documents_processed` - Documents processed per month

### **User Profile:**
- `tenure_band` - User tenure classification (0-1 months, 1-3 months, etc.)
- `user_role` - Professional role (Partner, Senior Associate, etc.)
- `usage_pattern` - Preferred event type pattern

## 🏗️ **Data Architecture**

### **Dimensional Models:**
- **`dim_user`**: SCD Type 2 table tracking user role changes over time
- **`dim_firms`**: SCD Type 2 table tracking firm attribute changes daily

### **Fact Tables:**
- **`daily_user_engagement`**: Daily user engagement with power user scoring
- **`daily_firm_usage_summary`**: Daily firm metrics with point-in-time accuracy
- **`harvey_analytics_dashboard`**: Daily platform KPIs with power user analytics

### **Point-in-Time Accuracy:**
- All joins use effective date ranges for historical accuracy
- User attributes reflect their status at the time of events
- Firm attributes reflect their status at the time of events

### **Table Descriptions:**

| Model Name | Frequency | Brief Description |
|------------|-----------|-------------------|
| **`dim_user`** | Daily | SCD Type 2 table tracking user role changes over time with effective dates. Enables point-in-time accurate user attribute analysis for historical events. |
| **`dim_firms`** | Daily | SCD Type 2 table tracking firm attribute changes daily with effective dates. Handles daily firm data updates for accurate historical analysis. |
| **`daily_user_engagement`** | Daily | Daily user engagement metrics with power user scoring, engagement levels, and tenure bands. Provides daily granularity for current month data. |
| **`daily_firm_usage_summary`** | Daily | Daily aggregated firm usage metrics with point-in-time accurate firm attributes. Incremental processing for efficient daily updates. |
| **`harvey_analytics_dashboard`** | Daily | Daily platform KPIs combining user engagement, firm metrics, and cohort analysis. Provides executive-level insights for business decision making. |

## 🔍 **How to Interpret Models and Metrics**

### **Understanding Power User Scoring**

#### **Power User Score Interpretation:**
- **80-100**: Elite users driving platform value - focus on retention and expansion
- **60-79**: High-value users with growth potential - nurture to power user status
- **40-59**: Moderate users needing engagement - identify barriers and opportunities
- **20-39**: Light users at risk - implement re-engagement strategies
- **0-19**: Inactive users - prioritize reactivation campaigns

#### **Score Component Analysis:**
- **Active Days Weight (40%)**: Measures user consistency and habit formation
- **Document Volume Weight (30%)**: Indicates workload intensity and platform dependency
- **Feature Breadth Weight (30%)**: Shows platform adoption and feature exploration

### **Engagement Level Classification**

#### **High Frequency User (20+ queries, 15+ active days):**
- Platform power users with strong daily habits
- Likely to be early adopters and product advocates
- Focus on feature expansion and advanced workflows

#### **Mid Frequency User (10-19 queries, 8+ active days):**
- Regular users with consistent but moderate engagement
- Good candidates for power user conversion
- Target with advanced feature training and efficiency tools

#### **Low Frequency User (5-9 queries, 3+ active days):**
- Occasional users with basic platform adoption
- Risk of churn if not properly engaged
- Focus on use case expansion and value demonstration

#### **Occasional User (1-4 queries):**
- Light users with minimal platform engagement
- High churn risk
- Prioritize onboarding improvements and value communication

#### **Inactive User (0 queries):**
- Users with no recent activity
- Critical for reactivation campaigns
- Analyze reasons for disengagement

### **Tenure Band Analysis**

#### **0-1 months (New Users):**
- Focus on onboarding success and first value realization
- Monitor early engagement patterns and drop-off points
- Implement progressive feature introduction

#### **1-3 months (Early Adopters):**
- Critical period for habit formation and platform adoption
- Identify power user potential and nurture engagement
- Address any early friction points

#### **3-6 months (Growing Users):**
- Users establishing regular usage patterns
- Opportunity for feature expansion and workflow optimization
- Monitor engagement stability and growth

#### **6-12 months (Established Users):**
- Stable users with established workflows
- Focus on efficiency improvements and advanced features
- Identify expansion opportunities within their firm

#### **12+ months (Mature Users):**
- Long-term platform users with deep expertise
- Potential for advanced workflows and cross-selling
- Monitor for any engagement decline or satisfaction issues

### **Firm Health Assessment**

#### **Firm Health Status:**
- **Excellent**: High feedback scores with strong user engagement
- **Good**: Above-average performance with room for improvement
- **Fair**: Moderate performance requiring attention
- **Needs Attention**: Below-average performance requiring intervention
- **Critical**: Poor performance requiring immediate action

#### **Adoption Level Classification:**
- **High Adoption (80%+)**: Strong platform penetration within firm
- **Good Adoption (60-79%)**: Healthy adoption with growth potential
- **Moderate Adoption (40-59%)**: Moderate penetration requiring improvement
- **Low Adoption (20-39%)**: Low penetration needing intervention
- **Very Low Adoption (<20%)**: Critical adoption issues requiring immediate action

### **Key Performance Indicators (KPIs)**

#### **User Engagement KPIs:**
- **Power User Rate**: Percentage of users achieving power user status
- **Active Days per User**: Average engagement frequency
- **Queries per User**: Average workload intensity
- **Feature Adoption Rate**: Percentage of users using multiple features

#### **Firm Performance KPIs:**
- **Daily Active Firms**: Number of firms with activity each day
- **Average Firm Feedback**: Overall satisfaction across all firms
- **Adoption Rate**: Percentage of firm employees using the platform
- **Query Volume**: Total platform usage across all firms

#### **Platform Health KPIs:**
- **Event Distribution**: Balance between different event types
- **Satisfaction Rates**: Percentage of high vs. low feedback scores
- **Efficiency Metrics**: Documents per event, events per user
- **Usage Volume**: Overall platform activity levels

### **Trend Analysis and Insights**

#### **Growth Trends:**
- **Strong Growth**: Increasing power users with high retention
- **Growing**: Positive momentum in user engagement
- **Stable**: Consistent performance without significant changes
- **Declining**: Decreasing engagement requiring intervention
- **Critical Decline**: Significant performance deterioration

#### **Usage Volume Categories:**
- **High Volume (10,000+ events)**: Peak platform usage
- **Medium Volume (5,000-9,999 events)**: Normal business activity
- **Low Volume (1,000-4,999 events)**: Reduced activity requiring attention
- **Minimal Usage (<1,000 events)**: Critical low activity state

## 📈 **Use Cases**

### **1. Power User Identification**
- Identify top 10% most engaged users
- Track power user growth over time
- Analyze power user retention rates

### **2. User Segmentation**
- Segment users by power tier for targeted engagement
- Identify users at risk of dropping to lower tiers
- Find users with potential to become power users

### **3. Product Optimization**
- Understand what drives power user behavior
- Identify features most valued by power users
- Optimize onboarding for power user conversion

### **4. Business Intelligence**
- Power user contribution to platform metrics
- Power user lifetime value analysis
- Power user churn prediction and prevention

## 📊 **Data Sources**

- **users**: User metadata and roles (SCD Type 2, daily updates)
- **events**: User activity and query logs (daily updates)
- **firms**: Firm information and ARR data (SCD Type 2, daily updates)

## 🔄 **ETL Strategy**

- **Daily incremental processing** for all models to handle daily data updates
- **Daily partitioning** for efficient storage and querying
- **Real-time power user scoring** with daily updates
- **Historical data stability** for trend analysis
- **Point-in-time accuracy** using SCD Type 2 dimensions

## 🧹 **Data Deduplication Implementation**

### **Why Deduplication is Needed:**
Daily data files may contain multiple records for the same user or firm due to:
- Multiple data source updates throughout the day
- System synchronization processes
- Data pipeline retries or reprocessing
- Manual data corrections or updates

### **Implementation in Dimensional Models:**

#### **`dim_user` Deduplication:**
1. **Deduplicate raw data**: Filter to latest record per user per day
2. **Track changes**: Use deduplicated data for SCD Type 2 tracking
3. **Maintain history**: Preserve all attribute changes over time
4. **Prevent duplicates**: Ensure no duplicate current records

#### **`dim_firms` Deduplication:**
1. **Deduplicate raw data**: Filter to latest record per firm per day
2. **Track changes**: Use deduplicated data for SCD Type 2 tracking
3. **Maintain history**: Preserve all attribute changes over time
4. **Prevent duplicates**: Ensure no duplicate current records

### **Understanding dbt Variables: `{{ this }}` vs `{{ ds }}`**

#### **`{{ this }}` Variable:**
- **Purpose**: References the current model being built
- **Usage**: Used in incremental logic to check against the model's existing data
- **Example**: `(select max(date) from {{ this }})` gets the maximum date from the current model
- **When to use**: For incremental processing logic within a model

#### **`{{ ds }}` Variable:**
- **Purpose**: Airflow execution date variable
- **Usage**: Used for date filtering and partitioning based on when the job runs
- **Example**: `{{ get_month_start('{{ ds }}') }}` gets the month start for the execution date
- **When to use**: For date-based filtering and partitioning logic


### **Benefits of This Approach:**
- **Data Quality**: Eliminates duplicate records that could skew analytics
- **Performance**: Reduces storage and processing overhead
- **Accuracy**: Ensures SCD Type 2 tables contain only valid historical changes
- **Reliability**: Handles various data update scenarios gracefully
- **Maintainability**: Clear logic that's easy to understand and modify

## ⚠️ **Data Quality Concerns & Potential Issues**

### **What Potential Issues or Data Quality Concerns Does the Data Surface?**

#### **1. Data Update Frequency Limitations**
- **Issue**: Users and firms data can change multiple times in a day, but we only take the latest record from the source file
- **Impact**: We may miss intermediate changes that occur between daily file updates
- **Mitigation**: 
  - Implemented deduplication to ensure we always get the most recent data
  - SCD Type 2 tracking preserves historical changes when they do occur
  - Daily processing minimizes the window of missed updates

#### **2. Invalid Feedback Score Ranges**
- **Issue**: Events data may contain feedback scores > 5 or < 1, which are outside valid range
- **Impact**: Invalid scores could skew analytics and cause calculation errors
- **Mitigation**:
  - **DBT checks**: Implemented data validation that sets invalid scores to NULL instead of failing
  - **Code implementation**: All models now handle invalid feedback scores gracefully
  - **Analytics impact**: NULL scores are excluded from averages and calculations

#### **3. Missing or NULL User/Firm Identifiers**
- **Issue**: Events data may contain NULL user_id or firm_id values
- **Impact**: These records cannot be properly attributed and would cause join failures
- **Mitigation**:
  - **Filtering**: All models now filter out records with NULL user_id or firm_id
  - **Data integrity**: Ensures referential integrity between fact and dimension tables
  - **Analytics accuracy**: Prevents orphaned events from skewing metrics

#### **4. Future Date Records**
- **Issue**: Source data may contain records with created dates > current execution date
- **Impact**: Future dates could cause incorrect temporal analysis and data freshness issues
- **Mitigation**:
  - **Date validation**: All models now filter out records with future dates
  - **Execution date check**: Uses `current_date` to ensure data timeliness
  - **Data quality**: Prevents future-dated records from corrupting historical analysis

### **Data Quality Implementation Details:**

#### **Feedback Score Validation:**
```sql
-- Handle invalid feedback scores by setting them to NULL
case 
  when e.feedback_score >= 1 and e.feedback_score <= 5 then e.feedback_score
  else null
end as feedback_score
```

#### **NULL ID Filtering:**
```sql
-- Ignore records with NULL user_id or firm_id
where e.user_id is not null and e.firm_id is not null
```

#### **Future Date Filtering:**
```sql
-- Ignore records with future dates (created > execution date)
and e.created <= current_date
```

#### **Deduplication Logic:**
```sql
-- Ensure only the latest record per user/firm per day
row_number() over (
  partition by id 
  order by created desc
) as dedup_rank
where dedup_rank = 1
```

### **Monitoring & Alerting Recommendations:**

#### **Data Quality Metrics to Track:**
- **Percentage of records with NULL user_id/firm_id**
- **Percentage of records with invalid feedback scores**
- **Percentage of records with future dates**
- **Number of duplicate records per day**
- **Data freshness (lag between event creation and processing)**

#### **Alert Thresholds:**
- **Critical**: >5% NULL identifiers, >10% invalid feedback scores
- **Warning**: >2% NULL identifiers, >5% invalid feedback scores
- **Info**: Any records with future dates, duplicate detection

#### **Recovery Procedures:**
- **Data validation failures**: Investigate source system issues
- **High NULL rates**: Check data pipeline integrity
- **Future dates**: Verify system clock synchronization
- **Duplicates**: Review deduplication logic and source data quality

---

**Harvey Analytics Team** | Building data-driven insights for legal AI excellence

