# Nov 2, 2025 DST Missing Data Issue - Debugging Guide

## Issue Summary
On November 2, 2025 at 01:00, **16 columns show 'I' (Interpolated)** values in the EVTEST data, meaning actual interval data was MISSING for that hour and had to be interpolated.

## Why Nov 2 is Special - DST Transition

```
Nov 2, 2025 at 2:00 AM Eastern Time:
  - Clocks "fall back" to 1:00 AM
  - The hour 01:00-02:00 occurs TWICE:
    
    First 01:00 = still in EDT (UTC-4)  → Unix timestamp X
    Second 01:00 = now in EST (UTC-5)   → Unix timestamp X + 3600
    
    These are DIFFERENT moments in time but have the SAME local time display!
```

## Suspected Code Locations

### 1. gulf_daily.json - Data Fetch Query (MOST LIKELY)

```sql
-- Current code uses:
read_strt_time >= '{0}'::timestamp with time zone
read_strt_time <= '{1}'::timestamp with time zone + 1

-- PROBLEM: If dates passed without timezone, one of the duplicate 
-- 01:00 hours may be missed or the query boundaries may be wrong
```

### 2. interpolation.py - Time Grid Generation

```python
# Line 49 - Creates time grid by exploding timestamps
_interval = _interval.withColumn("readtime", 
    F.explode(date_range("readtime_min", "readtime_max", "interval_spi")))

# PROBLEM: date_range function may not account for DST
# It generates timestamps at fixed intervals, but on Nov 2:
# - Normal day: 24 hours × 4 intervals = 96 intervals
# - Nov 2: 25 hours × 4 intervals = 100 intervals (due to extra hour)
```

### 3. interpolation.py - Timestamp Casting

```python
# Line 46 - Cast to Unix timestamp (integer)
df = df.withColumn("readtime", F.col(read_time).cast('integer'))

# Line 59 - Cast back to timestamp
df_all_dates = df_all_dates.withColumn('read_time', 
    F.col('readtime').cast('timestamp'))

# PROBLEM: Unix timestamps are timezone-agnostic, but when cast back
# to timestamp, Spark uses the session timezone. This can cause:
# - Duplicate local times to be treated as same time
# - Missing intervals when joining actual data to grid
```

---

## Debugging Queries

### Query 1: Check SOURCE Data (Greenplum)

```sql
-- Check how many hourly records exist on Nov 2 in source
SELECT 
    date_trunc('hour', read_strt_time) as hour_local,
    read_strt_time AT TIME ZONE 'UTC' as utc_time,
    count(*) as record_count
FROM utl.v_meter_duration_read_fact_nw_2yc
WHERE read_strt_time >= '2025-11-02 00:00:00'::timestamp
  AND read_strt_time < '2025-11-03 00:00:00'::timestamp
  AND mtr_id IN (SELECT meterid FROM your_ev_meters)  -- filter to EV meters
GROUP BY 1, 2
ORDER BY 2;

-- EXPECTED: 25 distinct hours (including both 01:00 hours)
-- IF 24: Problem is in source data
-- IF 25: Problem is in ALR processing
```

### Query 2: Check ALR fetch_results

```sql
-- Check interpolation percentage on Nov 2 vs other days
SELECT 
    greg_date,
    studyid,
    premiseid,
    channel,
    kwh,
    goodintvper,
    badintvper,
    interpolper
FROM clr.fetch_results
WHERE greg_date BETWEEN '2025-11-01' AND '2025-11-03'
  AND studyid LIKE '%EV%'
  AND channel = 1000
ORDER BY premiseid, greg_date;

-- Look for: High interpolper on Nov 2 compared to Nov 1 and Nov 3
```

### Query 3: Check LSE Daily File Record Counts

```sql
-- If you have access to LSE data, check record counts
SELECT 
    date_trunc('day', read_strt_time) as day,
    premiseid,
    count(*) as interval_count,
    count(distinct date_trunc('hour', read_strt_time)) as hour_count
FROM lse_daily
WHERE read_strt_time >= '2025-11-01'
  AND read_strt_time < '2025-11-04'
GROUP BY 1, 2
ORDER BY 2, 1;

-- Nov 1 and Nov 3: Should have 24 hours, 96 intervals (15-min)
-- Nov 2: Should have 25 hours, 100 intervals
-- If Nov 2 shows 24/96: DST hour is missing
```

---

## Quick Test: Check Your Excel File

From the EVTEST spreadsheet you already have:

```python
import pandas as pd

df = pd.read_excel('EVTEST-2025-ALL_YR-IESSUES.xlsx', sheet_name=0)

# Count records per day
df['date'] = df['TIME'].dt.date
daily_counts = df.groupby('date').size()

# Check Nov 1, 2, 3
print("Records per day:")
print(daily_counts['2025-11-01'])  # Should be 24
print(daily_counts['2025-11-02'])  # Should be 25 (but probably shows 24)
print(daily_counts['2025-11-03'])  # Should be 24
```

---

## What Output Do You Need to Provide?

Please run these checks and share:

1. **Source data count** for Nov 2 - does Greenplum have 25 hours of data?
2. **ALR fetch_results** for Nov 2 - what is the interpolper value?
3. **LSE file record count** for Nov 2 - is it 24 or 25 hours?

This will tell us WHERE the data is being lost:
- If source has 25 but ALR has 24 → Problem in gulf_daily.json fetch
- If source has 24 → Problem is upstream of ALR
- If source and ALR both have 25 but LSE has 24 → Problem in LSE generation

---

## Potential Fix (Once Root Cause Confirmed)

### Option A: Fix in gulf_daily.json

```sql
-- Use explicit UTC conversion to avoid DST ambiguity
read_strt_time >= '{0}'::timestamp AT TIME ZONE 'UTC'
read_strt_time < '{1}'::timestamp AT TIME ZONE 'UTC' + interval '1 day'
```

### Option B: Fix in interpolation.py

```python
# Use timezone-aware timestamps throughout
from pyspark.sql.functions import from_utc_timestamp, to_utc_timestamp

# Convert to UTC before processing
df = df.withColumn("readtime_utc", to_utc_timestamp(col(read_time), "America/New_York"))

# Process in UTC...

# Convert back to local time for output
df = df.withColumn("read_time_local", from_utc_timestamp(col("readtime_utc"), "America/New_York"))
```

### Option C: Handle DST in time grid generation

```python
# When generating time grid for Nov 2, explicitly add the extra hour
# Check if date is DST transition and adjust interval count accordingly
```
