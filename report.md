# Vessel Collision Detection — Report

## 1. Methodology
- Definition of collision (proximity threshold, time alignment rationale)
- Choice of 1-minute time buckets and ±1 slack justification

## 2. Data Quality & Noise Removal
- GPS jump filter: why 50 knots (fastest known ship ≈ 65 knots; 50 gives margin for noise)
- Stationary filter: nav status codes + SOG threshold
- Duplicate/null handling

## 3. Computational Strategy
- Why time-bucket join avoids O(n²) Cartesian product
- Haversine in native Spark SQL vs Python UDF
- Broadcast join for allowlists
- Partition and cache decisions

## 4. Results

[Fill in after running]
- MMSI A: ______  Name: ______
- MMSI B: ______  Name: ______
- Timestamp: ______
- Coordinates: ______°N, ______°E
- Separation: ______ nm (______ m)

## 5. Visualization

[Describe the map: two trajectories, collision marker, time window]

## 6. Limitations & Future Work
- AIS ping frequency varies — some collisions may be between pings
- No interpolation applied between pings (conservative approach)
