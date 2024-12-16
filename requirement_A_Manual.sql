
-- Define the trigger time (10:30 AM UTC) as a fixed reference
SET @trigger_time = UNIX_TIMESTAMP('2024-12-15 11:30:00') * 1000;

-- Step 1: Identify the latest active rates
WITH ActiveRates AS (
    SELECT
        ccy_couple,
        rate AS current_rate,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY ccy_couple ORDER BY event_time DESC) AS rnk
    FROM
        Currency_Exchange.forex_batch_processing_2_day
    WHERE
        event_time >= @trigger_time - 30000 -- Only include rates from the last 30 seconds before the trigger time
),

MostRecentRates AS (
    SELECT
        ccy_couple,
        current_rate,
        event_time AS latest_time
    FROM
        ActiveRates
    WHERE
        rnk = 1 -- Get the most recent active rate for each currency pair
),

-- Step 2: Identify the rate at (or close to) yesterday's 5 PM New York time
YesterdayRates AS (
    SELECT
        ccy_couple,
        rate AS yesterday_rate,
        event_time
    FROM (
        SELECT
            ccy_couple,
            rate,
            event_time,
            ROW_NUMBER() OVER (
                PARTITION BY ccy_couple
                ORDER BY ABS(event_time - UNIX_TIMESTAMP('2024-12-14 22:00:00') * 1000) ASC
            ) AS rnk_1
        FROM
            Currency_Exchange.forex_batch_processing_2_day
        WHERE
            event_time BETWEEN
                UNIX_TIMESTAMP('2024-12-14 21:00:00') * 1000 AND
                UNIX_TIMESTAMP('2024-12-14 23:00:00') * 1000
    ) AS subquery
    WHERE rnk_1 = 1 -- Closest rate to 5 PM New York time
)

-- Step 3: Calculate percentage change and display output
SELECT
    mr.ccy_couple,
    mr.current_rate,
    CONCAT(ROUND((mr.current_rate - yr.yesterday_rate) / yr.yesterday_rate * 100, 3), '%') AS percentage_change
FROM
    MostRecentRates mr
JOIN
    YesterdayRates yr
ON
    mr.ccy_couple = yr.ccy_couple;
