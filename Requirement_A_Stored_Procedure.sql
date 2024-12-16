DELIMITER $$

CREATE PROCEDURE GetActiveRates()
BEGIN
    DECLARE rows_affected INT DEFAULT 0;

    -- Define the trigger time dynamically for the current hour
    SET @trigger_time = UNIX_TIMESTAMP(DATE_FORMAT(NOW(), '%Y-%m-%d %H:30:00')) * 1000;

    -- Insert results into active_rates_log
    START TRANSACTION;
    BEGIN
        -- Insert Active Rates into active_rates_log
        INSERT INTO Currency_Exchange.active_rates_log (ccy_couple, current_rate, percentage_change)
        SELECT
            mr.ccy_couple,
            mr.current_rate,
            CONCAT(ROUND((mr.current_rate - yr.yesterday_rate) / yr.yesterday_rate * 100, 3), '%')
        FROM (
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
                    event_time >= @trigger_time - 30000 -- Last 30 seconds
            ),
            MostRecentRates AS (
                SELECT
                    ccy_couple,
                    current_rate,
                    event_time AS latest_time
                FROM ActiveRates
                WHERE rnk = 1
            ),
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
                            ORDER BY ABS(event_time - UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY) + INTERVAL 17 HOUR) * 1000) ASC
                        ) AS rnk_1
                    FROM
                        Currency_Exchange.forex_batch_processing_2_day
                    WHERE
                        event_time BETWEEN
                            UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY) + INTERVAL 17 HOUR - INTERVAL 1 HOUR) * 1000 AND
                            UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY) + INTERVAL 17 HOUR + INTERVAL 1 HOUR) * 1000
                ) AS subquery
                WHERE rnk_1 = 1
            )
            SELECT
                mr.ccy_couple,
                mr.current_rate,
                yr.yesterday_rate
            FROM
                MostRecentRates mr
            JOIN
                YesterdayRates yr
            ON
                mr.ccy_couple = yr.ccy_couple
        ) AS final_result;

        -- Get the number of rows affected
        SET rows_affected = ROW_COUNT();

        -- Log the execution status into procedure_execution_log
        INSERT INTO Currency_Exchange.procedure_execution_log (procedure_name, rows_affected, execution_status)
        VALUES ('GetActiveRates', rows_affected, 'SUCCESS');

        COMMIT;
    END;

    -- Error Handling
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO Currency_Exchange.procedure_execution_log (procedure_name, rows_affected, execution_status)
        VALUES ('GetActiveRates', 0, 'FAILED');
    END;
END$$

DELIMITER ;
