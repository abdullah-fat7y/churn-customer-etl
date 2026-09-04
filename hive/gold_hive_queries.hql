/*Dim Customer*/
CREATE EXTERNAL TABLE IF NOT EXISTS dim_customer_gold (
    cust_key INT,
    customer_id BIGINT,
    is_churned INT,
    CLV_LTV DOUBLE,
    avg_ticket_res_time_hrs DOUBLE,
    num_products INT,
    dw_start_date DATE,
    dw_end_date DATE
)
STORED AS PARQUET
LOCATION 'hdfs:///project/gold/dim_customer';

SELECT * 
FROM dim_customer_gold
LIMIT 10;

/*Fact Customer Activity*/
CREATE EXTERNAL TABLE IF NOT EXISTS fact_customer_activity_gold (
    customer_id BIGINT,
    activity_month STRING,
    usage_events BIGINT,
    total_balance DOUBLE,
    avg_balance DOUBLE,
    ticket_count BIGINT,
    avg_resolution_time DOUBLE,
    offer_count BIGINT,
    accepted_offers BIGINT,
    acceptance_rate DOUBLE,
    cust_key INT,
    activity_key INT
)
STORED AS PARQUET
LOCATION 'hdfs:///project/gold/fact_customer_activity';

SELECT *
FROM fact_customer_activity_gold
LIMIT 10;

/*Monthly Churn*/
CREATE EXTERNAL TABLE IF NOT EXISTS monthly_churn_rate_gold (
    activity_month STRING,
    active_customers BIGINT,
    churned_customers BIGINT,
    churn_rate DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:///project/gold/monthly_churn_rate';

SELECT *
FROM monthly_churn_rate_gold
ORDER BY activity_month;

-- ============================================================
-- GOLD LAYER - BUSINESS QUESTIONS & ANALYTICS QUERIES
-- ============================================================

-- ============================================================
-- 1. WHO ARE OUR HIGHEST-VALUE CUSTOMERS?
-- ============================================================

SELECT
    customer_id,
    CLV_LTV,
    num_products,
    is_churned
FROM dim_customer_gold
ORDER BY CLV_LTV DESC
LIMIT 10;


-- ============================================================
-- 2. ARE HIGH-VALUE CUSTOMERS AT RISK OF CHURN?
-- ============================================================

SELECT
    CASE
        WHEN CLV_LTV >= 10000 THEN 'High Value'
        WHEN CLV_LTV >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,

    COUNT(*) AS customers,

    SUM(is_churned) AS churned_customers,

    ROUND(AVG(is_churned) * 100, 2) AS churn_rate

FROM dim_customer_gold

GROUP BY
    CASE
        WHEN CLV_LTV >= 10000 THEN 'High Value'
        WHEN CLV_LTV >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END

ORDER BY churn_rate DESC;


-- ============================================================
-- 3. WHICH MONTHS HAVE THE HIGHEST CHURN?
-- ============================================================

SELECT
    activity_month,
    active_customers,
    churned_customers,
    ROUND(churn_rate * 100, 2) AS churn_percentage

FROM monthly_churn_rate_gold

ORDER BY churn_rate DESC

LIMIT 5;


-- ============================================================
-- 4. HOW IS CUSTOMER CHURN TRENDING OVER TIME?
-- ============================================================

SELECT
    activity_month,
    active_customers,
    churned_customers,
    ROUND(churn_rate * 100, 2) AS churn_percentage

FROM monthly_churn_rate_gold

ORDER BY activity_month;


-- ============================================================
-- 5. WHICH CUSTOMERS GENERATE THE MOST SUPPORT TICKETS?
-- ============================================================

SELECT
    f.customer_id,
    SUM(f.ticket_count) AS total_tickets,
    ROUND(AVG(f.avg_resolution_time), 2) AS avg_resolution_time,
    d.CLV_LTV,
    d.is_churned

FROM fact_customer_activity_gold f

JOIN dim_customer_gold d
    ON f.cust_key = d.cust_key

GROUP BY
    f.customer_id,
    d.CLV_LTV,
    d.is_churned

ORDER BY total_tickets DESC

LIMIT 10;


-- ============================================================
-- 6. DOES SUPPORT RESOLUTION TIME RELATE TO CHURN?
-- ============================================================

SELECT
    CASE
        WHEN avg_resolution_time < 24
            THEN 'Less than 24 hrs'

        WHEN avg_resolution_time < 48
            THEN '24-48 hrs'

        ELSE '48+ hrs'
    END AS resolution_group,

    COUNT(DISTINCT d.customer_id) AS customers,

    COUNT(DISTINCT
        CASE
            WHEN d.is_churned = 1
            THEN d.customer_id
        END
    ) AS churned_customers,

    ROUND(
        COUNT(DISTINCT
            CASE
                WHEN d.is_churned = 1
                THEN d.customer_id
            END
        ) * 100.0
        / COUNT(DISTINCT d.customer_id),
        2
    ) AS churn_rate

FROM fact_customer_activity_gold f

JOIN dim_customer_gold d
    ON f.cust_key = d.cust_key

GROUP BY
    CASE
        WHEN avg_resolution_time < 24
            THEN 'Less than 24 hrs'

        WHEN avg_resolution_time < 48
            THEN '24-48 hrs'

        ELSE '48+ hrs'
    END

ORDER BY churn_rate DESC;


-- ============================================================
-- 7. HOW EFFECTIVE ARE OUR OFFERS?
-- ============================================================

SELECT
    SUM(offer_count) AS total_offers,

    SUM(accepted_offers) AS accepted_offers,

    ROUND(
        SUM(accepted_offers) * 100.0
        / NULLIF(SUM(offer_count), 0),
        2
    ) AS acceptance_rate

FROM fact_customer_activity_gold;


-- ============================================================
-- 8. DOES THE NUMBER OF PRODUCTS AFFECT CUSTOMER VALUE?
-- ============================================================

SELECT
    num_products,

    COUNT(*) AS customers,

    ROUND(AVG(CLV_LTV), 2) AS avg_clv,

    ROUND(
        AVG(is_churned) * 100,
        2
    ) AS churn_rate

FROM dim_customer_gold

GROUP BY num_products

ORDER BY num_products;

-- ============================================================
-- 9. DOES CUSTOMER BALANCE RELATE TO CHURN?
-- ============================================================

WITH customer_balance AS (
    SELECT
        customer_id,
        SUM(total_balance) AS total_balance
    FROM fact_customer_activity_gold
    GROUP BY customer_id
)

SELECT
    CASE
        WHEN total_balance >= 10000 THEN 'High Balance'
        WHEN total_balance >= 1000 THEN 'Medium Balance'
        ELSE 'Low Balance'
    END AS balance_segment,

    COUNT(*) AS customers,

    SUM(d.is_churned) AS churned_customers,

    ROUND(
        AVG(d.is_churned) * 100,
        2
    ) AS churn_rate

FROM customer_balance cb

JOIN dim_customer_gold d
    ON cb.customer_id = d.customer_id

GROUP BY
    CASE
        WHEN total_balance >= 10000 THEN 'High Balance'
        WHEN total_balance >= 1000 THEN 'Medium Balance'
        ELSE 'Low Balance'
    END

ORDER BY churn_rate DESC;

-- ============================================================
-- 10. DOES CUSTOMER ENGAGEMENT RELATE TO CHURN?
-- ============================================================

WITH customer_engagement AS (
    SELECT
        customer_id,
        SUM(usage_events) AS total_usage
    FROM fact_customer_activity_gold
    GROUP BY customer_id
)

SELECT
    CASE
        WHEN total_usage >= 20 THEN 'Highly Engaged'
        WHEN total_usage >= 5 THEN 'Moderately Engaged'
        ELSE 'Low Engagement'
    END AS engagement_segment,

    COUNT(*) AS customers,

    SUM(d.is_churned) AS churned_customers,

    ROUND(
        AVG(d.is_churned) * 100,
        2
    ) AS churn_rate

FROM customer_engagement ce

JOIN dim_customer_gold d
    ON ce.customer_id = d.customer_id

GROUP BY
    CASE
        WHEN total_usage >= 20 THEN 'Highly Engaged'
        WHEN total_usage >= 5 THEN 'Moderately Engaged'
        ELSE 'Low Engagement'
    END

ORDER BY churn_rate DESC;

-- ============================================================
-- 11. DOES HAVING MORE PRODUCTS REDUCE CHURN?
-- ============================================================

SELECT
    num_products,

    COUNT(*) AS customers,

    SUM(is_churned) AS churned_customers,

    ROUND(
        AVG(is_churned) * 100,
        2
    ) AS churn_rate,

    ROUND(
        AVG(CLV_LTV),
        2
    ) AS average_clv

FROM dim_customer_gold

GROUP BY num_products

ORDER BY num_products;

-- ============================================================
-- 12. WHICH CUSTOMERS SHOULD BE PRIORITIZED FOR RETENTION?
-- ============================================================

WITH customer_engagement AS (
    SELECT
        customer_id,
        SUM(usage_events) AS total_usage,
        SUM(ticket_count) AS total_tickets
    FROM fact_customer_activity_gold
    GROUP BY customer_id
)

SELECT
    d.customer_id,
    d.CLV_LTV,
    d.num_products,
    ce.total_usage,
    ce.total_tickets,
    d.is_churned

FROM dim_customer_gold d

JOIN customer_engagement ce
    ON d.customer_id = ce.customer_id

WHERE
    d.is_churned = 0
    AND d.CLV_LTV >= 1000
    AND ce.total_usage < 5

ORDER BY
    d.CLV_LTV DESC

LIMIT 20;

-- ============================================================
-- 13. CUSTOMER CHURN RISK SEGMENTATION
-- ============================================================

WITH customer_metrics AS (
    SELECT
        d.customer_id,
        d.CLV_LTV,
        d.is_churned,
        COALESCE(SUM(f.usage_events), 0) AS total_usage,
        COALESCE(SUM(f.ticket_count), 0) AS total_tickets

    FROM dim_customer_gold d

    LEFT JOIN fact_customer_activity_gold f
        ON d.cust_key = f.cust_key

    GROUP BY
        d.customer_id,
        d.CLV_LTV,
        d.is_churned
)

SELECT
    CASE

        WHEN is_churned = 1
            THEN 'Already Churned'

        WHEN CLV_LTV >= 10000
             AND total_usage < 5
            THEN 'High Value - High Risk'

        WHEN CLV_LTV >= 1000
             AND total_usage < 5
            THEN 'Medium Value - High Risk'

        WHEN total_usage >= 20
            THEN 'Highly Engaged'

        ELSE 'Stable Customer'

    END AS customer_segment,

    COUNT(*) AS customers,

    ROUND(
        AVG(CLV_LTV),
        2
    ) AS average_clv,

    ROUND(
        AVG(total_usage),
        2
    ) AS average_usage,

    ROUND(
        AVG(total_tickets),
        2
    ) AS average_tickets

FROM customer_metrics

GROUP BY
    CASE

        WHEN is_churned = 1
            THEN 'Already Churned'

        WHEN CLV_LTV >= 10000
             AND total_usage < 5
            THEN 'High Value - High Risk'

        WHEN CLV_LTV >= 1000
             AND total_usage < 5
            THEN 'Medium Value - High Risk'

        WHEN total_usage >= 20
            THEN 'Highly Engaged'

        ELSE 'Stable Customer'

    END

ORDER BY customers DESC;
