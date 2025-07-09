~~ Задание 2
WITH
sessions_with_leads AS (
SELECT
s.visitor_id,
s.visit_date,
s.source AS utm_source,
s.medium AS utm_medium,
s.campaign AS utm_campaign,
l.lead_id,
l.amount,
l.created_at,
l.closing_reason,
l.status_id,
ROW_NUMBER() OVER (PARTITION BY l.visitor_id ORDER BY s.visit_date DESC) AS rn
FROM sessions s
JOIN leads l
ON s.visitor_id = l.visitor_id
AND s.visit_date <= l.created_at
WHERE LOWER(s.medium) <>'organic'
),
last_paid_clicks AS (
SELECT *
FROM sessions_with_leads
WHERE rn = 1
),
all_visits AS (
SELECT
s.visitor_id,
s.visit_date,
s.source AS utm_source,
s.medium AS utm_medium,
s.campaign AS utm_campaign
FROM sessions s
)
SELECT
v.visitor_id,
v.visit_date,
v.utm_source,
v.utm_medium,
v.utm_campaign,
l.lead_id,
l.created_at,
l.amount,
l.closing_reason,
l.status_id
FROM all_visits v
LEFT JOIN last_paid_clicks l
ON v.visitor_id = l.visitor_id
AND v.visit_date = l.visit_date
ORDER BY
amount DESC NULLS LAST,
visit_date ASC,
utm_source ASC,
utm_medium ASC,
utm_campaign ASC;
