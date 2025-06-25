# Задание 3.
WITH paid_mediums AS (
SELECT unnest(ARRAY['cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social']) AS medium
),
paid_sessions AS (
SELECT
s.visitor_id,
s.visit_date::date AS visit_date,
lower(s.source) AS utm_source,
lower(s.medium) AS utm_medium,
lower(s.campaign) AS utm_campaign,
lower(s.content) AS utm_content
FROM sessions s
JOIN paid_mediums p ON lower(s.medium) = p.medium
),
last_paid_clicks AS (
SELECT DISTINCT ON (l.lead_id)
l.lead_id,
l.created_at,
l.amount,
l.closing_reason,
l.status_id,
ps.visit_date,
ps.utm_source,
ps.utm_medium,
ps.utm_campaign,
ps.visitor_id
FROM leads l
JOIN paid_sessions ps
ON l.visitor_id = ps.visitor_id
AND ps.visit_date <= l.created_at::date
ORDER BY l.lead_id, ps.visit_date DESC
),
vk_costs AS (
SELECT
campaign_date::date AS visit_date,
lower(utm_source) AS utm_source,
lower(utm_medium) AS utm_medium,
lower(utm_campaign) AS utm_campaign,
SUM(daily_spent) AS cost
FROM vk_ads
GROUP BY 1, 2, 3, 4
),
ya_costs AS (
SELECT
campaign_date::date AS visit_date,
lower(utm_source) AS utm_source,
lower(utm_medium) AS utm_medium,
lower(utm_campaign) AS utm_campaign,
SUM(daily_spent) AS cost
FROM ya_ads
GROUP BY 1, 2, 3, 4
),
all_costs AS (
SELECT * FROM vk_costs
UNION ALL
SELECT * FROM ya_costs
),
visits_agg AS (
SELECT
ps.visit_date,
ps.utm_source,
ps.utm_medium,
ps.utm_campaign,
COUNT(*) AS visitors_count
FROM paid_sessions ps
GROUP BY 1, 2, 3, 4
),
leads_agg AS (
SELECT
lpc.visit_date,
lpc.utm_source,
lpc.utm_medium,
lpc.utm_campaign,
COUNT(DISTINCT lpc.lead_id) AS leads_count,
COUNT(DISTINCT CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.lead_id END) AS purchases_count,
SUM(CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.amount END) AS revenue
FROM last_paid_clicks lpc
GROUP BY 1, 2, 3, 4
),
final_agg AS (
SELECT
v.visit_date,
v.utm_source,
v.utm_medium,
v.utm_campaign,
v.visitors_count,
COALESCE(c.cost, 0) AS total_cost,
COALESCE(l.leads_count, 0) AS leads_count,
COALESCE(l.purchases_count, 0) AS purchases_count,
l.revenue
FROM visits_agg v
LEFT JOIN all_costs c
ON v.visit_date = c.visit_date
AND v.utm_source = c.utm_source
AND v.utm_medium = c.utm_medium
AND v.utm_campaign = c.utm_campaign
LEFT JOIN leads_agg l
ON v.visit_date = l.visit_date
AND v.utm_source = l.utm_source
AND v.utm_medium = l.utm_medium
AND v.utm_campaign = l.utm_campaign
)
SELECT *
FROM final_agg
ORDER BY
revenue DESC NULLS LAST,
visit_date ASC,
visitors_count DESC,
utm_source ASC,
utm_medium ASC,
utm_campaign ASC;


~~ Черновик

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
),
tab_1 as (
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
utm_campaign asc),
vk_costs AS (
SELECT
campaign_date::date AS visit_date,
lower(utm_source) AS utm_source,
lower(utm_medium) AS utm_medium,
lower(utm_campaign) AS utm_campaign,
SUM(daily_spent) AS cost
FROM vk_ads
GROUP BY 1, 2, 3, 4
),
ya_costs AS (
SELECT
campaign_date::date AS visit_date,
lower(utm_source) AS utm_source,
lower(utm_medium) AS utm_medium,
lower(utm_campaign) AS utm_campaign,
SUM(daily_spent) AS cost
FROM ya_ads
GROUP BY 1, 2, 3, 4
),
all_costs AS (
SELECT * FROM vk_costs
UNION ALL
SELECT * FROM ya_costs
),
visits_agg AS (
SELECT
ps.visit_date,
ps.utm_source,
ps.utm_medium,
ps.utm_campaign,
COUNT(*) AS visitors_count
FROM paid_sessions ps
GROUP BY 1, 2, 3, 4
),
leads_agg AS (
SELECT
lpc.visit_date,
lpc.utm_source,
lpc.utm_medium,
lpc.utm_campaign,
COUNT(DISTINCT lpc.lead_id) AS leads_count,
COUNT(DISTINCT CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.lead_id END) AS purchases_count,
SUM(CASE WHEN lpc.closing_reason = 'Успешно реализовано' OR lpc.status_id = 142 THEN lpc.amount END) AS revenue
FROM last_paid_clicks lpc
GROUP BY 1, 2, 3, 4
),
final_agg AS (
SELECT
v.visit_date,
v.visitors_count,
v.utm_source,
v.utm_medium,
v.utm_campaign,
COALESCE(c.cost, 0) AS total_cost,
COALESCE(l.leads_count, 0) AS leads_count,
COALESCE(l.purchases_count, 0) AS purchases_count,
l.revenue
FROM visits_agg v
LEFT JOIN all_costs c
ON v.visit_date = c.visit_date
AND v.utm_source = c.utm_source
AND v.utm_medium = c.utm_medium
AND v.utm_campaign = c.utm_campaign
LEFT JOIN leads_agg l
ON v.visit_date = l.visit_date
AND v.utm_source = l.utm_source
AND v.utm_medium = l.utm_medium
AND v.utm_campaign = l.utm_campaign
)
SELECT *
FROM final_agg
ORDER BY
revenue DESC NULLS LAST,
visit_date ASC,
visitors_count DESC,
utm_source ASC,
utm_medium ASC,
utm_campaign ASC;
