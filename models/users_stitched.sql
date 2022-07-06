--models/users_stitched.sql

with stitched_visitor_ids as
(
  select
    pageviews.customer_id,
    min(pageviews.visitor_id) visitor_id_stitched
  from {{source('web_tracking', 'pageviews')}} as pageviews
  group by 1
)

select 
  pageviews.id,
  stitched_visitor_ids.visitor_id_stitched as visitor_id,
  pageviews.device_type,
  pageviews.timestamp,
  pageviews.page,
  pageviews.customer_id
from {{source('web_tracking', 'pageviews')}} as pageviews
left join stitched_visitor_ids
  on pageviews.customer_id = stitched_visitor_ids.customer_id
where pageviews.customer_id is not null
order by pageviews.customer_id