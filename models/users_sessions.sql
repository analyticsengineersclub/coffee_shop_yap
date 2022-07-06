--models/users_stitched.sql

with stitched_visitor_ids as
(
  select
    pageviews.customer_id
    ,min(pageviews.visitor_id) visitor_id_stitched
  from {{source('web_tracking', 'pageviews')}} as pageviews
  group by 1
)
,users_stitched as
(
  select 
    pageviews.id
    ,stitched_visitor_ids.visitor_id_stitched as visitor_id
    ,pageviews.device_type
    ,pageviews.timestamp
    ,pageviews.page
    ,pageviews.customer_id
  from {{source('web_tracking', 'pageviews')}} as pageviews
  left join stitched_visitor_ids
    on pageviews.customer_id = stitched_visitor_ids.customer_id
  where pageviews.customer_id is not null
  order by pageviews.customer_id
)
,browsing_ordered as(
select 
  users_stitched.id
  ,device_type
  ,users_stitched.timestamp
  ,customer_id
  ,(lag(users_stitched.timestamp) over (partition by customer_id, device_type order by customer_id, device_type, users_stitched.timestamp asc)) as prev_page_visit
  ,timestamp_diff(users_stitched.timestamp, (lag(users_stitched.timestamp) over (partition by customer_id, device_type order by customer_id, device_type, users_stitched.timestamp asc)), SECOND) as page_gap
  ,CASE
    when timestamp_diff(users_stitched.timestamp, (lag(users_stitched.timestamp) over (partition by customer_id, device_type order by customer_id, device_type, users_stitched.timestamp asc)), SECOND) >30 
    or timestamp_diff(users_stitched.timestamp, (lag(users_stitched.timestamp) over (partition by customer_id, device_type order by customer_id, device_type, users_stitched.timestamp asc)), SECOND) is null 
    then 1
    else 0 end as is_new_session
from users_stitched
order by customer_id, device_type, users_stitched.timestamp asc
)
,browsing_session_ids as
(
  select
    users_stitched.id
    ,users_stitched.timestamp
    ,sum(browsing_ordered.is_new_session) over (order by users_stitched.customer_id,users_stitched.device_type, users_stitched.timestamp) as session_id
    from users_stitched
    join browsing_ordered
      on users_stitched.id = browsing_ordered.id
)
,session_times as
(
  select
    browsing_session_ids.session_id
    ,min(browsing_session_ids.timestamp) as session_start_time
    ,max(browsing_session_ids.timestamp) as session_end_time
  from browsing_session_ids
  group by browsing_session_ids.session_id
)
select
  users_stitched.id
  ,users_stitched.visitor_id
  ,users_stitched.device_type
  ,users_stitched.timestamp
  ,users_stitched.page
  ,users_stitched.customer_id
  ,browsing_session_ids.session_id
  ,session_times.session_start_time
  ,session_times.session_end_time
  from users_stitched
  join browsing_ordered
    on users_stitched.id = browsing_ordered.id
  join browsing_session_ids
    on users_stitched.id = browsing_session_ids.id
  join session_times
    on browsing_session_ids.session_id = session_times.session_id