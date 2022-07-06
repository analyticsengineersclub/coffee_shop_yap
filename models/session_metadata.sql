--models/session_metadata.sql

{{
  config (materialized = 'table')
}}

with 
distinct_sessions as(
  select distinct users_sessions.session_id
  ,timestamp_diff(min(users_sessions.session_end_time), min(users_sessions.session_start_time), SECOND) as length
  ,count(users_sessions.page) as page_count
  from {{ref('users_sessions')}} as users_sessions
  group by users_sessions.session_id
)
,purchase_sessions as(
  select distinct session_id
  from {{ref('users_sessions')}} users_sessions
  where page = 'order-confirmation'
)
select 
  distinct_sessions.session_id
  ,distinct_sessions.length
  ,distinct_sessions.page_count
  ,case 
    when purchase_sessions.session_id is not null then true
    else false
  end as purchase_made
from distinct_sessions 
left join purchase_sessions
  on distinct_sessions.session_id = purchase_sessions.session_id