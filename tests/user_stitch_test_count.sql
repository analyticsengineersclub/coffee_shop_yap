--testing for no customer ids with more than one visitor id
with pageview_count as(
select
count(pageviews.id) as result_count
from {{source('web_tracking', 'pageviews')}} as pageviews
where pageviews.customer_id is not null
)
select count(users_stitched.id) as result_count
from {{ref('users_stitched')}} users_stitched
having result_count <> (select result_count from pageview_count)
