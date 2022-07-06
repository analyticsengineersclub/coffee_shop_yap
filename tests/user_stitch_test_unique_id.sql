--testing for no customer ids with more than one visitor id

select customer_id, 
from {{ref('users_stitched')}}
where customer_id is not null
group by 1 
having count(distinct visitor_id) > 1