{{config (
  materialized='table'
)}}

select 
cus.id
,cus.name
,cus.email
,min(ord.created_at) as first_order_at
,count(ord.total) as number_of_orders
from analytics-engineers-club.coffee_shop.customers cus
  left join analytics-engineers-club.coffee_shop.orders ord 
  on cus.id = ord.customer_id
group by cus.id, cus.name, cus.email
order by first_order_at
