with 
customers as 
(

    select *
    from {{ ref('stg_customers') }}

),

orders as (

    select * 
    from {{ ref('stg_orders') }}

),

payments as (

    select * 
    from {{ ref('stg_payments') }}
    where STATUS = 'success'
),



customer_orders as (

    select orders.customer_id
         , min(orders.order_date) as first_order_date
         , max(orders.order_date) as most_recent_order_date
         , count(orders.order_id) as number_of_orders
         , SUM(payments.AMOUNT) as lifetime_ammount
    from orders 
    left
    join payments ON orders.order_id = payments.order_id 
    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_ammount, 0) as lifetime_ammount
    from customers
    left join customer_orders ON customers.customer_id = customer_orders.customer_id

)

select * from final