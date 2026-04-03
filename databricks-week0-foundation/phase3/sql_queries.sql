select sale_date,sum(total_amount) from sales where total_amount is not null group by sale_date;

select c.city, sum(s.total_amount) from customers c join sales s on c.customer_id = s.customer_id group by c.city;

select customer_id, count(*) from sales group by customer_id having count(*) > 2;

select city, customer_id, total_spend from (select c.city, s.customer_id, sum(s.total_amount) as total_spend, rank() 
  over (partition by c.city order by sum(s.total_amount) desc) as rnk from customers c join sales s on c.customer_id = s.customer_id 
  group by c.city, s.customer_id) temp where rnk = 1;

select c.customer_id, c.city, sum(s.total_amount) as total_spend, count(s.sale_id) as order_count from customers c join 
  sales s on c.customer_id = s.customer_id group by c.customer_id, c.city;
