WITH cte1 AS (
	SELECT 
    	s.store_id,
    	DATE(r.rental_date) AS rental_day,
    	COUNT(i.inventory_id) AS rental_count,
    	ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY count(i.inventory_id) DESC) as store_rank
	FROM 
	    store s
	    JOIN inventory i ON s.store_id = i.store_id 
	    JOIN rental r ON i.inventory_id = r.inventory_id 
	GROUP BY
	    s.store_id,
	    r.rental_date
), 
cte2 AS(
	SELECT
		s.store_id,
		date(p.payment_date) AS pay_date,
		sum(p.amount) AS total_sales,
		ROW_NUMBER () OVER (PARTITION BY s.store_id ORDER BY sum(p.amount)) AS store_rank
	FROM
		store s
	JOIN customer c ON s.store_id = c.store_id 
	JOIN payment p  ON c.customer_id = p.customer_id
	GROUP BY
		s.store_id,
		pay_date
)
SELECT 
	cte1.store_id,
	cte1.rental_day,
	cte1.rental_count,
	cte2.pay_date,
	cte2.total_sales
FROM
	cte1
JOIN cte2 ON cte1.store_id = cte2.store_id
WHERE
	cte1.store_rank = 1 AND cte2.store_rank = 1
