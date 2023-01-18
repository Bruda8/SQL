--=============== МОДУЛЬ 5. РАБОТА С POSTGRESQL =======================================
--= ПОМНИТЕ, ЧТО НЕОБХОДИМО УСТАНОВИТЬ ВЕРНОЕ СОЕДИНЕНИЕ И ВЫБРАТЬ СХЕМУ PUBLIC===========
SET search_path TO public;

--======== ОСНОВНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--Сделайте запрос к таблице payment и с помощью оконных функций добавьте вычисляемые колонки согласно условиям:
--Пронумеруйте все платежи от 1 до N по дате
--Пронумеруйте платежи для каждого покупателя, сортировка платежей должна быть по дате
--Посчитайте нарастающим итогом сумму всех платежей для каждого покупателя, сортировка должна 
--быть сперва по дате платежа, а затем по сумме платежа от наименьшей к большей
--Пронумеруйте платежи для каждого покупателя по стоимости платежа от наибольших к меньшим 
--так, чтобы платежи с одинаковым значением имели одинаковое значение номера.
--Можно составить на каждый пункт отдельный SQL-запрос, а можно объединить все колонки в одном запросе.

SELECT
	customer_id,
    payment_id, 
    payment_date,
    amount,
    ROW_NUMBER() OVER (ORDER BY payment_date) as a, 
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY payment_date) as b,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY payment_date, amount) as c,
    DENSE_RANK  () OVER (PARTITION BY customer_id ORDER BY amount desc) AS d
FROM payment
ORDER BY
	customer_id,
	amount 

--ЗАДАНИЕ №2
--С помощью оконной функции выведите для каждого покупателя стоимость платежа и стоимость 
--платежа из предыдущей строки со значением по умолчанию 0.0 с сортировкой по дате.

SELECT
	customer_id,
	payment_id,
	payment_date,
	amount,
	LAG (amount, 1, 0.0) OVER (PARTITION BY customer_id ORDER BY payment_date) AS last_amount
FROM
	payment

--ЗАДАНИЕ №3
--С помощью оконной функции определите, на сколько каждый следующий платеж покупателя больше или меньше текущего.

SELECT
	customer_id,
	payment_id,
	payment_date,
	amount,
	amount - LEAD (amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY payment_date) AS difference
FROM
	payment
ORDER BY
	customer_id,
	payment_date

--ЗАДАНИЕ №4
--С помощью оконной функции для каждого покупателя выведите данные о его последней оплате аренды.

WITH cte as (
	SELECT
		customer_id,
		payment_id,
		payment_date,
		amount,
		ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY payment_date desc) AS rn
	FROM 
		payment
)
SELECT
	customer_id,
	payment_id,
	payment_date,
	amount
FROM
	cte
WHERE
	rn = 1
	
--------второй вариант через обычный подзапрос---------------	

SELECT
	customer_id,
	payment_id,
	payment_date,
	amount
FROM
	(
	SELECT
		customer_id,
		payment_id,
		payment_date,
		amount,
		ROW_NUMBER () OVER (PARTITION BY customer_id ORDER BY payment_date desc) AS rn
	FROM 
		payment
) t
WHERE
	rn = 1
	
--======== ДОПОЛНИТЕЛЬНАЯ ЧАСТЬ ==============

--ЗАДАНИЕ №1
--С помощью оконной функции выведите для каждого сотрудника сумму продаж за август 2005 года 
--с нарастающим итогом по каждому сотруднику и по каждой дате продажи (без учёта времени) 
--с сортировкой по дате.

SELECT
	staff_id,
	payment_date,
	amount,
	sum(amount) OVER (PARTITION BY staff_id, payment_date::date ORDER BY payment_date)
FROM 
	payment p
WHERE
	EXTRACT (MONTH FROM payment_date) = 8
	AND EXTRACT (YEAR FROM payment_date) = 2005
ORDER BY
	staff_id,
	payment_date 

--ЗАДАНИЕ №2
--20 августа 2005 года в магазинах проходила акция: покупатель каждого сотого платежа получал
--дополнительную скидку на следующую аренду. С помощью оконной функции выведите всех покупателей,
--которые в день проведения акции получили скидку

WITH cte AS (
	SELECT
		customer_id,
		payment_date,
		amount,
		ROW_NUMBER () OVER (ORDER BY payment_date) AS rn
	FROM 
		payment
	WHERE
		EXTRACT (DAY FROM payment_date) = 20
		AND EXTRACT (MONTH FROM payment_date) = 8
		AND EXTRACT (YEAR FROM payment_date) = 2005	
	)
SELECT
	customer_id,
	payment_date,
	amount
FROM
	cte
WHERE
	rn % 100 = 0

	
--ЗАДАНИЕ №3
--Для каждой страны определите и выведите одним SQL-запросом покупателей, которые попадают под условия:
-- 1. покупатель, арендовавший наибольшее количество фильмов
-- 2. покупатель, арендовавший фильмов на самую большую сумму
-- 3. покупатель, который последним арендовал фильм

WITH cte1 AS (
	SELECT 
		country,
		customer_id,
		qty,
		ROW_NUMBER () OVER (PARTITION BY country ORDER BY qty desc) AS max_film
	FROM 
		(
		SELECT
			co.country,
			c.customer_id,
			count(r.rental_id) AS qty
		FROM 
			country co
		JOIN city ci ON co.country_id = ci.country_id 
		JOIN address a ON ci.city_id = a.city_id 
		JOIN customer c ON a.address_id = c.address_id 
		JOIN rental r ON c.customer_id = r.customer_id
		JOIN payment p ON c.customer_id = p.customer_id 
		GROUP BY 
			co.country,
			c.customer_id
		) t
),
cte2 AS (
	SELECT 
		country,
		customer_id,
		sum_amount,
		ROW_NUMBER () OVER (PARTITION BY country ORDER BY sum_amount desc) AS max_sum
	FROM 
		(
		SELECT
			co.country,
			c.customer_id,
			sum(p.amount) AS sum_amount
		FROM 
			country co
		JOIN city ci ON co.country_id = ci.country_id 
		JOIN address a ON ci.city_id = a.city_id 
		JOIN customer c ON a.address_id = c.address_id 
		JOIN rental r ON c.customer_id = r.customer_id
		JOIN payment p ON c.customer_id = p.customer_id 
		GROUP BY 
			co.country,
			c.customer_id
		) t
),
cte3 AS (
	SELECT 
		country,
		customer_id,
		ROW_NUMBER () OVER (PARTITION BY country ORDER BY max_rental desc) AS max_rental
	FROM 
		(
		SELECT
			co.country,
			c.customer_id,
			max(r.rental_date) AS max_rental
		FROM 
			country co
		JOIN city ci ON co.country_id = ci.country_id 
		JOIN address a ON ci.city_id = a.city_id 
		JOIN customer c ON a.address_id = c.address_id 
		JOIN rental r ON c.customer_id = r.customer_id
		JOIN payment p ON c.customer_id = p.customer_id 
		GROUP BY 
			co.country,
			c.customer_id
		) t
)
SELECT 
	cte1.country,
	cte1.customer_id AS max_film_customer,
	cte2.customer_id AS max_sum_customer,
	cte3.customer_id AS max_rental_customer
FROM
	cte1
JOIN cte2 ON cte1.country = cte2.country
JOIN cte3 ON cte1.country = cte3.country
WHERE 
	cte1.max_film = 1
	AND	cte2.max_sum = 1
	AND	cte3.max_rental = 1
