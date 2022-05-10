# Проект 1
Опишите здесь поэтапно ход решения задачи. Вы можете ориентироваться на тот план выполнения проекта, который мы предлагаем в инструкции на платформе.

1. Построeние витрины для RFM-анализа
1.1. Выясните требования к целевой витрине
Витрина должна находиться в схеме analysis.

Структура витрины:
user_id
recency (число от 1 до 5)
frequency (число от 1 до 5)
monetary_value (число от 1 до 5)

В витрину должны попасть данные с 2021 года.
Витрина должна называться dm_rfm_segments.
Обновлять витрину не нужно.


1.2. Изучите структуру исходных данных
Таблицы необходимые для построения витрины:
production.users - клиенты
production.orders - основная таблица для расчета метрик
production.orderstatuses - нужны заказы со статусом "Closed"

Поле user_id получаем из таблицы production.users.
Поле recency (давность последнего заказа) считаем на основе поля order_ts из таблицы production.orders.
Поле frequency (количество заказов) считаем по полям order_id, user_id из таблицы production.orders.
Поле monetary_value (общая стоимость заказов) по полям "cost", user_id из таблицы production.orders.

1.3. Проанализируйте качество данных
1. Проверки входных данных.
Таблица production.orderitems:
- дублей по полям product_id, order_id не обнаружено;
- для всех полей установлено значение NOT NULL;
- отрицательных значений в полях price, discount, quantity не обнаружено;
- поле discount во всех записях равно 0, выбросов нет.

Таблица production.orders:
- для всех полей установлено значение NOT NULL;
- значения поля order_ts между 2022-02-12 и 2022-03-14;
- отрицательных значений в полях bonus_payment, payment, cost, bonus_grant не обнаружено.

Таблица production.orderstatuses:
- для всех полей установлено значение NOT NULL;
- дублей и выбросов нет, содержит только статусы заказов.

Таблица production.orderstatuslog:
- для всех полей установлено значение NOT NULL;
- значения поля dttm между 2022-02-12 и 2022-03-14;
- дублей и выбросов нет.

Таблица production.products:
- для всех полей установлено значение NOT NULL;
- дублей и выбросов нет.

Таблица production.users:
- дублей и выбросов нет.

2. Можно утверждать что данные качественные.

3. Инструменты для обеспечения качества данных использованы в таблицах в схеме production.
Таблица production.orderitems:
Поле discount положительное и не превышает цену;
Ключ уникальности по полям order_id, product_id;
Первичный ключ id;
Поле price положительное;
Поле quantity положительное;
Поле order_id содержит значения содержащиеся в поле order_id таблицы production.orders (внешний ключ);
Поле product_id содержит значения содержащиеся в поле id таблицы production.products (внешний ключ).

Таблица production.orders:
Для всех полей установлено значение NOT NULL;
Поле cost равно сумме полей payment и bonus_payment;
Первичный ключ order_id.

Таблица production.orderstatuses:
Для всех полей установлено значение NOT NULL;
Первичный ключ id.

Таблица production.orderstatuslog:
Для всех полей установлено значение NOT NULL;
Ключ уникальности по полям order_id, status_id;
Первичный ключ id;
Поле order_id содержит значения содержащиеся в поле order_id таблицы production.orders (внешний ключ);
Поле status_id содержит значения содержащиеся в поле id таблицы production.orderstatuses (внешний ключ).

Таблица production.products:
Для всех полей установлено значение NOT NULL;
Первичный ключ id;
Поле price положительное.

Таблица production.users:
Первичный ключ id.


1.4. Подготовьте витрину данных
1. Сделайте представление для таблиц из базы production.
CREATE OR REPLACE VIEW analysis.orderitems AS
SELECT 
	id 		   AS "id",
	product_id AS product_id,
	order_id   AS order_id,
	"name"     AS "name",
	price      AS price,
	discount   AS discount,
	quantity   AS quantity
FROM production.orderitems;

CREATE OR REPLACE VIEW analysis.orders AS 
SELECT 
	order_id      AS order_id,
	order_ts      AS order_ts,
	user_id       AS user_id,
	bonus_payment AS bonus_payment,
	payment       AS payment,
	"cost"        AS "cost",
	bonus_grant   AS bonus_grant,
	status        AS status
FROM production.orders;

CREATE OR REPLACE VIEW analysis.orderstatuses AS
SELECT 
	id    AS id, 
	"key" AS "key"
FROM production.orderstatuses;

CREATE OR REPLACE VIEW analysis.orderstatuslog AS
SELECT 
	id        AS id,
	order_id  AS order_id,
	status_id AS status_id,
	dttm      AS dttm
FROM production.orderstatuslog;

CREATE OR REPLACE VIEW analysis.products AS 
SELECT 
	id     AS id,
	"name" AS "name",
	price  AS price
FROM production.products;

CREATE OR REPLACE VIEW analysis.users AS 
SELECT 
	id     AS id,
	"name" AS "name",
	login  AS login
FROM production.users;

2. Напишите DDL-запрос для создания витрины.
CREATE TABLE analysis.dm_rfm_segments (
	user_id INTEGER NOT NULL,
	recency SMALLINT CHECK (recency >= 1 AND recency <= 5),
	frequency SMALLINT CHECK (frequency >= 1 AND frequency <= 5),
	monetary_value SMALLINT CHECK (frequency >= 1 AND frequency <= 5)
);

3. Напишите SQL-запрос для заполнения витрины.
INSERT INTO analysis.dm_rfm_segments (user_id, recency, frequency, monetary_value)
WITH cte AS (
	SELECT DISTINCT
		user_id,
		LAST_VALUE(order_ts) OVER (PARTITION BY user_id ORDER BY order_ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_order_date,
		COUNT(order_id) OVER (PARTITION BY user_id) count_orders,
		SUM("cost") OVER (PARTITION BY user_id) sum_cost_orders
	FROM analysis.orders o 
	JOIN analysis.orderstatuses os ON o.status = os.id AND os."key" = 'Closed'
	WHERE o.order_ts::date >= date'2021-01-01'
)
SELECT
	u.id AS user_id,
	NTILE(5) OVER (ORDER BY cte.last_order_date) AS recency,
	NTILE(5) OVER (ORDER BY cte.count_orders) AS frequency,
	NTILE(5) OVER (ORDER BY cte.sum_cost_orders) AS monetary_value
FROM analysis.users u LEFT JOIN cte ON u.id = cte.user_id;


2. Доработка представлений
CREATE OR REPLACE VIEW analysis.orders AS 
WITH cte AS (
SELECT DISTINCT
	order_id,
	LAST_VALUE(dttm) OVER (PARTITION BY order_id ORDER BY dttm ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_date
FROM production.orderstatuslog  
),
last_status AS (
SELECT 
	osl.order_id,
	osl.status_id AS status
FROM production.orderstatuslog osl RIGHT JOIN cte  
ON osl.order_id = cte.order_id AND osl.dttm = cte.last_date
)
SELECT 
	o.order_id      AS order_id,
	o.order_ts      AS order_ts,
	o.user_id       AS user_id,
	o.bonus_payment AS bonus_payment,
	o.payment       AS payment,
	o."cost"        AS "cost",
	o.bonus_grant   AS bonus_grant,
	ls.status       AS status
FROM production.orders o 
LEFT JOIN last_status ls ON o.order_id = ls.order_id;