/*Descarga los archivos CSV, estudiales y diseña una base de datos con un esquema 
de estrella que contenga, al menos 4 tablas de las que puedas realizar las siguientes consultas: */

CREATE DATABASE sprint4;

CREATE TABLE transactions (
    id VARCHAR(100) PRIMARY KEY,
    card_id VARCHAR(15),
    business_id VARCHAR(20),
    timestamp TIMESTAMP,
    amount DECIMAL(10,2),
    declined TINYINT(1),
    product_ids VARCHAR(30),
    user_id INT,  -- Sin número en los paréntesis
    lat FLOAT,
    longitude FLOAT
);

show global variables like "local_infile";
set global local_infile = 1;

-- sigue error
show grants;
GRANT FILE on *.* to 'root'@'localhost';

SHOW SESSION VARIABLES LIKE 'local_infile';
show variables like 'pid_file';

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA LOCAL INFILE '/Users/arlintv/Downloads/sprint4/transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ';'  
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

-- credit card

CREATE TABLE credit_cards (
id VARCHAR(20) PRIMARY KEY,
user_id varchar (20),
iban VARCHAR(50),
pan VARCHAR (20),
pin VARCHAR(4),
cvv INT,
track1 varchar(50),
track2 varchar(50),
expiring_date VARCHAR(20)
);

LOAD DATA LOCAL INFILE '/Users/arlintv/Downloads/sprint4/credit_cards.csv' 
INTO TABLE credit_cards 
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

-- tabla products
CREATE TABLE products (
id VARCHAR(20) PRIMARY KEY,
product_name varchar (50),
price VARCHAR(10),
colour VARCHAR(20),
weight VARCHAR(20),
warehouse_id VARCHAR(20)
);

LOAD DATA LOCAL INFILE '/Users/arlintv/Downloads/sprint4/products.csv' 
INTO TABLE products
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

-- companies
CREATE TABLE companies (
    company_id VARCHAR(100) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    email VARCHAR(100) UNIQUE,
    country VARCHAR(100),
    website VARCHAR(255)
);

LOAD DATA LOCAL INFILE '/Users/arlintv/Downloads/sprint4/companies.csv' 
INTO TABLE companies
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

-- users
CREATE TABLE users (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(100),
    surname VARCHAR(100),
    phone VARCHAR(200),
    email VARCHAR(150),
    birth_date VARCHAR(100),
    country VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(200),
    address VARCHAR(255)
);

LOAD DATA LOCAL INFILE '/Users/arlintv/Downloads/sprint4/users_ca.csv' 
INTO TABLE users
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/arlintv/Downloads/sprint4/users_uk.csv' 
INTO TABLE users
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/arlintv/Downloads/sprint4/users_usa.csv' 
INTO TABLE users
FIELDS TERMINATED BY ','  
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS;

-- Hago los cambios en los nombres de tablas que necesito,
ALTER TABLE products
RENAME COLUMN id to product_id;

ALTER TABLE transactions
RENAME COLUMN product_ids to product_id;

ALTER TABLE credit_cards
RENAME COLUMN id to card_id;

ALTER TABLE transactions
RENAME COLUMN business_id to company_id;

-- Agrego FK
ALTER TABLE transactions 
ADD CONSTRAINT fk_card_id FOREIGN KEY (card_id) 
REFERENCES credit_cards(card_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE transactions 
ADD CONSTRAINT fk_company_id FOREIGN KEY (company_id) 
REFERENCES companies(company_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE transactions MODIFY COLUMN product_id varchar(200);
ALTER TABLE products MODIFY COLUMN product_id varchar(200);

ALTER TABLE transactions 
ADD foreign key (user_id) references users(id);

/* la tabla products no la haré por ahora, me da muchos errores y ya logre conectar 4 tablas
ALTER TABLE transactions 
ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) 
REFERENCES products(product_id) 
ON DELETE CASCADE ON UPDATE CASCADE; 

SELECT DISTINCT product_id 
FROM transactions 
WHERE product_id NOT IN (SELECT product_id FROM products);
*/

/*- Ejercicio 1
Realiza una subconsulta que muestre a todos los usuarios con más de 30 transacciones utilizando al menos 2 tablas.
*/
-- primero la hice sin hacer subconsulta, pero en la revisión me lo comentaron, así que la cambié

/* SELECT u.name, u.surname, count(t.user_id) as numero_transacciones
from users as u
JOIN transactions t ON t.user_id = u.id
group by u.name, u.surname
having numero_transacciones >= 30
order by numero_transacciones desc; */

-- Usando subconsulta
SELECT u.name, u.surname, t.numero_transacciones
FROM users u
JOIN (
    SELECT user_id, COUNT(*) AS numero_transacciones
    FROM transactions
    GROUP BY user_id
    HAVING COUNT(*) >= 30
) AS t ON u.id = t.user_id
ORDER BY t.numero_transacciones DESC;

/* - Ejercicio 2
Muestra la media de amount por IBAN de las tarjetas de crédito en la compañía Donec Ltd., utiliza por lo menos 2 tablas. */

SELECT c_c.iban, avg (t.amount) as media_ventas, c.company_name
from transactions as t
JOIN companies c ON c.company_id = t.company_id
JOIN credit_cards c_c ON c_c.card_id = t.card_id
WHERE c.company_name ='Donec Ltd'
group by c_c.iban;


/* Nivel 2
Crea una nueva tabla que refleje el estado de las tarjetas de crédito basado en si las últimas tres transacciones 
fueron declinadas y genera la siguiente consulta:
Ejercicio 1: ¿Cuántas tarjetas están activas? */

-- Mi planteamiento es desglosar paso por paso, para poder entender los nuevos conceptos como el uso de "with", las ventanas 
-- y asignacion de un orden en la tabla

SELECT card_id, declined, timestamp,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS fila
    FROM transactions;
  -- en este paso le asigno un orden a cada transaccion de una card_id 

WITH ultimastransacciones AS (
    SELECT card_id, declined, timestamp,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS fila
    FROM transactions) SELECT * FROM ultimastransacciones WHERE fila <= 3;
    
  -- aqui ordeno las transacciones por fecha y selecciono las ultimas tres transacciones, al quere continuar con esta consulta
-- me daba errorer de syntax, entonces tuve que ubicar la seleccion de las ultimas tres transferencias en el bloque siguiente
-- que es hacer un bucle para determinar las tarjetas que estan activas

create table estado_tarjeta as
(
WITH ultimastransacciones AS (
    SELECT card_id, declined, timestamp,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS fila
    FROM transactions
),
estado_transaccion AS (
    SELECT card_id, SUM(declined) AS declinaciones, 
        CASE 
            WHEN SUM(declined) = 3 THEN 'BLOQUEADA' 
            ELSE 'ACTIVA' 
        END AS estado_tarjeta
    FROM ultimastransacciones
	WHERE fila <= 3 
    GROUP BY card_id
)
SELECT * FROM estado_transaccion
);

ALTER TABLE estado_tarjeta 
add primary key (card_id);

ALTER TABLE estado_tarjeta 
add foreign key (card_id) references credit_cards(card_id);

SELECT count(estado_tarjeta) as conteo_tarjetas
 FROM estado_tarjeta
 where estado_tarjeta = 'ACTIVA' ;

-- Como resultado: 275 tarjetas activas

