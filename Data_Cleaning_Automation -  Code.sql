-- -----------------------------------------------------------------------------------------------------------------------
-- ----------- Data Cleaning  -----------------------------------------------------
-- -----------------------------------------------------------------------------------------------------------------------

# 1. Create table linkedin_ofertas

CREATE TABLE linkedin_ofertas (
  id_oferta INT PRIMARY KEY,
  fecha_actualizacion datetime,
  nombre_empresa varchar(200) ,
  fecha_busqueda_oferta_linkedin datetime ,
  fecha_publicacion_oferta date ,
  ubicacion_oferta varchar(200),
  search_id_oferta int ,
  titulo_oferta varchar(200),
  fecha_actualizacion_sp datetime
);

# 2. Modify the date restrictions by executing the following statement:
SET @@SESSION.sql_mode='ALLOW_INVALID_DATES';


# 3. Define the query that will contain the clean data in the desired format.

INSERT INTO linkedin_data.linkedin_ofertas

SELECT 
id as id_oferta,
_fivetran_synced as fecha_actualizacion,
company_name as nombre_empresa,
DATE_FORMAT(STR_TO_DATE(date,"%Y-%m-%d %H:%i:%s"),'%Y-%m-%d %H:%i:%s') as fecha_busqueda_oferta_linkedin,
date_published as fecha_publicacion_oferta,
location as ubicacion_oferta,
searches as search_id_oferta,
title as titulo_oferta,
NOW() AS fecha_actualizacion_sp
FROM linkedin_data.raw_linkedin_results
WHERE _fivetran_synced is not null;

# 4. Create SP with this query:


CREATE DEFINER=`root`@`localhost` PROCEDURE `update_table_linkedin_ofertas`()
BEGIN  

SET @@SESSION.sql_mode='ALLOW_INVALID_DATES';

INSERT INTO linkedin_data.linkedin_ofertas 
	SELECT 
		id as id_oferta,
		DATE_FORMAT(STR_TO_DATE(_fivetran_synced,"%Y-%m-%d %H:%i:%s"),'%Y-%m-%d %H:%i:%s') as fecha_actualizacion,
		company_name as nombre_empresa,
		DATE_FORMAT(STR_TO_DATE(date,"%Y-%m-%d %H:%i:%s"),'%Y-%m-%d %H:%i:%s') as fecha_busqueda_oferta_linkedin,
		date_published as fecha_publicacion_oferta,
		location as pais_oferta,
		searches as search_id_oferta,
		title as titulo_oferta,
        NOW() as fecha_actualizacion_sp
	FROM linkedin_data.raw_linkedin_results
	WHERE id not in (SELECT id_oferta FROM linkedin_data.linkedin_ofertas);

END


# 5. Create an event to run the SP on a daily basis.

CREATE 
EVENT `update_table_linkedin_ofertas`
ON SCHEDULE EVERY 1 DAY 
STARTS TIMESTAMP(NOW() + INTERVAL 1 MINUTE) 
DO CALL update_table_linkedin_ofertas();

# 6. Show the event.

SHOW EVENTS;

# 7.View event code
SHOW CREATE EVENT update_table_linkedin_ofertas;



 
 ------ PART II - Create busquedas

# 1. Create table linkedin_busquedas

CREATE TABLE linkedin_busquedas (
  id_busqueda  INT PRIMARY KEY,
  fecha_busqueda datetime ,
  fecha_actualizacion datetime,
  keyword_busqueda varchar(200) ,
  pais_busqueda varchar(200),
  n_resultados_busqueda int,
  fecha_actualizacion_sp datetime 
);

# 2. Modify the date restrictions by executing the following statement:

SET @@SESSION.sql_mode='ALLOW_INVALID_DATES';


# 3. Define the query that will contain the clean data in the desired format.

SELECT    
	id as id_busqueda,
	timestamp(STR_TO_DATE(date,"%Y-%m-%d %H:%i:%s")) as fecha_busqueda_1, -- posible solucion
	DATE_FORMAT(STR_TO_DATE(date,"%Y-%m-%d %H:%i:%s"),'%Y-%m-%d %H:%i:%s') as fecha_busqueda,
	DATE_FORMAT(STR_TO_DATE(_fivetran_synced,"%Y-%m-%d %H:%i:%s"),'%Y-%m-%d %H:%i:%s') as fecha_actualizacion,   
	keyword as keyword_busqueda,   location as pais_busqueda, 
   cast(REPLACE(REPLACE(n_results,",",""),"+","") as UNSIGNED) as n_resultados_busqueda  
	FROM linkedin_data.raw_linkedin_searches
      WHERE _fivetran_synced is not null AND id not in (SELECT id_busqueda FROM linkedin_data.linkedin_busquedas);


# 4. Create a SP with this query:


CREATE DEFINER=`root`@`localhost` PROCEDURE `update_table_linkedin_busquedas`()
BEGIN  

SET @@SESSION.sql_mode='ALLOW_INVALID_DATES';
     
INSERT INTO linkedin_data.linkedin_busquedas (id_busqueda, fecha_busqueda, fecha_actualizacion, keyword_busqueda, pais_busqueda, n_resultados_busqueda)  
	SELECT    
	id as id_busqueda,
	DATE_FORMAT(STR_TO_DATE(date,"%Y-%m-%d %H:%i:%s"),'%Y-%m-%d %H:%i:%s') as fecha_busqueda,
	DATE_FORMAT(STR_TO_DATE(_fivetran_synced,"%Y-%m-%d %H:%i:%s"),'%Y-%m-%d %H:%i:%s') as fecha_actualizacion,   
	keyword as keyword_busqueda,   location as pais_busqueda,   
	cast(REPLACE(REPLACE(n_results,",",""),"+","") as UNSIGNED) as n_resultados_busqueda   
	FROM linkedin_data.raw_linkedin_searches         
	WHERE _fivetran_synced is not null AND id not in (SELECT id_busqueda FROM linkedin_data.linkedin_busquedas)

END;

# 5. Create an event to run the SP

# CREAMOS UN EJECUTADOR DEL SP CON EVENTOS
CREATE 
EVENT `update_table_linkedin_busquedas`
ON SCHEDULE EVERY 1 DAY 
STARTS TIMESTAMP(NOW() + INTERVAL 1 MINUTE) 
DO CALL update_table_linkedin_busquedas();

# 6. Show event
SHOW EVENTS;

# 7. View event code

SHOW CREATE EVENT update_table_linkedin_busquedas;


-- -----------------------------------------------------------------------------------------------------------------------
-- -----------  Exploratory analysis - data validation  -----------------------------------------------------
-- -----------------------------------------------------------------------------------------------------------------------

# To validate data we must always counter it with the original source.
# In order to do that we go to Linkedin and check

# 1. One check that we can do is see the number of offers per day if it makes sense
SELECT
fecha_publicacion_oferta,
count(*)
FROM linkedin_data.linkedin_ofertas r
GROUP BY fecha_publicacion_oferta
ORDER BY fecha_publicacion_oferta DESC;



-- -----------------------------------------------------------------------------------------------------------------------
-- -----------  Analysis Linkedin data  -----------------------------------------------------
-- -----------------------------------------------------------------------------------------------------------------------
# 1. Which are the companies with the greatest number of offers?

SELECT
nombre_empresa,
count(*) AS cantidad_ofertas
FROM linkedin_data.linkedin_ofertas
GROUP BY nombre_empresa
ORDER BY count(*) DESC;

# 2. How many offers do we have in the table by location?
SELECT
ubicacion_oferta,
count(*) AS cantidad_ofertas
FROM linkedin_data.linkedin_ofertas
GROUP BY ubicacion_oferta
ORDER BY count(*) DESC;


# 3. How many offers do we have published per day?
SELECT
fecha_publicacion_oferta,
count(*) AS cantidad_ofertas
FROM linkedin_data.linkedin_ofertas
GROUP BY fecha_publicacion_oferta
ORDER BY count(*) DESC;

## 4. What are the top 10 role titles used to post offers?

SELECT 
titulo_oferta,
count(*) AS cantidad_ofertas
FROM linkedin_data.linkedin_ofertas
GROUP BY titulo_oferta
ORDER BY count(*) DESC
LIMIT 10;

# 5. Which are the 5 locations with the most offers?

SELECT 
ubicacion_oferta,
COUNT(*) as cantidad_ofertas
FROM linkedin_data.linkedin_ofertas
GROUP BY ubicacion_oferta
ORDER BY count(*) DESC
LIMIT 5;



# 6. How many job offers are there combining keyword with job title?
# Can you return the amount by adding for both fields?

SELECT
	b.keyword_busqueda,
	o.titulo_oferta,
	count(*) as cantidad_ofertas
FROM linkedin_data.linkedin_busquedas b
LEFT JOIN linkedin_data.linkedin_ofertas o on search_id_oferta = id_busqueda
GROUP BY 
	keyword_busqueda,
	titulo_oferta
ORDER BY cantidad_ofertas desc

;

# 7. How many positions do we have as juniors, can you bring the amount per title?

SELECT 
titulo_oferta,
count(*)
FROM linkedin_data.linkedin_ofertas o 
LEFT JOIN linkedin_data.linkedin_busquedas b on o.search_id_oferta = b.id_busqueda
WHERE 
titulo_oferta LIKE  '%Junior%' 
OR titulo_oferta LIKE '%Jr%'
OR titulo_oferta LIKE '%Intern%'
OR titulo_oferta LIKE '%Entry-Level%'
OR titulo_oferta LIKE '%Entry%'
GROUP BY titulo_oferta
order by count(*) DESC;

# 8. Can you now return the number of offers with the junior title, but by country?

SELECT 
ubicacion_oferta,
count(*)
FROM linkedin_data.linkedin_ofertas o 
LEFT JOIN linkedin_data.linkedin_busquedas b on o.search_id_oferta = b.id_busqueda
WHERE 
titulo_oferta LIKE  '%Junior%' 
OR titulo_oferta LIKE '%Jr%'
OR titulo_oferta LIKE '%Intern%'
OR titulo_oferta LIKE '%Entry-Level%'
OR titulo_oferta LIKE '%Entry%'
GROUP BY ubicacion_oferta
order by count(*) DESC;

# 9. Can we know the number of offers published per month and keyword?
# Which months are top and with what keywords?

SELECT 
month(fecha_publicacion_oferta) AS mes_oferta,
b.keyword_busqueda,
count(*) as cantidad_de_ofertas
FROM linkedin_data.linkedin_ofertas o 
LEFT JOIN linkedin_data.linkedin_busquedas b on o.search_id_oferta = b.id_busqueda
GROUP BY mes_oferta,b.keyword_busqueda
order by mes_oferta,count(*) DESC