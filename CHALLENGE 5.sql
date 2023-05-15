CREATE database TELENGANA;
USE TELENGANA;

CREATE TABLE DOMESTIC_VISITOR 
( srno int,
district VARCHAR(255),
date date,
month varchar(50),
year VARCHAR(4),
no_of_visitors INT,
visitor varchar(50)
);


LOAD DATA INFILE 'merged_domestic_vistors.CSV' INTO TABLE DOMESTIC_VISITOR
FIELDS terminated by ','
enclosed by '"'
IGNORE 1 LINES;


CREATE TABLE FOREIGN_VISITOR 
( srno int,
district VARCHAR(255),
date date,
month varchar(50),
year VARCHAR(4),
no_of_visitors INT,
visitor varchar(50)
);

LOAD DATA INFILE 'merged_foreign_visitors.CSV' INTO TABLE FOREIGN_VISITOR
FIELDS terminated by ','
enclosed by '"'
IGNORE 1 LINES;

alter table domestic_visitor add column visitor varchar(50);

SET sql_safe_updates = 0;

CREATE TABLE POPULATION
(DISTRICT VARCHAR(50),
POP INT
);


#--------------------------------------
#Q1 TOP 10 DOMESTIC DIST 
SELECT district,sum(no_of_visitors) AS Visitor 
FROM domestic_visitor 
GROUP BY district 
ORDER BY Visitor DESC 
LIMIT 10;


#Q2 top 3 district base on cagr
WITH a AS 
	(SELECT district,sum(no_of_visitors) AS 2016_Visitor 
	FROM domestic_visitor 
    WHERE year = '2016' 
    GROUP BY district 
    ORDER BY Visitor DESC),
b AS 
	(SELECT district,sum(no_of_visitors) AS 2019_Visitor 
    FROM domestic_visitor 
    WHERE year = '2019' 
    GROUP BY district 
    ORDER BY Visitor DESC),
c AS 
	(SELECT a.district,2016_Visitor,2019_Visitor 
    FROM a 
    INNER JOIN b 
    ON a.district = b.district)
SELECT District,round((power((2019_Visitor/2016_Visitor),0.33333)-1)*100,2) AS CAGR 
FROM c 
ORDER BY CAGR DESC LIMIT 3; #DOMESTIC
 
WITH a AS 
	(SELECT district,sum(no_of_visitors) AS 2016_Visitor 
    FROM foreign_visitor 
    WHERE year = '2016' 
    GROUP BY district 
    ORDER BY Visitor DESC),
b AS 
	(SELECT district,sum(no_of_visitors) AS 2019_Visitor 
    FROM foreign_visitor 
    WHERE year = '2019' 
    GROUP BY district 
    ORDER BY Visitor DESC),
c AS 
	(SELECT a.district,2016_Visitor,2019_Visitor 
    FROM a 
    INNER JOIN b 
    ON a.district = b.district)
    
SELECT district,round((power((2019_Visitor/2016_Visitor),0.33333)-1)*100,2) AS cagr 
FROM c 
ORDER BY cagr DESC LIMIT 3; #FOREIGN


#Q3 bottom 3 district base on cagr
WITH a AS 
	(SELECT district,sum(no_of_visitors) AS 2016_Visitor 
    FROM domestic_visitor 
    WHERE year = '2016' 
    GROUP BY district 
    ORDER BY Visitor DESC),
b AS 
	(SELECT district,sum(no_of_visitors) AS 2019_Visitor 
    FROM domestic_visitor 
    WHERE year = '2019' 
    GROUP BY district 
    ORDER BY Visitor DESC),
c AS 
	(SELECT a.district,2016_Visitor,2019_Visitor 
    FROM a 
    INNER JOIN b 
    ON a.district = b.district),
D AS 
	(SELECT district,round((power((2019_Visitor/2016_Visitor),0.33333)-1)*100,2) AS cagr 
    FROM c) 
    
SELECT * 
FROM D 
WHERE CAGR IS NOT NULL 
ORDER BY cagr LIMIT 3; #DOMESTIC

WITH a AS 
	(SELECT district,sum(no_of_visitors) AS 2016_Visitor 
    FROM foreign_visitor 
    WHERE year = '2016' 
    GROUP BY district 
    ORDER BY Visitor DESC),
b AS 
	(SELECT district,sum(no_of_visitors) AS 2019_Visitor 
    FROM foreign_visitor 
    WHERE year = '2019' 
    GROUP BY district 
    ORDER BY Visitor DESC),
c AS 
	(SELECT a.district,2016_Visitor,2019_Visitor 
    FROM a 
    INNER JOIN b 
    ON a.district = b.district),
D AS 
	(SELECT district,round((power((2019_Visitor/2016_Visitor),0.33333)-1)*100,2) AS cagr 
    FROM c) 
SELECT * 
FROM D 
WHERE CAGR IS NOT NULL 
ORDER BY CAGR LIMIT 3; #FOREIGN


#Q4 peak & low season for hydrabad
CREATE VIEW merged_data AS (SELECT * FROM domestic_visitor UNION ALL SELECT * FROM foreign_visitor);

WITH cte AS 
	(SELECT year,month,sum(no_of_visitors) AS visitors 
    FROM merged_data 
    WHERE district = 'Hyderabad' 
    GROUP BY YEAR,MONTH)
SELECT * 
FROM cte 
WHERE visitors IN (SELECT max(visitors) FROM cte 
GROUP BY YEAR); #PEAK SEASON

WITH cte AS 
	(SELECT year,month,sum(no_of_visitors) AS visitors 
    FROM merged_data 
    WHERE district = 'Hyderabad' 
    GROUP BY YEAR,MONTH)
SELECT * 
FROM cte 
WHERE visitors IN (SELECT MIN(visitors) FROM cte 
GROUP BY YEAR); #LOW SEASON


#Q5 TOP & BOTTOM 3 WITH HIGH DOMESTIC TO FOREIGN RATIO

WITH CTE1 AS 
	(SELECT DISTRICT,SUM(no_of_visitors) AS DOMESTIC_no_of_visitors 
    FROM merged_data 
    WHERE VISITOR = 'Domestic\r' 
    GROUP BY DISTRICT),
CTE2 AS 
	(SELECT DISTRICT,SUM(no_of_visitors) AS FOREIGN_no_of_visitors 
    FROM merged_data 
    WHERE VISITOR = 'FOREIGN\r' 
    GROUP BY DISTRICT),
CTE3 AS 
	(SELECT CTE1.DISTRICT,DOMESTIC_no_of_visitors,FOREIGN_no_of_visitors 
    FROM CTE1 
    JOIN CTE2 
    ON CTE1.DISTRICT = CTE2.DISTRICT)
SELECT DISTRICT,(DOMESTIC_no_of_visitors/FOREIGN_no_of_visitors) AS D2F_RATIO 
FROM CTE3 
ORDER BY D2F_RATIO DESC LIMIT 3; #TOP

WITH CTE1 AS 
	(SELECT DISTRICT,SUM(no_of_visitors) AS DOMESTIC_no_of_visitors 
    FROM merged_data 
    WHERE VISITOR = 'Domestic\r' 
    GROUP BY DISTRICT),
CTE2 AS 
	(SELECT DISTRICT,SUM(no_of_visitors) AS FOREIGN_no_of_visitors 
    FROM merged_data 
    WHERE VISITOR = 'FOREIGN\r' 
    GROUP BY DISTRICT),
CTE3 AS 
	(SELECT CTE1.DISTRICT,DOMESTIC_no_of_visitors,FOREIGN_no_of_visitors,(DOMESTIC_no_of_visitors/FOREIGN_no_of_visitors) AS D2F_RATIO 
    FROM CTE1 
    JOIN CTE2 
    ON CTE1.DISTRICT = CTE2.DISTRICT 
    GROUP BY DISTRICT)
SELECT DISTRICT,D2F_RATIO 
FROM CTE3 
WHERE D2F_RATIO IS NOT NULL 
ORDER BY D2F_RATIO DESC;


#6 TOP & BOTTOM 5 BASED ON FOOTFALL RATIO

WITH CTE1 AS 
	(SELECT DISTRICT,SUM(no_of_visitors) AS VISITOR 
    FROM merged_data 
    GROUP BY DISTRICT),
CTE2 AS 
	(SELECT CTE1.DISTRICT,CTE1.VISITOR,POP 
    FROM CTE1 
    JOIN population 
    ON CTE1.DISTRICT = population.DISTRICT)
SELECT DISTRICT,(VISITOR/POP) AS V2P 
FROM CTE2 
GROUP BY DISTRICT 
ORDER BY V2P LIMIT 5; #BOTTOM

WITH CTE1 AS 
	(SELECT DISTRICT,SUM(no_of_visitors) AS VISITOR 
    FROM merged_data 
    GROUP BY DISTRICT),
CTE2 AS 
	(SELECT CTE1.DISTRICT,CTE1.VISITOR,POP 
    FROM CTE1 
    JOIN population 
    ON CTE1.DISTRICT = population.DISTRICT)
SELECT DISTRICT,(VISITOR/POP) AS V2P 
FROM CTE2 
GROUP BY DISTRICT 
ORDER BY V2P DESC LIMIT 5; #TOP
