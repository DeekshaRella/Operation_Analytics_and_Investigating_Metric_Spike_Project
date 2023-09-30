create database project3;
show databases;

use project3;

CREATE TABLE job_data
(
    ds DATE,
    job_id INT NOT NULL,
    actor_id INT NOT NULL,
    event VARCHAR(15) NOT NULL,
    language VARCHAR(15) NOT NULL,
    time_spent INT NOT NULL,
    org CHAR(2)
);


INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org)
VALUES ('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
    ('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
    ('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
    ('2020-11-28', 23, 1005,'transfer', 'Persian', 22, 'D'),
    ('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
    ('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
    ('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
    ('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');


SELECT 
    ds, SUM(time_spent) / COUNT(*) AS avg_time_spent
FROM
    job_data
WHERE
    ds BETWEEN '2020-11-01' AND '2020-11-30'
GROUP BY ds
ORDER BY ds;

SELECT 
    (COUNT(job_id) / (24 * 30)) AS avg_per_hour_per_day
FROM
    job_data;
    
    
select
 ds as date_of_record, 
 avg(count(event)) over()  as no_events_per_day 
 from 
 job_data 
 group by ds 
 order by ds asc;
 
 SELECT ds, event_per_day,   
 AVG(event_per_day)OVER(ORDER BY ds ROWS BETWEEN 6 PRECEDING AND  
 CURRENT ROW)AS 7_day_rolling_avg FROM  
 (SELECT ds, COUNT(DISTINCT event) AS event_per_day  
 FROM job_data WHERE       
 ds BETWEEN '2020-11-01' and '2020-11-30'  
 GROUP BY ds  
 ORDER BY ds)a;
 
 
 
 SELECT 
    job_data.language,
    COUNT(job_id) AS cnt,
    (SELECT 
            COUNT(job_id)
        FROM
            job_data) AS total,
    ROUND(((COUNT(job_id) / ((SELECT 
                    COUNT(job_id)
                FROM
                    job_data))) * 100),
            1) AS Lang_Share
FROM
    job_data
GROUP BY job_data.language
ORDER BY Lang_Share DESC;



SELECT * 
from ( select *, ROW_NUMBER() OVER( partition by job_id) as cnt from job_data) as a 
where cnt > 1;

SELECT 
    *
FROM
    job_data
WHERE
    job_id IN (SELECT 
            job_id
        FROM
            job_data
        GROUP BY job_id
        HAVING COUNT(*) > 1)