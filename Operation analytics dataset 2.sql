create database project4;
show databases;
use project4;

create table users (
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Table-1 users (1).csv"
Into TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
Ignore 1 ROWS;

select * from users;

alter table users add column temp_created_at datetime;

UPDATE users SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');

ALTER TABLE users DROP COLUMN created_at;

ALTER TABLE users CHANGE COLUMN temp_created_at created_at DATETIME;



CREATE TABLE events (
user_id INT,
occurred_at VARCHAR(100),
event_type VARCHAR(50),
event_name VARCHAR(100),
location VARCHAR(50),
device VARCHAR(50),
user_type int);


LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Table-2 events (1).csv"
Into TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
Ignore 1 ROWS;

desc events;

select * from events;

ALTER TABLE events add column temp_occurred_at datetime;

UPDATE events SET temp_occurred_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

ALTER TABLE events DROP COLUMN occurred_at;

ALTER TABLE events CHANGE COLUMN temp_occurred_at occurred_at DATETIME;


drop table emailevents;
show databases;

create table emailEvents(
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int
);


LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Table-3 email_events (1).csv"
Into TABLE emailEvents
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
Ignore 1 ROWS;

select * from emailEvents;


SELECT 
    EXTRACT(WEEK FROM occurred_at) AS week_num,
    COUNT(DISTINCT user_id) AS engagement
FROM
    events
GROUP BY week_num;


select extract(week from occurred_at) as sign_up_week,count(distinct user_id) as 'count of users sign up'
from events
where event_type = 'signup_flow'
and event_name = 'complete_signup'
group by sign_up_week;

SELECT 
    EXTRACT(YEAR FROM OCCURRED_AT) AS YEAR,
    EXTRACT(WEEK FROM OCCURRED_AT) AS WEEK_NUMBER,
    DEVICE,
    COUNT(DISTINCT USER_ID) USER_TYPE
FROM
    events
WHERE
    EVENT_TYPE = 'ENGAGEMENT'
GROUP BY 1 , 2 , 3
ORDER BY 1 , 2 , 3;


select device,extract(week from occurred_at) as week_number, count(user_id) from 
events where event_type='engagement' group by device, week_number;



SELECT device,
       EXTRACT(week FROM occurred_at) AS week_number,
       COUNT(user_id) AS active_users
FROM events
WHERE event_type = 'engagement'
GROUP BY device, week_number;


select
100.0 * sum(case when email_cat = 'email_opened' then 1 else 0 end)
 /sum(case when email_cat = 'email_sent' then 1 else 0 end)
as email_opening_rate,
100.0 * sum(case when email_cat = 'email_clicked' then 1 else 0 end)
 /sum(case when email_cat = 'email_sent' then 1 else 0 end)
as email_clicking_rate
from
(
select *,
case when action in ('sent_weekly_digest', 'sent_reengagement_email')
 then 'email_sent'
 when action in ('email_open')
 then 'email_opened'
 when action in ('email_clickthrough')
 then 'email_clicked'
end as email_cat
from emailEvents
)a;


SELECT YEAR,WEEK_NUMBER,NUM_USERS,SUM(NUM_USERS)OVER(ORDER BY YEAR,WEEK_NUMBER ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)AS CUMULATIVE_ACTIVE_USERS  FROM  (  SELECT  EXTRACT(YEAR  FROM ACTIVATED_AT  )  AS  YEAR,  EXTRACT(WEEK  FROM  ACTIVATED_AT)AS WEEK_NUMBER, COUNT(DISTINCT USER_ID) AS NUM_USERS  
FROM USERS  
WHERE STATE='ACTIVE'  
GROUP BY YEAR,WEEK_NUMBER  
ORDER BY YEAR,WEEK_NUMBER)A;

select day(created_at) as day,
count(*) as all_users,
count(activated_at) as activated_users
from users u
where created_at>='2013-03-01'
and created_at<'2023-03-31'
group by 1 order by 1;


SELECT
  cohort_week,
  COUNT(DISTINCT user_id) AS active_users
FROM
  (
    SELECT
      user_id,
      cohort_week,
      WEEK(date_created) AS week_created
    FROM
      users
    WHERE
      date_created BETWEEN '2023-08-01' AND '2023-08-31'
  )
GROUP BY
  cohort_week,
  week_created
ORDER BY
  cohort_week,
  week_created