
select count(*) from T1 where K1 IS NULL 
select count(*) from T1 where K2 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2 FROM T1 ) as result;
SELECT COUNT(*) FROM T1;
select count(*) from T2 where K1 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1 FROM T2 ) as result;
SELECT COUNT(*) FROM T2;
select count(*) from (select A,K1 from T2 GROUP BY A,K1) as result; 
select count(*) from (select DISTINCT A from T2) as result; 
select count(*) from T3 where K1 IS NULL 
select count(*) from T3 where K2 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2 FROM T3 ) as result;
SELECT COUNT(*) FROM T3;
select count(*) from (select K1,D from T3 GROUP BY K1,D) as result; 
select count(*) from (select DISTINCT K1 from T3) as result; 
select count(*) from (select K1,E from T3 GROUP BY K1,E) as result; 
select count(*) from (select DISTINCT K1 from T3) as result; 
select count(*) from (select K2,D from T3 GROUP BY K2,D) as result; 
select count(*) from (select DISTINCT K2 from T3) as result; 
select count(*) from (select K2,E from T3 GROUP BY K2,E) as result; 
select count(*) from (select DISTINCT K2 from T3) as result; 
DROP TABLE IF EXISTS team14schema.T32
create table team14schema.T32 as (select K1,D from T3 GROUP BY K1,D);
DROP TABLE IF EXISTS team14schema.T31
create table team14schema.T31 as (select K1,K2,E from T3);
SELECT count(*) from T3;
SELECT count(*) from team14schema.T31 JOIN team14schema.T32 ON team14schema.T31.K1=team14schema.T32.K1
select count(*) from T4 where K1 IS NULL 
select count(*) from T4 where K2 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2 FROM T4 ) as result;
SELECT COUNT(*) FROM T4;
select count(*) from (select K1,D from T4 GROUP BY K1,D) as result; 
select count(*) from (select DISTINCT K1 from T4) as result; 
select count(*) from (select K1,E from T4 GROUP BY K1,E) as result; 
select count(*) from (select DISTINCT K1 from T4) as result; 
select count(*) from (select K2,D from T4 GROUP BY K2,D) as result; 
select count(*) from (select DISTINCT K2 from T4) as result; 
select count(*) from (select K2,E from T4 GROUP BY K2,E) as result; 
select count(*) from (select DISTINCT K2 from T4) as result; 
select count(*) from (select D,E from T4 GROUP BY D,E) as result; 
select count(*) from (select DISTINCT D from T4) as result; 
select count(*) from (select D,K1 from T4 GROUP BY D,K1) as result; 
select count(*) from (select DISTINCT D from T4) as result; 
select count(*) from (select D,E,K1 from T4 GROUP BY D,E,K1) as result; 
select count(*) from (select DISTINCT D,E from T4) as result; 
select count(*) from (select D,K2 from T4 GROUP BY D,K2) as result; 
select count(*) from (select DISTINCT D from T4) as result; 
select count(*) from (select D,E,K2 from T4 GROUP BY D,E,K2) as result; 
select count(*) from (select DISTINCT D,E from T4) as result; 
select count(*) from (select E,K1 from T4 GROUP BY E,K1) as result; 
select count(*) from (select DISTINCT E from T4) as result; 
select count(*) from (select E,K2 from T4 GROUP BY E,K2) as result; 
select count(*) from (select DISTINCT E from T4) as result; 
select count(*) from T50 where K1 IS NULL 
select count(*) from T50 where K2 IS NULL 
select count(*) from T50 where K3 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2,K3 FROM T50 ) as result;
SELECT COUNT(*) FROM T50;
select count(*) from (select K1,K2,A from T50 GROUP BY K1,K2,A) as result; 
select count(*) from (select DISTINCT K1,K2 from T50) as result; 
select count(*) from (select K1,K2,B from T50 GROUP BY K1,K2,B) as result; 
select count(*) from (select DISTINCT K1,K2 from T50) as result; 
select count(*) from (select K1,K3,A from T50 GROUP BY K1,K3,A) as result; 
select count(*) from (select DISTINCT K1,K3 from T50) as result; 
select count(*) from (select K1,K3,B from T50 GROUP BY K1,K3,B) as result; 
select count(*) from (select DISTINCT K1,K3 from T50) as result; 
select count(*) from (select K2,K3,A from T50 GROUP BY K2,K3,A) as result; 
select count(*) from (select DISTINCT K2,K3 from T50) as result; 
select count(*) from (select K2,K3,B from T50 GROUP BY K2,K3,B) as result; 
select count(*) from (select DISTINCT K2,K3 from T50) as result; 
select count(*) from (select K1,A from T50 GROUP BY K1,A) as result; 
select count(*) from (select DISTINCT K1 from T50) as result; 
select count(*) from (select K1,B from T50 GROUP BY K1,B) as result; 
select count(*) from (select DISTINCT K1 from T50) as result; 
select count(*) from (select K2,A from T50 GROUP BY K2,A) as result; 
select count(*) from (select DISTINCT K2 from T50) as result; 
select count(*) from (select K2,B from T50 GROUP BY K2,B) as result; 
select count(*) from (select DISTINCT K2 from T50) as result; 
select count(*) from (select K3,A from T50 GROUP BY K3,A) as result; 
select count(*) from (select DISTINCT K3 from T50) as result; 
select count(*) from (select K3,B from T50 GROUP BY K3,B) as result; 
select count(*) from (select DISTINCT K3 from T50) as result; 
DROP TABLE IF EXISTS team14schema.T502
create table team14schema.T502 as (select K1,K2 from A GROUP BY T50,K1);
DROP TABLE IF EXISTS team14schema.T503
create table team14schema.T503 as (select K1,K2 from B GROUP BY T50,K1);
DROP TABLE IF EXISTS team14schema.T501
create table team14schema.T501 as (select K1,K2,K3 from T50);
SELECT count(*) from T50;
SELECT count(*) from team14schema.T501 JOIN team14schema.T503 ON team14schema.T501.K1=team14schema.T503.K1 and team14schema.T501.K2=team14schema.T503.K2 JOIN team14schema.T502 ON team14schema.T502.K1=team14schema.T502.K1 and team14schema.T502.K2=team14schema.T502.K2
select count(*) from T6 where K1 IS NULL 
select count(*) from T6 where K2 IS NULL 
select count(*) from T6 where K3 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2,K3 FROM T6 ) as result;
SELECT COUNT(*) FROM T6;
select count(*) from (select K1,K2,A from T6 GROUP BY K1,K2,A) as result; 
select count(*) from (select DISTINCT K1,K2 from T6) as result; 
select count(*) from (select K1,K3,A from T6 GROUP BY K1,K3,A) as result; 
select count(*) from (select DISTINCT K1,K3 from T6) as result; 
select count(*) from (select K2,K3,A from T6 GROUP BY K2,K3,A) as result; 
select count(*) from (select DISTINCT K2,K3 from T6) as result; 
select count(*) from (select K1,A from T6 GROUP BY K1,A) as result; 
select count(*) from (select DISTINCT K1 from T6) as result; 
select count(*) from (select K2,A from T6 GROUP BY K2,A) as result; 
select count(*) from (select DISTINCT K2 from T6) as result; 
select count(*) from (select K3,A from T6 GROUP BY K3,A) as result; 
select count(*) from (select DISTINCT K3 from T6) as result; 
select count(*) from (select A,K1 from T6 GROUP BY A,K1) as result; 
select count(*) from (select DISTINCT A from T6) as result; 
select count(*) from (select A,K2 from T6 GROUP BY A,K2) as result; 
select count(*) from (select DISTINCT A from T6) as result; 
select count(*) from (select A,K3 from T6 GROUP BY A,K3) as result; 
select count(*) from (select DISTINCT A from T6) as result; 
select count(*) from S7 where K1 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1 FROM S7 ) as result;
SELECT COUNT(*) FROM S7;
select count(*) from (select A,B from S7 GROUP BY A,B) as result; 
select count(*) from (select DISTINCT A from S7) as result; 
select count(*) from (select A,B,C from S7 GROUP BY A,B,C) as result; 
select count(*) from (select DISTINCT A,B from S7) as result; 
select count(*) from (select A,C from S7 GROUP BY A,C) as result; 
select count(*) from (select DISTINCT A from S7) as result; 
select count(*) from (select B,C from S7 GROUP BY B,C) as result; 
select count(*) from (select DISTINCT B from S7) as result; 
select count(*) from (select A,K1 from S7 GROUP BY A,K1) as result; 
select count(*) from (select DISTINCT A from S7) as result; 
select count(*) from (select A,B,K1 from S7 GROUP BY A,B,K1) as result; 
select count(*) from (select DISTINCT A,B from S7) as result; 
select count(*) from (select A,B,C from K1 GROUP BY S7,A,B) as result; 
select count(*) from (select DISTINCT A,B,C from S7) as result; 
select count(*) from (select B,K1 from S7 GROUP BY B,K1) as result; 
select count(*) from (select DISTINCT B from S7) as result; 
select count(*) from (select B,C,K1 from S7 GROUP BY B,C,K1) as result; 
select count(*) from (select DISTINCT B,C from S7) as result; 
select count(*) from (select C,K1 from S7 GROUP BY C,K1) as result; 
select count(*) from (select DISTINCT C from S7) as result; 
select count(*) from T8 where K1 IS NULL 
select count(*) from T8 where K2 IS NULL 
select count(*) from T8 where K3 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2,K3 FROM T8 ) as result;
SELECT COUNT(*) FROM T8;
select count(*) from (select K1,K2,A from T8 GROUP BY K1,K2,A) as result; 
select count(*) from (select DISTINCT K1,K2 from T8) as result; 
select count(*) from (select K1,K2,B from T8 GROUP BY K1,K2,B) as result; 
select count(*) from (select DISTINCT K1,K2 from T8) as result; 
select count(*) from (select K1,K3,A from T8 GROUP BY K1,K3,A) as result; 
select count(*) from (select DISTINCT K1,K3 from T8) as result; 
select count(*) from (select K1,K3,B from T8 GROUP BY K1,K3,B) as result; 
select count(*) from (select DISTINCT K1,K3 from T8) as result; 
select count(*) from (select K2,K3,A from T8 GROUP BY K2,K3,A) as result; 
select count(*) from (select DISTINCT K2,K3 from T8) as result; 
select count(*) from (select K2,K3,B from T8 GROUP BY K2,K3,B) as result; 
select count(*) from (select DISTINCT K2,K3 from T8) as result; 
select count(*) from (select K1,A from T8 GROUP BY K1,A) as result; 
select count(*) from (select DISTINCT K1 from T8) as result; 
select count(*) from (select K1,B from T8 GROUP BY K1,B) as result; 
select count(*) from (select DISTINCT K1 from T8) as result; 
select count(*) from (select K2,A from T8 GROUP BY K2,A) as result; 
select count(*) from (select DISTINCT K2 from T8) as result; 
select count(*) from (select K2,B from T8 GROUP BY K2,B) as result; 
select count(*) from (select DISTINCT K2 from T8) as result; 
select count(*) from (select K3,A from T8 GROUP BY K3,A) as result; 
select count(*) from (select DISTINCT K3 from T8) as result; 
select count(*) from (select K3,B from T8 GROUP BY K3,B) as result; 
select count(*) from (select DISTINCT K3 from T8) as result; 
select count(*) from (select A,B from T8 GROUP BY A,B) as result; 
select count(*) from (select DISTINCT A from T8) as result; 
DROP TABLE IF EXISTS team14schema.T82
create table team14schema.T82 as (select A,B from T8 GROUP BY A,B)
DROP TABLE IF EXISTS team14schema.T81
create table team14schema.T81 as (select K1,K2,K3,A from T8);
SELECT count(*) from T8;
SELECT count(*) from team14schema.T81 JOIN team14schema.T82 ON team14schema.T81.A=team14schema.T82.A
select count(*) from T9 where K1 IS NULL 
select count(*) from T9 where K2 IS NULL 
select count(*) from T9 where K3 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2,K3 FROM T9 ) as result;
SELECT COUNT(*) FROM T9;
select count(*) from (select K1,K2,A from T9 GROUP BY K1,K2,A) as result; 
select count(*) from (select DISTINCT K1,K2 from T9) as result; 
select count(*) from (select K1,K2,B from T9 GROUP BY K1,K2,B) as result; 
select count(*) from (select DISTINCT K1,K2 from T9) as result; 
select count(*) from (select K1,K2,C from T9 GROUP BY K1,K2,C) as result; 
select count(*) from (select DISTINCT K1,K2 from T9) as result; 
select count(*) from (select K1,K3,A from T9 GROUP BY K1,K3,A) as result; 
select count(*) from (select DISTINCT K1,K3 from T9) as result; 
select count(*) from (select K1,K3,B from T9 GROUP BY K1,K3,B) as result; 
select count(*) from (select DISTINCT K1,K3 from T9) as result; 
select count(*) from (select K1,K3,C from T9 GROUP BY K1,K3,C) as result; 
select count(*) from (select DISTINCT K1,K3 from T9) as result; 
select count(*) from (select K2,K3,A from T9 GROUP BY K2,K3,A) as result; 
select count(*) from (select DISTINCT K2,K3 from T9) as result; 
select count(*) from (select K2,K3,B from T9 GROUP BY K2,K3,B) as result; 
select count(*) from (select DISTINCT K2,K3 from T9) as result; 
select count(*) from (select K2,K3,C from T9 GROUP BY K2,K3,C) as result; 
select count(*) from (select DISTINCT K2,K3 from T9) as result; 
select count(*) from (select K1,A from T9 GROUP BY K1,A) as result; 
select count(*) from (select DISTINCT K1 from T9) as result; 
select count(*) from (select K1,B from T9 GROUP BY K1,B) as result; 
select count(*) from (select DISTINCT K1 from T9) as result; 
select count(*) from (select K1,C from T9 GROUP BY K1,C) as result; 
select count(*) from (select DISTINCT K1 from T9) as result; 
select count(*) from (select K2,A from T9 GROUP BY K2,A) as result; 
select count(*) from (select DISTINCT K2 from T9) as result; 
select count(*) from (select K2,B from T9 GROUP BY K2,B) as result; 
select count(*) from (select DISTINCT K2 from T9) as result; 
select count(*) from (select K2,C from T9 GROUP BY K2,C) as result; 
select count(*) from (select DISTINCT K2 from T9) as result; 
select count(*) from (select K3,A from T9 GROUP BY K3,A) as result; 
select count(*) from (select DISTINCT K3 from T9) as result; 
select count(*) from (select K3,B from T9 GROUP BY K3,B) as result; 
select count(*) from (select DISTINCT K3 from T9) as result; 
select count(*) from (select K3,C from T9 GROUP BY K3,C) as result; 
select count(*) from (select DISTINCT K3 from T9) as result; 
select count(*) from (select A,B from T9 GROUP BY A,B) as result; 
select count(*) from (select DISTINCT A from T9) as result; 
select count(*) from (select A,B,C from T9 GROUP BY A,B,C) as result; 
select count(*) from (select DISTINCT A,B from T9) as result; 
select count(*) from (select B,C from T9 GROUP BY B,C) as result; 
select count(*) from (select DISTINCT B from T9) as result; 
DROP TABLE IF EXISTS team14schema.T92
create table team14schema.T92 as (select A,B,C from T9 GROUP BY A,B,C)
DROP TABLE IF EXISTS team14schema.T91
create table team14schema.T91 as (select K1,K2,K3,A,B from T9);
SELECT count(*) from T9;
SELECT count(*) from team14schema.T91 JOIN team14schema.T92 ON team14schema.T91.A=team14schema.T92.A and team14schema.T91.B=team14schema.T92.B
select count(*) from G10 where K1 IS NULL 
select count(*) from G10 where K2 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1,K2 FROM G10 ) as result;
SELECT COUNT(*) FROM G10;
select count(*) from (select K1,A from G10 GROUP BY K1,A) as result; 
select count(*) from (select DISTINCT K1 from G10) as result; 
select count(*) from (select K2,A from G10 GROUP BY K2,A) as result; 
select count(*) from (select DISTINCT K2 from G10) as result; 
select count(*) from (select A,K1 from G10 GROUP BY A,K1) as result; 
select count(*) from (select DISTINCT A from G10) as result; 
select count(*) from (select A,K2 from G10 GROUP BY A,K2) as result; 
select count(*) from (select DISTINCT A from G10) as result; 
select count(*) from R7 where K1 IS NULL 
SELECT COUNT(*) from (select DISTINCT K1 FROM R7 ) as result;
SELECT COUNT(*) FROM R7;
select count(*) from (select A,B from R7 GROUP BY A,B) as result; 
select count(*) from (select DISTINCT A from R7) as result; 
select count(*) from (select B,C from R7 GROUP BY B,C) as result; 
select count(*) from (select DISTINCT B from R7) as result; 
DROP TABLE IF EXISTS team14schema.R72
create table team14schema.R72 as (select A,B from R7 GROUP BY A,B)
DROP TABLE IF EXISTS team14schema.R73
create table team14schema.R73 as (select B,C from R7 GROUP BY B,C)
DROP TABLE IF EXISTS team14schema.R71
create table team14schema.R71 as (select K1,A from R7);
SELECT count(*) from R7;
SELECT count(*) from team14schema.R71 JOIN team14schema.R72 ON team14schema.R71.A=team14schema.R72.A JOIN team14schema.R73 ON team14schema.R72.B=team14schema.R73.B