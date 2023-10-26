--DATABASE
SET hive.metastore.warehouse.dir = /user/itv003722/warehouse;
USE comment_analysis;
show tables;

--Using the over-all rating fields display trend:
--1. Globally by company: Identify trends at 25%, 50%, 75%

select company,percentile(cast(overall_ratings as bigint), 0.25) as 25_perentage,percentile(cast(overall_ratings as bigint), 0.50) as 50_perentage,percentile(cast(overall_ratings as bigint), 0.75) as 75_perentage   from review
group by company;


---2. Globally by company per year: Identify trends at 25%, 50%, 75%

select company,year,percentile(cast(overall_ratings as bigint), 0.25) as 25_perentage,percentile(cast(overall_ratings as bigint), 0.50) as 50_perentage,percentile(cast(overall_ratings as bigint), 0.75) as 75_perentage   from review
group by company,year;

---3. By company by country (Identify trends for each company by country: Identify trends at 25%, 50%, 75%

select country,company,percentile(cast(overall_ratings as bigint), 0.25) as 25_perentage,percentile(cast(overall_ratings as bigint), 0.50) as 50_perentage,percentile(cast(overall_ratings as bigint), 0.75) as 75_perentage   from review
group by country,company;


--Overall-ratings Versus  Work-balance stars  Culture values stars  Career opportunities-stars Comp-benefit-stars Senior-management-stars
select overall_ratings,work_balance_stars,culture_values_stars,carrer_opportunities_stars,comp_benefit_stars,senior_mangemnet_stars
from review;

--Display the impact of job role on rating a company using the overall-ratings field by the company by year.

select job_title,company,year,overall_ratings from review
group by job_title,company,year,overall_ratings
order by overall_ratings desc

--a) Which corporation is worth working for
select company,count(overall_ratings) from review where overall_ratings=5 group by company;

--b) Classification of satisfied or unsatisfied employees?

select company , sum(satisfied) as satisfied,sum(unsatisfied) as unstatisfied from 
       ( select company, (case when overall_ratings>=3 then 1 else 0 end) as satisfied,
       (case when overall_ratings<3  then 1 else 0 end)  as unsatisfied from review
 )a group by a.company


