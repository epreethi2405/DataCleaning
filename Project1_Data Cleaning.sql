-- Overview
-- This project focuses on data cleaning techniques applied to a dataset using MySQL Workbench. 

--  https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- The goal is to prepare the data for further analysis by identifying and addressing common data quality issues such as duplicates, missing values, and inconsistencies.
-- data cleaning involves 4 steps
-- deleting duplicates
-- standordizing data
-- repopulating null or blank values
-- deleting rows or columns if required

-- In this project, we utilize MySQL Workbench to perform various data cleaning operations on a dataset.
-- Tools and Technologies
-- MySQL Workbench: For database management and executing SQL queries.
-- SQL: For data manipulation and cleaning tasks.

-- Shouldn't make modifications to raw dataset so create new database with same dataset

create table staging_layoffs
like layoffs;

select * from staging_layoffs;

insert into staging_layoffs
select * from layoffs;

-- first step: Deleting Duplicates
-- with the rowno we can delete duplicate

with duplicate_cte as(
select *, row_number() over( partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country,funds_raised_millions ) as rowno from 
staging_layoffs
)
select * from duplicate_cte;

-- Delete can't be performed on CTE, beacuse they are not are not updatable

-- so we will create new table with new column row number

CREATE TABLE `staging2_layoffs` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_no` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into staging2_layoffs
select *, row_number() over( partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country,funds_raised_millions ) as rowno from 
staging_layoffs;

select * from staging2_layoffs ;

select * from staging2_layoffs where row_no>1;

delete from staging2_layoffs where row_no>1;

-- second step standordizing data involve fixing issues

select * from staging2_layoffs;

select distinct company from
staging2_layoffs;

select distinct company, trim(company) from
staging2_layoffs;

update staging2_layoffs
set company=trim(company);

select distinct industry from 
staging2_layoffs order by 1;

select distinct industry from 
staging2_layoffs 
where industry like 'Crypto%';
 
 update staging2_layoffs
 set industry='Crypto'
 where industry like 'Crypto%';
 
 select distinct country 
 from staging2_layoffs
 order by 1;
 
 select distinct country , trim(trailing '.' from country)
 from staging2_layoffs
where country like 'United States%';

update staging2_layoffs
set country= trim(trailing '.' from country)
where country like 'United States%';

select `date` , str_to_date(`date`, '%m/%d/%Y')
from staging2_layoffs;

update staging2_layoffs
set `date`= str_to_date(`date`, '%m/%d/%Y');

-- to change data type of column

Alter table staging2_layoffs
modify column `date` date;

-- Third Step: updating null or blank values

select * from 
staging2_layoffs where 
industry IS NULL or
industry='';

select * from 
staging2_layoffs where 
company='Airbnb';

update staging2_layoffs
set industry=null 
where industry='';

update staging2_layoffs t1
join staging2_layoffs t2 on 
t1.company=t2.company and t1.location=t2.location
set t1.industry=t2.industry
where t1.industry is null and t2.industry is not null;

select * from staging2_layoffs t1
join staging2_layoffs t2 on 
t1.company=t2.company and t1.location=t2.location
where t1.industry is null and t2.industry is not null;

select * from 
staging2_layoffs where 
industry IS NULL or
industry='';

-- Fourth Step:  deleting unneccessary rows and cols

select * from 
staging2_layoffs where 
percentage_laid_off is null and total_laid_off is null;

delete from 
staging2_layoffs where 
percentage_laid_off is null and total_laid_off is null;

alter table staging2_layoffs
drop column row_no; 

select * from staging2_layoffs;