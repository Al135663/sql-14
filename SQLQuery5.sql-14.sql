----tsql 14----

----Using PIVOT to Transform Data

-- Using a Common Table Expression (CTE) to simplify the PIVOT operation

WITH RuntimeByYearGenre as (
select TRY_CAST(m.release_date AS DATE) as ValidReleaseDate, g.genre_id,
AVG(CAST(m.runtime as float)) as AvgRuntime  -- Ensuring runtime is cast to a float for averaging
FROM movie as m
INNER JOIN movie_genres mg on m.movie_id = mg.movie_id
INNER JOIN genre as g on mg.genre_id = g.genre_id
WHERE m.runtime is not null  
AND TRY_CAST(m.release_date AS DATE) IS NOT NULL  
GROUP BY TRY_CAST(m.release_date AS DATE), g.genre_id)

-- Executing the PIVOT operation
select
    genre_id,
    [2019], 
    [2020], 
    [2021], 
    [2022], 
    [2023]  
from(SELECT YEAR(ValidReleaseDate) as ReleaseYear,genre_id,AvgRuntime
from RuntimeByYearGenre) as SourceQuery
PIVOT(AVG(AvgRuntime) 
for ReleaseYear IN ([2019], [2020], [2021], [2022], [2023])) as PivotedTable;


-- using cte to aggregate the count of movies by genre and status
with statusgenrecounts as (select
g.genre_name, m.movie_status,count(m.movie_id) as moviecount
from movie as m
join movie_genres mg on m.movie_id = mg.movie_id
join genre g on mg.genre_id = g.genre_id
group by g.genre_name,m.movie_status)

-- pivoting the data to show the counts of movies in each status by genre
select genre_name,[released], [post production],[in production]
from(select genre_name, movie_status, moviecount
from statusgenrecounts) as sourcetable
pivot( sum(moviecount)
for movie_status in ([released], [post production], [in production])) as pivotedtable;


---Using UNPIVOT to Reverse Pivoted Data 
with statusgenrecounts as (select
g.genre_name,m.movie_status, count(m.movie_id) as moviecount
from movie as m
join movie_genres mg on m.movie_id = mg.movie_id
join genre g on mg.genre_id = g.genre_id
group by g.genre_name, m.movie_status),

-- Pivoting the data to show the counts of movies in each status by genre
pivotedtable as (select genre_name,[released], [post production], [in production]
from(select genre_name,movie_status, moviecount
from statusgenrecounts) as sourcetable
pivot(sum(moviecount)
for movie_status in ([released], [post production], [in production])) as pivoted)

-- Unpivoting the data to reverse the status counts back to row format
select genre_name,movie_status,moviecount
from pivotedtable
unpivot( moviecount for movie_status in ([released], [post production], [in production])) as unpvt;

--Aggregating Data with GROUPING SETS

select coalesce(g.genre_name, 'Total') as genre,
-- used COALESCE to return non-null value
coalesce(pc.company_name, 'All Companies') as company_name,
sum(m.revenue) as total_revenue,
avg(m.vote_average) as average_vote
from movie as m
join movie_genres as mg on m.movie_id = mg.movie_id
join genre as g on mg.genre_id = g.genre_id
join movie_company as mc on m.movie_id = mc.movie_id
join production_company as pc on mc.company_id = pc.company_id
group by grouping sets (
(g.genre_name),(pc.company_name),());

--Handling NULLs in PIVOT and GROUPING Queries

WITH RevenueData AS (
SELECT pc.company_name, m.movie_status,
COALESCE(m.revenue, 0) AS revenue -- Replace NULL revenue with zero
FROM movie as m
LEFT JOIN movie_company as mc ON m.movie_id = mc.movie_id
LEFT JOIN production_company as pc ON mc.company_id = pc.company_id
WHERE m.movie_status IS NOT NULL)
-- applying PIVOT to summarize revenue by movie status 
SELECT company_name,[released],[in production],[post production]
FROM RevenueData
PIVOT (SUM(revenue) FOR movie_status IN ([released], [in production], [post production])) AS PivotTable
ORDER BY company_name;


---Using CUBE for Multidimensional Aggregation

select
    coalesce(g.genre_name, 'All Genres') as Genre,
    coalesce(cast(pc.country_id as varchar(255)), 'All Countries') as Country,
    coalesce(m.movie_status, 'All Statuses') as Status,
    sum(m.revenue) as Total_Revenue,
    avg(m.vote_average) as Average_Vote
from movie as m
left join movie_genres as mg on m.movie_id = mg.movie_id
left join genre as g on mg.genre_id = g.genre_id
left join production_country as pc on m.movie_id = pc.movie_id
group by cube (g.genre_name, pc.country_id, m.movie_status)
order by Genre, Country,Status;

----Combining GROUPING SETS, ROLLUP, and CUBE
select
    coalesce(g.genre_name, 'Total Genre') as genre,
    coalesce(cast(pc.country_id as varchar(255)), 'Total Country') as country,
    coalesce(m.movie_status, 'Total Status') as status,
    sum(m.revenue) as total_revenue,
    avg(m.vote_average) as average_vote
from movie as m
left join movie_genres as mg on m.movie_id = mg.movie_id
left join genre as g on mg.genre_id = g.genre_id
left join production_country as pc on m.movie_id = pc.movie_id
group by grouping sets (
rollup (g.genre_name, pc.country_id),
cube (m.movie_status))
order by genre, country, status;

--Performance Tuning in Pivot and Grouping Queries
select
    g.genre_name,
    pc.country_id,
    sum(m.revenue) as total_revenue
from movie as m
inner join movie_genres as mg on m.movie_id = mg.movie_id
inner join genre as g on mg.genre_id = g.genre_id
inner join production_country as pc on m.movie_id = pc.movie_id
group by g.genre_name, pc.country_id;

select 
    genre_name,
    [1] as Country_1_Revenue,
    [2] as Country_2_Revenue,
    [3] as Country_3_Revenue
from (select g.genre_name, pc.country_id, sum(m.revenue) as total_revenue
     from movie m
     inner join movie_genres mg on m.movie_id = mg.movie_id
     inner join genre g on mg.genre_id = g.genre_id
     inner join production_country pc on m.movie_id = pc.movie_id
     group by g.genre_name, pc.country_id) as source_data
pivot
    (sum(total_revenue) 
        for country_id in ([1], [2], [3])) as pivot_table;

