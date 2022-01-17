{{ config(
  materialized = 'table'
)}}

select * from {{ ref('stg_1987') }}
 union all
select * from {{ ref('stg_1988') }}
 union all
select * from {{ ref('stg_1989') }}
 union all
select * from {{ ref('stg_1990') }}
 union all
select * from {{ ref('stg_1991') }}
 union all
select * from {{ ref('stg_1992') }}
 union all
select * from {{ ref('stg_1993') }}
 union all
select * from {{ ref('stg_1994') }}
 union all
select * from {{ ref('stg_1995') }}
 union all
select * from {{ ref('stg_1996') }}
 union all
select * from {{ ref('stg_1997') }}
 union all
select * from {{ ref('stg_1998') }}
 union all
select * from {{ ref('stg_1999') }}
 union all
select * from {{ ref('stg_2000') }}
 union all
select * from {{ ref('stg_2001') }}
 union all
select * from {{ ref('stg_2002') }}
 union all
select * from {{ ref('stg_2003') }}
 union all
select * from {{ ref('stg_2004') }}
 union all
select * from {{ ref('stg_2005') }}
 union all
select * from {{ ref('stg_2006') }}
 union all
select * from {{ ref('stg_2007') }}
 union all
select * from {{ ref('stg_2008') }}
 union all
select * from {{ ref('stg_2009') }}
 union all
select * from {{ ref('stg_2010') }}
 union all
select * from {{ ref('stg_2011') }}
 union all
select * from {{ ref('stg_2012') }}
 union all
select * from {{ ref('stg_2013') }}
 union all
select * from {{ ref('stg_2014') }}
 union all
select * from {{ ref('stg_2015') }}