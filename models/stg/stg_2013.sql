{{ config(
  materialized = 'table'
)}}

select value[0]::number as STN_CODE
       ,case when split(value[1]::varchar,'-')[1] is not null
             then DATE_FROM_PARTS(2013
                                  ,split(value[1]::varchar,'-')[1]
                                  ,split(value[1]::varchar,'-')[0]
                                 ) 
             else DATE_FROM_PARTS(split(value[1]::varchar,'/')[2]
                                  ,split(value[1]::varchar,'/')[1]
                                  ,split(value[1]::varchar,'/')[0]
                                 ) 
         end as DT
       ,value[2]::varchar as ST
       ,value[3]::varchar as CTY
       ,value[4]::varchar as LOCATION
       ,value[5]::varchar as AGNCY
       ,value[6]::varchar as LOC_TYP
       ,case when value[7]::VARCHAR = 'NA'
             then NULL
             else value[7]::DECIMAL(9,3)
         end as SO2
       ,case when value[8]::VARCHAR = 'NA'
             then NULL
             else value[8]::DECIMAL(9,3)
         end as NO2
       ,case when value[9]::VARCHAR = 'NA'
             then NULL
             else value[9]::DECIMAL(9,3)
         end as RSPM_PM10
       ,case when value[10]::VARCHAR = 'NA'
             then NULL
             else value[10]::DECIMAL(9,3)
         end as SPM ,value
FROM 
  {{ source('BHAVIK_S3','MH_AQI')}},
  LATERAL FLATTEN(data)
 where _file = 'MH_2013.json'
 group by 1,2,3,4,5,6,7,8,9,10,11,12