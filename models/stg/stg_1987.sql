{{ config(
  materialized = 'table'
)}}

select value[0]::number as STN_CODE
       ,case when split(value[1]::varchar,'-')[1] is not null
             then DATE_FROM_PARTS(1987
                                  ,split(value[1]::varchar,'-')[1]
                                  ,split(value[1]::varchar,'-')[0]
                                 ) 
             else DATE_FROM_PARTS(split(value[1]::varchar,'/')[2]
                                  ,split(value[1]::varchar,'/')[0]
                                  ,split(value[1]::varchar,'/')[1]
                                 ) 
         end as DT
       ,value[2]::varchar as ST
       ,value[3]::varchar as CTY
       ,NULL AS LOCATION
       ,value[4]::varchar as AGNCY
       ,value[5]::varchar as LOC_TYP
       ,case when value[6]::VARCHAR = 'NA'
             then NULL
             else value[6]::DECIMAL(9,3)
         end as SO2
       ,case when value[7]::VARCHAR = 'NA'
             then NULL
             else value[7]::DECIMAL(9,3)
         end as NO2
       ,case when value[8]::VARCHAR = 'NA'
             then NULL
             else value[8]::DECIMAL(9,3)
         end as RSPM_PM10
       ,case when value[9]::VARCHAR = 'NA'
             then NULL
             else value[9]::DECIMAL(9,3)
         end as SPM,value
FROM 
  {{ source('BHAVIK_S3','MH_AQI')}}
  ,LATERAL FLATTEN(data)
 where _file = 'MH_1987.json'