select *
from {{ ref('int_weather_unified') }}
where (is_raining = true and (rain_mm is null or rain_mm = 0))
   or (is_raining = false and rain_mm > 0)
   or (is_snowing = true and (snow_mm is null or snow_mm = 0))
   or (is_snowing = false and snow_mm > 0)