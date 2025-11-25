select *
from {{ ref('int_weather_unified') }}
where temp < -90 or temp > 60
   or feels_like < -100 or feels_like > 70
   or wind_speed < 0 or wind_speed > 100
   or wind_gust < 0 or wind_gust > 120