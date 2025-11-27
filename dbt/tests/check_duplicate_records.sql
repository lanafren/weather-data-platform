select record_time_utc, record_type, count(*)
from {{ ref('int_weather_unified') }}
group by record_time_utc, record_type
having count(*) > 1