select *
from {{ ref('int_weather_unified')}}
where TIMESTAMP(fetched_at) > CURRENT_TIMESTAMP()
