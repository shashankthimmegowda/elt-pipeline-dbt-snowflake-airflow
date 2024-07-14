with source as (
    select
        raw_data,
        loaded_at
    from {{ source('raw_weather', 'CURRENT_WEATHER') }}
),

parsed as (
    select
        raw_data:city_name::varchar               as city_name,
        raw_data:latitude::float                  as latitude,
        raw_data:longitude::float                 as longitude,
        raw_data:temperature_c::float             as temperature_c,
        raw_data:feels_like_c::float              as feels_like_c,
        raw_data:temp_min_c::float                as temp_min_c,
        raw_data:temp_max_c::float                as temp_max_c,
        raw_data:pressure_hpa::integer            as pressure_hpa,
        raw_data:humidity_pct::integer            as humidity_pct,
        raw_data:visibility_m::integer            as visibility_m,
        raw_data:wind_speed_ms::float             as wind_speed_ms,
        raw_data:wind_deg::integer                as wind_deg,
        raw_data:wind_gust_ms::float              as wind_gust_ms,
        raw_data:clouds_pct::integer              as clouds_pct,
        raw_data:weather_main::varchar            as weather_condition,
        raw_data:weather_description::varchar     as weather_description,
        raw_data:rain_1h_mm::float                as rain_1h_mm,
        raw_data:snow_1h_mm::float                as snow_1h_mm,
        raw_data:sunrise_utc::timestamp_tz        as sunrise_at,
        raw_data:sunset_utc::timestamp_tz         as sunset_at,
        raw_data:observation_dt_utc::timestamp_tz as observed_at,
        raw_data:extracted_at::timestamp_tz       as extracted_at,
        loaded_at,
        row_number() over (
            partition by raw_data:city_name::varchar,
                         date_trunc('hour', raw_data:observation_dt_utc::timestamp_tz)
            order by loaded_at desc
        ) as _row_num
    from source
)

select * from parsed
where _row_num = 1
