{{ config(materialized='table') }}

SELECT 
    -- identifiers
    name,
    album,
    artists,
    explicit,
    danceability,
    energy,
    {{ get_key_description('key') }} AS key_description, 
    loudness,
    {{ get_modality_description('mode') }} AS modality_description, 
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    duration_s,
    year_date,
     CASE
        WHEN year_date BETWEEN '1900-01-01 00:00:00 UTC' AND '1909-12-31 00:00:00 UTC' THEN 'Naughts'
        WHEN year_date BETWEEN '1910-01-01 00:00:00 UTC' AND '1919-12-31 00:00:00 UTC' THEN 'Tens'
        WHEN year_date BETWEEN '1920-01-01 00:00:00 UTC' AND '1929-12-31 00:00:00 UTC' THEN 'Roaring Twenties'
        WHEN year_date BETWEEN '1930-01-01 00:00:00 UTC' AND '1939-12-31 00:00:00 UTC' THEN 'Dirty Thirties'
        WHEN year_date BETWEEN '1940-01-01 00:00:00 UTC' AND '1949-12-31 00:00:00 UTC' THEN 'Forties'
        WHEN year_date BETWEEN '1950-01-01 00:00:00 UTC' AND '1959-12-31 00:00:00 UTC' THEN 'Fabulous Fifties'
        WHEN year_date BETWEEN '1960-01-01 00:00:00 UTC' AND '1969-12-31 00:00:00 UTC' THEN 'Swinging Sixties'
        WHEN year_date BETWEEN '1970-01-01 00:00:00 UTC' AND '1979-12-31 00:00:00 UTC' THEN 'Seventies'
        WHEN year_date BETWEEN '1980-01-01 00:00:00 UTC' AND '1989-12-31 00:00:00 UTC' THEN 'Eighties'
        WHEN year_date BETWEEN '1990-01-01 00:00:00 UTC' AND '1999-12-31 00:00:00 UTC' THEN 'Nineties'
        WHEN year_date BETWEEN '2000-01-01 00:00:00 UTC' AND '2009-12-31 00:00:00 UTC' THEN 'Noughties'
        WHEN year_date BETWEEN '2010-01-01 00:00:00 UTC' AND '2019-12-31 00:00:00 UTC' THEN 'Teens'
        WHEN year_date = '2020-01-01 00:00:00 UTC' THEN '2020'
    END AS Decade,
    CASE
        WHEN valence > 0.5 THEN 'Happy'
        WHEN valence < 0.5 THEN 'Sad'
        ELSE 'Ambivalent'
    END AS Happy_Sad
        
FROM {{ source('spotify', 'spotify_one_point_two_million')}}



