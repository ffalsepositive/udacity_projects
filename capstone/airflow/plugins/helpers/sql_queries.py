class SqlQueries:
    genres_insert = ("""
        SELECT distinct genre_id, 
                        name as genre_name
        FROM staging_genres
        WHERE genre_id IS NOT NULL
    """)
    
    production_companies_insert = ("""
        SELECT distinct production_companies_id as company_id, 
                        name as company_name
        FROM staging_production_companies
        WHERE company_id IS NOT NULL
    """)
    
    production_countries_insert = ("""
        SELECT distinct production_countries_abbv as country_abbv, 
                        name as country_name
        FROM staging_production_countries
        WHERE country_abbv IS NOT NULL
    """)
    
    spoken_languages_insert = ("""
        SELECT distinct spoken_languages_abbv as lang_abbv, 
                        name as lang_name
        FROM staging_spoken_languages
        WHERE lang_abbv IS NOT NULL
    """)

    
    release_dates_insert = ("""
        SELECT distinct release_date, 
                        release_year, 
                        release_month,
                        extract(day from release_date) as release_day,
                        extract(week from release_date) as release_week,
                        extract(dayofweek from release_date) as release_weekday
        FROM staging_movies
        WHERE release_date IS NOT NULL
    """)

    movies_insert = ("""
        SELECT distinct movie_id,
                        imdb_id,
                        budget,
                        adult,
                        homepage,
                        original_language,
                        original_title,
                        title,
                        overview,
                        popularity,
                        runtime,
                        revenue,
                        status,
                        tagline,
                        video,
                        vote_average,
                        vote_count
        FROM staging_movies
        WHERE movie_id IS NOT NULL
    """)

    rating_times_insert = ("""
        SELECT distinct rating_time, 
                        rating_year, 
                        rating_month,
                        extract(day from rating_time) as rating_day,
                        extract(day from rating_time) as rating_hour,
                        extract(week from rating_time) as rating_week,
                        extract(dayofweek from rating_time) as rating_weekday
        FROM staging_ratings
        WHERE rating_time IS NOT NULL
    """)

    ratings_insert = ("""
        SELECT distinct user_id, 
                        movie_id, 
                        rating_time,
                        rating
        FROM staging_ratings
        WHERE user_id IS NOT NULL
        AND movie_id IS NOT NULL
        AND rating_time IS NOT NULL
    """)
    
    user_movie_ratings_insert = ("""
        SELECT distinct t1.user_id, 
                        t1.movie_id,
                        t1.rating_time,
                        t2.release_date,
                        t2.genre_id_1,
                        t2.genre_id_2,
                        t2.prod_company_id_1,
                        t2.prod_company_id_2,
                        t2.prod_country_abbv_1,
                        t2.prod_country_abbv_2,
                        t2.spoken_lang_abbv_1,
                        t2.spoken_lang_abbv_2
         FROM staging_ratings t1
         LEFT JOIN
         (
            SELECT movie_id,
                   release_date,
                   split_part(genre_ids, ',', 1) as genre_id_1,
                   split_part(genre_ids, ',', 2) as genre_id_2,
                   split_part(production_companies_ids, ',', 1) as prod_company_id_1,
                   split_part(production_companies_ids, ',', 2) as prod_company_id_2,
                   split_part(production_countries_abbvs, ',', 1) as prod_country_abbv_1,
                   split_part(production_countries_abbvs, ',', 2) as prod_country_abbv_2,
                   split_part(spoken_languages_abbvs, ',', 1) as spoken_lang_abbv_1,
                   split_part(spoken_languages_abbvs, ',', 2) as spoken_lang_abbv_2
            FROM staging_movies
         ) t2
         ON t1.movie_id = t2.movie_id
    """)