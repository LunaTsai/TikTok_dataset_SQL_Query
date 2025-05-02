-- Append Follower_Following_Ratio attribute to an existing table
DROP TABLE IF EXISTS TABLE1_n_TABLE2_n_TABLE3_n_TABLE4_n_TABLE5_n_TABLE6_20250105;

CREATE TABLE TABLE1_n_TABLE2_n_TABLE3_n_TABLE4_n_TABLE5_n_TABLE6_20250105 AS
SELECT DISTINCT 
    t5.`user.uniqueId`,
    t5.`nickname`,
    t5.`user.create_time`,
    t5.`user.gender`,
    t5.`SOURCE`,
    t5.`days_difference`,
    t5.`user.region`,
    t5.`stats.followingCount`,
    t5.`stats.followerCount`,
    t5.`stats.diggCount`,
    MAX(r.`follower_to_following_ratio`) AS `follower_to_following_ratio`
FROM 
    TABLE1_n_TABLE2_n_TABLE3_n_TABLE4_n_TABLE520250105 t5
LEFT JOIN 
    User_Follower_Following_Ratio r
    ON t5.`user.uniqueId` = r.`user.uniqueId`
GROUP BY 
    `user.uniqueId`,
    `nickname`,
    `user.create_time`,
    `user.gender`,
    `SOURCE`,
    `days_difference`,
    `user.region`,
    `stats.followingCount`,
    `stats.followerCount`,
    `stats.diggCount`;


-- - Append user favoriting count ratio to existing table
CREATE TABLE TABLE1_n_TABLE2020250109 AS
SELECT DISTINCT 
    t.`user.uniqueId`,
    t.`nickname`,
    t.`user.create_time`,
    t.`user.gender`,
    t.`SOURCE`,
    t.`days_difference`,
    t.`user.region`,
    t.`stats.followingCount`,
    t.`stats.followerCount`,
    t.`stats.diggCount`,
    t.`follower_to_following_ratio`,
    t.`stats.videoCount`,
    t.`stats.heartCount`,
    t.`user.avatar_168x168.uri`,
    t.`avg_response_time`,
    t.`max_weekly_avg_comments_2023Q4`,
    t.`max_weekly_avg_comments_2024Q1`,
    t.`difference_in_weekly_avg_comments`,
    t.`comment_and_post_count_ratio`,
    t.`user.search_user_desc`,
    t.`is_default_profile_name`,
    t.`search_user_desc_contains_keyword`,
    t.`user.signature`,
    t.`user_signature_with_without_key_words`,
    t.`words_before_slash`,
    t.`if_account_linked_with_other_social_media`,
    t.`dominant_language_consistency_score`,
    MAX(f.`user.favoriting_count`) AS `user.favoriting_count`
FROM 
    TABLE1_n_TABLE1920250109 AS t
LEFT JOIN 
    favourite_counts AS f
    ON t.`user.uniqueId` = f.`user.uniqueId`
GROUP BY 
    t.`user.uniqueId`,
    t.`nickname`,
    t.`user.create_time`,
    t.`user.gender`,
    t.`SOURCE`,
    t.`days_difference`,
    t.`user.region`,
    t.`stats.followingCount`,
    t.`stats.followerCount`,
    t.`stats.diggCount`,
    t.`follower_to_following_ratio`,
    t.`stats.videoCount`,
    t.`stats.heartCount`,
    t.`user.avatar_168x168.uri`,
    t.`avg_response_time`,
    t.`max_weekly_avg_comments_2023Q4`,
    t.`max_weekly_avg_comments_2024Q1`,
    t.`difference_in_weekly_avg_comments`,
    t.`comment_and_post_count_ratio`,
    t.`user.search_user_desc`,
    t.`is_default_profile_name`,
    t.`search_user_desc_contains_keyword`,
    t.`user.signature`,
    t.`user_signature_with_without_key_words`,
    t.`words_before_slash`,
    t.`if_account_linked_with_other_social_media`,
    t.`dominant_language_consistency_score`;


-- Consolidate all intermediate tables and attributes into a single final table
CREATE TABLE FinalTable20250128 AS
WITH RankedRows AS (
    SELECT 
        *,
        -- Count the number of NULL values per row
        (
            CASE WHEN `nickname` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.create_time` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.gender` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `SOURCE` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `days_difference` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.region` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `stats.followingCount` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `stats.followerCount` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `stats.diggCount` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `follower_to_following_ratio` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `stats.videoCount` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `stats.heartCount` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.avatar_168x168.uri` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `avg_response_time` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `max_weekly_avg_comments_2023Q4` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `max_weekly_avg_comments_2024Q1` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `difference_in_weekly_avg_comments` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `comment_and_post_count_ratio` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.search_user_desc` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `is_default_profile_name` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `search_user_desc_contains_keyword` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.signature` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user_signature_with_without_key_words` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `words_before_slash` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `if_account_linked_with_other_social_media` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `dominant_language_consistency_score` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.favoriting_count` IS NULL THEN 1 ELSE 0 END +
            CASE WHEN `user.hide_search` IS NULL THEN 1 ELSE 0 END
        ) AS null_count,

        -- Rank rows by fewest NULLs, then by create time
        ROW_NUMBER() OVER (
            PARTITION BY `user.uniqueId`
            ORDER BY
                (
                    CASE WHEN `nickname` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.create_time` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.gender` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `SOURCE` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `days_difference` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.region` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `stats.followingCount` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `stats.followerCount` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `stats.diggCount` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `follower_to_following_ratio` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `stats.videoCount` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `stats.heartCount` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.avatar_168x168.uri` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `avg_response_time` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `max_weekly_avg_comments_2023Q4` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `max_weekly_avg_comments_2024Q1` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `difference_in_weekly_avg_comments` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `comment_and_post_count_ratio` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.search_user_desc` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `is_default_profile_name` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `search_user_desc_contains_keyword` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.signature` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user_signature_with_without_key_words` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `words_before_slash` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `if_account_linked_with_other_social_media` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `dominant_language_consistency_score` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.favoriting_count` IS NULL THEN 1 ELSE 0 END +
                    CASE WHEN `user.hide_search` IS NULL THEN 1 ELSE 0 END
                ) ASC,
                `user.create_time` ASC
        ) AS `rank`
    FROM `TABLE1_n_TABLE2120250109`
)

-- Keep only the best record per user
SELECT *
FROM RankedRows
WHERE `rank` = 1;
