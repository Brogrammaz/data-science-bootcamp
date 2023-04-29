WITH recursive_cte AS (SELECT DISTINCT submission_date, hacker_id 
        FROM submissions
        WHERE submission_date = (select min(submission_date) FROM submissions)
        UNION
        SELECT  s.submission_date, s.hacker_id
        FROM submissions s
        JOIN cte on cte.hacker_id = s.hacker_id
        WHERE s.submission_date = (SELECT min(submission_date)
        FROM submissions WHERE submission_date > cte.submission_date)),
        unique_hackers AS 
        (SELECT submission_date, count(1) AS total_unique_hackers
        FROM cte GROUP BY submission_date),
        count_submissions AS 
        (SELECT submission_date, hacker_id, count(1) AS no_of_submissions
        FROM submissions GROUP BY submission_date, hacker_id ORDER BY 1),
        max_submissions AS 
        (SELECT submission_date, max(no_of_submissions) AS max_submissions
        FROM count_submissions GROUP BY submission_date),
        final_list AS
        (SELECT v.submission_date, min(v.hacker_id) AS hacker_id 
        FROM max_submissions z
        JOIN count_submissions v ON v.submission_date = z.submission_date)
SELECT UH.submission_date, UH.unique_hackers, FL.hacker_id, hack.name AS Hacker_name FROM unique_hackers UH JOIN final_list FL ON FL.submission_date = UH.submission_date JOIN hackers hack ON hack.hacker_id = FL.hacker_id ORDER BY 1;
