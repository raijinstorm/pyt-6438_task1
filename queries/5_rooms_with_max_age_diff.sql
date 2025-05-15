SELECT r.id, r.name, COUNT(*), ROUND(MAX(CURRENT_DATE - s.birthday) / 365.25, 2)  - ROUND(min(CURRENT_DATE - s.birthday) / 365.25, 2) AS "Age_diff"
FROM students AS s 
JOIN rooms AS r 
    ON s.room = r.id  
GROUP BY r.id
ORDER BY "Age_diff" DESC
LIMIT 5;