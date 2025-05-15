SELECT r.id, r.name, COUNT(*), ROUND(AVG(CURRENT_DATE - s.birthday) / 365.25, 2) AS "Avg_age"
FROM students AS s 
JOIN rooms AS r 
    ON s.room = r.id  
GROUP BY r.id
ORDER BY AVG(CURRENT_DATE - s.birthday) ASC
LIMIT 5;