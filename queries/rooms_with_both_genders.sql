SELECT r.id, r.name
FROM students AS s 
JOIN rooms AS r 
    ON s.room = r.id  
GROUP BY r.id
HAVING MAX(s.sex) = MIN(s.sex);