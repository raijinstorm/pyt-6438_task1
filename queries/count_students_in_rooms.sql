SELECT r.id, r.name, COUNT(*) 
FROM students AS s 
JOIN rooms AS r 
    ON s.room = r.id  
GROUP BY r.id;