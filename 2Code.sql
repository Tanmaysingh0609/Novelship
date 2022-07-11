Deliverable C: Write a query to counts the number of duplicates in car table for each company.

select make , count(*) as duplicate car from
group by make 
having count(*) > 1


Deliverable D: Write a query to list every entry in car table and its associated company owner. If an entry doesn't have an owner in the company table, then print 'not found' in the cell. 

select c.id ,c.make ,c.color,c.owner , coalesce(comp.name , 'not_found')  from car c
left join company comp 
on c.id = comp.id
