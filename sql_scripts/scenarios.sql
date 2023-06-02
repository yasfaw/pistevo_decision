'''
#1
Get all claims that have not been billed. 
The results should include the claim informationas well as the patient’s name, patient’s address and provider’s name
'''
SELECT 
DISTINCT
    claim_id, 
    p.fname, 
    p.lname, 
    p.address, 
    p.city, 
    p.state, 
    p.zipcode, 
    pr.fname, 
    pr.lname
FROM billing.claim c
JOIN billing.patients p
ON p.patient_id =c.patient_id
JOIN  billing.provider pr
ON c.provider_id = pr.provider_id
WHERE c.billed = 'T';

'''
#2.
Get all providers who have had claims in the last 30 days where the insurance claimmust be 100% covered
'''
SELECT 
    pr.provider_id, pr.fname, pr.lname
FROM billing.provider pr
JOIN billing.claim c
ON pr.provider_id = c.provider_id
WHERE DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'), c.last_modified_date) <=30
GROUP BY pr.provider_id, pr.fname, pr.lname
HAVING sum(c.coverage_type) = 100 ;