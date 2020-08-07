-- FUNCTION: covid19_schema.covid19_compare_tables1(text)

-- DROP FUNCTION covid19_schema.covid19_compare_tables1(text);

-- validation functon for incoming data: Incoming source data is validated against exsiting data if no difference then
-- the data is inserted into the epidemiology table , otherwise email should be set

CREATE OR REPLACE FUNCTION covid19_schema.covid19_validation(
	source_code text)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    
AS $BODY$
     
--AS $BODY$
DECLARE 
valid_flag integer := 0;
BEGIN
--RETURN(
  with A as (
    select hashtext(textin(record_out(staging_epidemiology ))) as h, count(*) as c
      from staging_epidemiology 
	  where staging_epidemiology.source= source_code and staging_epidemiology.date in (select epidemiology.date from epidemiology where epidemiology.source=source_code ) 
      group by 1
),
 B as (
    select hashtext(textin(record_out(epidemiology))) as h, count(*) as c
    from epidemiology where epidemiology.source=source_code
    group by 1
)
select count(*) into valid_flag
 from A full outer join B on (A.h + A.c = B.h + B.c)
 where A.h is null or B.h is null;
 raise notice 'Value: %', valid_flag;
 
	if valid_flag=0 then
  		insert into epidemiology (select * from staging_epidemiology where staging_epidemiology.source= source_code)
		on conflict (date, country, countrycode, COALESCE(adm_area_1, ''), COALESCE(adm_area_2, ''), COALESCE(adm_area_3, ''), source)
	 	DO
        	UPDATE 
				SET confirmed        = epidemiology.confirmed,
					dead             = epidemiology.dead,
					recovered        = epidemiology.recovered,
					hospitalised 	 = epidemiology.hospitalised,
					hospitalised_icu = epidemiology.hospitalised_icu;
					
					
       -- RETURNING *;
	 ELSE

	 END IF;
 return valid_flag;
 
-- );
END
$BODY$;

ALTER FUNCTION covid19_schema.covid19_validation(text)
    OWNER TO covid19_read_write;
