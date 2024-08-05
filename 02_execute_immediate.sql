create or replace view EDP_QA.REPORTS."Engine Hours"(
	"Calendar Date",
	"TV Customer ID",
	"LW CustomerId",
	"Customer Number",
	"Customer Name",
	"Customer Number-Name",
	ISCURRENTASSIGNMENT,
	VEHICLEENTITYKEY,
	"Unit Number",
	CVN,
	VIN,
	"Year",
	"Make",
	"Model",
	"Segment",
	"Driver Type",
	"Driver/Pool",
	"Pool Contact",
	"Driver State",
	"Vehicle State",
	"Vehicle Status",
	"Months in Service",
	"Months Billed",
	"Latest Odometer",
	"Current Engine Hours",
	"Total Trip Count",
	"Total Trip Duration",
	"Trip Duration in Seconds",
	"Total Accumulated Miles",
	"Engine Hours Begin",
	"Engine Hours Begin in Seconds",
	"Engine Hours End",
	"Engine Hours End in Seconds",
	"Total Accumulated Engine Hours",
	"Total Accumulated Engine Hours in Seconds",
	"Client Data 1",
	"Client Data 2",
	"Client Data 3",
	"Client Data 4",
	"Client Data 5",
	"Client Data 6",
	"Client Data 7",
	"Employee Data Field 1",
	"Employee Data Field 2",
	"Employee Data Field 3",
	"Fleet Number",
	"Fleet Name",
	"Fleet Number-Name",
	"Account Number",
	"Account Name",
	"Account Number- Name",
	"Subaccount Number",
	"Subaccount Name",
	"Subaccount Number-Name",
	"CreatedDate"
) as
WITH MERCHANTSTRIPS_DISTINCT AS (
SELECT DISTINCT VEHICLEENTITYKEY, MERCHANT_TRIP_START ,TOTAL_MILES , TRIPKEY_DURATION , TRIP_KEY , START_ODOMETER, END_ODOMETER
FROM COMMON.MERCHANTSTRIPS_VIEW
),

TRIP_DETAILS_CALC_CTE
AS (
    SELECT DISTINCT
        MTV.VEHICLEENTITYKEY
        ,to_date(MTV.MERCHANT_TRIP_START) AS "Trip Begin Date"
        ,sum(MTV.TOTAL_MILES ) as "Total Miles"
        ,SUM(to_double( (split_part(MTV.TRIPKEY_DURATION,':',1) * 60) + 
         (split_part(MTV.TRIPKEY_DURATION,':',2)) +
         (split_part(MTV.TRIPKEY_DURATION,':',3) * 0.01) ) ) AS "Trip Duration"
       --,SUM(MTV.TRIPKEY_DURATION) AS "Trip Duration in Seconds"
         ,SUM(to_double( (split_part(MTV.TRIPKEY_DURATION,':',1) * 3600) + 
         (split_part(MTV.TRIPKEY_DURATION,':',2) * 60) +
         (split_part(MTV.TRIPKEY_DURATION,':',3) ) ) ) AS "Trip Duration in Seconds"
        ,COUNT(DISTINCT TRIP_KEY) AS "Trip Count"
	    ,min(MTV.START_ODOMETER) as "Begin Odometer Reading"
        ,max(MTV.END_ODOMETER) as "End Odometer reading"      
       FROM MERCHANTSTRIPS_DISTINCT AS MTV
          group by MTV.VEHICLEENTITYKEY,
           to_date(MTV.MERCHANT_TRIP_START)
        )
,ENGINE_HRS_CTE
AS (
SELECT DATE_TRUNC('DAY',OBSERVATIONDATETIME)TELEMETRY_DATE,VIN,MIN(ENGINE_RUN_TIME) MIN_ENGINE_RUN_TIME,MAX(ENGINE_RUN_TIME) MAX_ENGINE_RUN_TIME FROM FACT.ENGINERUNTIME ERT
WHERE ERT.ENGINE_RUN_TIME > 0 GROUP BY DATE_TRUNC('DAY',OBSERVATIONDATETIME),VIN
        )
,ENGINE_HRS_LATEST_CTE
AS(SELECT  VIN,MAX(ENGINE_RUN_TIME) CURRENT_ENGINE_RUN_TIME FROM FACT.ENGINERUNTIME ERT
GROUP BY VIN)

,CALENDAT_LIST_CTE as (select * from COMMON."Calendar List" cl where "Calendar Date">=ADD_MONTHS(current_timestamp::date, -18))


SELECT DISTINCT
    cl."Calendar Date" as "Calendar Date",
    chl."TV CustomerId" AS "TV Customer ID"
    ,TO_NUMBER(chl."LW CustomerId") AS "LW CustomerId"
    ,IFNULL(NULLIF(TRIM(chl."Customer Number"),''),'-') AS "Customer Number"
    ,IFNULL(NULLIF(TRIM(chl."Customer Name"),''),'-') AS "Customer Name"
   	 ,IFNULL(NULLIF(TRIM(chl."Customer Number-Name"),''),'-') AS "Customer Number-Name"
    ,cva.ISCURRENTASSIGNMENT
    ,ve.VEHICLEENTITYKEY
	,IFNULL(NULLIF(TRIM(ve.LW_VehicleUnitNumber),''),'-') AS "Unit Number"
	,IFNULL(NULLIF(TRIM(cva.CUSTOMERVEHICLENUMBER),''),'-') AS "CVN"
	,IFNULL(NULLIF(TRIM(v.VIN),''),'-') AS "VIN"
	,v."VehicleYear":: NUMBER(38,0) AS "Year"
	,IFNULL(NULLIF(TRIM(v."Vehicle Make"),''),'-') AS "Make"
	,IFNULL(NULLIF(TRIM(v."Vehicle Model"),''),'-') AS "Model"
	,IFNULL(v."Vehicle Segment", 'Not Available') AS "Segment"
	,CASE WHEN p.PersonEntityKey IS NOT NULL THEN 'Driver' ELSE 'Pool' END "Driver Type"
	,NVL(CASE WHEN p.PersonEntityKey IS NOT NULL THEN CONCAT (pe.personlastname,',',' ',pe.personfirstname)ELSE IFNULL(NULLIF(TRIM(dp.driverpoolname),''),'-') END,'-') AS "Driver/Pool"
	,IFNULL(TRIM(CASE WHEN p.PersonEntityKey IS NOT NULL THEN NULL ELSE CONCAT (p1.personlastname,',',' ',p1.personfirstname) END),'-') AS "Pool Contact"
	,COALESCE(pe.DRIVERSTATE, dp.DRIVERSTATE, '-') AS "Driver State"
	,IFNULL(NULLIF(TRIM(v."Vehicle State"),''),'-') AS "Vehicle State"
   	,IFNULL(NULLIF(TRIM(v."Vehicle Status"),''),'-') AS "Vehicle Status"
	,DATEDIFF(month, v."Service Start Date", IFNULL(v."Service End Date", getdate())) + 
    CASE WHEN DATE_PART(day, v."Service Start Date") > DATE_PART(day,IFNULL(v."Service End Date", getdate()))THEN - 1 ELSE 0 END AS "Months in Service"
    ,DATEDIFF(month, v."Bill Start Date", IFNULL(v."Bill End Date", getdate())) + 
    CASE WHEN DATE_PART(day, v."Bill Start Date") > DATE_PART(day, IFNULL(v."Bill End Date", getdate()))	THEN - 1 ELSE 0	  END AS "Months Billed"
    --,IFNULL(NULLIF(TRIM(vrl."Plate Number"),''),'-') AS "Plate Number"
   -- ,IFNULL(NULLIF(TRIM(vrl."Plate State Abbrev"),''),'-') AS "Plate State"
    ,lo.ODOMETER AS "Latest Odometer"
    ,EHL.CURRENT_ENGINE_RUN_TIME
    ,IFNULL(TD."Trip Count",0) AS "Trip Count"
    --,TO_CHAR(TRUNC(TD."Trip Duration"::NUMBER(38,2)), 'FM9999999999900') || ':' ||
     --TO_CHAR(TRUNC(MOD(TD."Trip Duration"::NUMBER(38,2) * 60, 60)), 'FM00') || ':' ||
     --TO_CHAR(TRUNC(MOD(TD."Trip Duration"::NUMBER(38,2) * 3600, 60)), 'FM00')  AS "Trip Duration"
     ,TO_CHAR(floor(TD."Trip Duration in Seconds"/3600)||':'||
              floor((TD."Trip Duration in Seconds"%3600)/60)||':'||
              (TD."Trip Duration in Seconds"%3600)%60) AS "Trip Duration"
    , TD."Trip Duration in Seconds" as "Trip Duration in Seconds" 
    ,IFNULL(TD."Total Miles",0) AS "Total Miles"
    ,TO_CHAR(TRUNC(EH. MIN_ENGINE_RUN_TIME::NUMBER(38,2)), 'FM9999999999900') || ':' ||
     TO_CHAR(TRUNC(MOD(EH. MIN_ENGINE_RUN_TIME::NUMBER(38,2) * 60, 60)), 'FM00') || ':' ||
     TO_CHAR(TRUNC(MOD(EH. MIN_ENGINE_RUN_TIME::NUMBER(38,2) * 3600, 60)), 'FM00') AS "Engine Hours Begin"
     ,EH.MIN_ENGINE_RUN_TIME::NUMBER(38,2) * 3600 AS "Engine Hours Begin in Seconds"
    ,TO_CHAR(TRUNC(EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2)), 'FM9999999999900') || ':' ||
     TO_CHAR(TRUNC(MOD(EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2) * 60, 60)), 'FM00') || ':' ||
     TO_CHAR(TRUNC(MOD(EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2) * 3600, 60)), 'FM00') AS "Engine Hours End"
     ,EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2) * 3600 AS "Engine Hours End in Seconds"
    ,TO_CHAR(TRUNC((EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2) - EH. MIN_ENGINE_RUN_TIME::NUMBER(38,2))), 'FM9999999999900') || ':' ||
     TO_CHAR(TRUNC(MOD((EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2) - EH. MIN_ENGINE_RUN_TIME::NUMBER(38,2)) * 60, 60)), 'FM00') || ':' ||
     TO_CHAR(TRUNC(MOD((EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2) - EH. MIN_ENGINE_RUN_TIME::NUMBER(38,2)) * 3600, 60)), 'FM00') AS "Total Accumulated Engine Hours "
     ,(EH.MAX_ENGINE_RUN_TIME::NUMBER(38,2) - EH. MIN_ENGINE_RUN_TIME::NUMBER(38,2)) * 3600  as "Total Accumulated Engine Hours in Seconds"
    ,IFNULL(NULLIF(TRIM(cdf.CLIENTDATAFIELD1),''),'-') AS "Client Data 1"
    ,IFNULL(NULLIF(TRIM(cdf.CLIENTDATAFIELD2),''),'-') AS "Client Data 2"
    ,IFNULL(NULLIF(TRIM(cdf.CLIENTDATAFIELD3),''),'-') AS "Client Data 3"
    ,IFNULL(NULLIF(TRIM(cdf.CLIENTDATAFIELD4),''),'-') AS "Client Data 4"
    ,IFNULL(NULLIF(TRIM(cdf.CLIENTDATAFIELD5),''),'-') AS "Client Data 5"
    ,IFNULL(NULLIF(TRIM(cdf.CLIENTDATAFIELD6),''),'-') AS "Client Data 6"
    ,IFNULL(NULLIF(TRIM(cdf.CLIENTDATAFIELD7),''),'-') AS "Client Data 7"
    ,IFNULL(NULLIF(TRIM(edf."Employee Data Field 1"),''),'-') AS "Employee Data Field 1"
	,IFNULL(NULLIF(TRIM(edf."Employee Data Field 2"),''),'-') AS "Employee Data Field 2"
	,IFNULL(NULLIF(TRIM(edf."Employee Data Field 3"),''),'-') AS "Employee Data Field 3"
	,IFNULL(NULLIF(TRIM(chl."Fleet Number"),''),'-') AS "Fleet Number"
	,IFNULL(NULLIF(TRIM(chl."Fleet Name"),''),'-') AS "Fleet Name"
    ,IFNULL(NULLIF(TRIM(chl."Fleet Number-Name"),''),'-') AS "Fleet Number-Name"
	,IFNULL(NULLIF(TRIM(chl."Account Number"),''),'-') AS "Account Number"
	,IFNULL(NULLIF(TRIM(chl."Account Name"),''),'-') AS "Account Name"
    ,IFNULL(NULLIF(TRIM(chl."Account Number-Name"),''),'-') AS "Account Number-Name"
	,IFNULL(NULLIF(TRIM(chl."Subaccount Number"),''),'-') AS "Subaccount Number"
	,IFNULL(NULLIF(TRIM(chl."Subaccount Name"),''),'-') AS "Subaccount Name"
    ,IFNULL(NULLIF(TRIM(chl."Subaccount Number-Name"),''),'-') AS "Subaccount Number-Name"
    ,CURRENT_TIMESTAMP as CREATEDDATE
FROM CALENDAT_LIST_CTE cl
JOIN ENGINE_HRS_CTE EH
ON EH.TELEMETRY_DATE=cl."Calendar Date"
JOIN COMMON."Vehicle List" v
on EH.VIN=v.VIN
JOIN COMMON.REPORT_VEHICLECUSTOMERASSIGNEMT AS cva
ON  cva.VEHICLEENTITYKEY = v."Vehicle Entity Key"
    and cl."Calendar Date" >= cva.ASSIGNEDDATETIME and cl."Calendar Date" <= cva.UNASSIGNEDDATETIME 
    JOIN ( SELECT * FROM MDM.VEHICLEENTITY QUALIFY row_number() over(partition by tv_vehicleunitnumber order by EFFECTIVEENDDATE DESC) = 1) AS ve
    ON ve.VEHICLEENTITYKEY = cva.VEHICLEENTITYKEY   
JOIN ( SELECT * FROM MDM.CUSTOMERENTITY  QUALIFY row_number() over(partition by tv_customerid order by EFFECTIVEENDDATE DESC) = 1) AS ce 
    ON ce.CUSTOMERENTITYKEY = cva.CUSTOMERENTITYKEY
JOIN COMMON."Customer Hierarchy List" AS chl
    ON chl."TV CustomerId" = ce.TV_CUSTOMERID
LEFT JOIN TRIP_DETAILS_CALC_CTE AS TD
    ON TD.VEHICLEENTITYKEY=v."Vehicle Entity Key"
    AND TD."Trip Begin Date" =cl."Calendar Date"
LEFT JOIN (
	SELECT *
	FROM Fact.VEHICLEDRIVERASSIGNMENT QUALIFY row_number() OVER (
			PARTITION BY CUSTOMERENTITYKEY
			,VEHICLEENTITYKEY ORDER BY EffectiveEndDate DESC
				,EFFECTIVESTARTDATE DESC
			) = 1
	) AS vda
	ON cva.VEHICLEENTITYKEY = vda.VEHICLEENTITYKEY
	AND cva.CUSTOMERENTITYKEY = vda.CUSTOMERENTITYKEY
LEFT JOIN DIM.Person AS p
	ON vda.driverentitykey = p.PersonEntityKey
LEFT JOIN mdm.personentity AS pe
	ON pe.personentitykey = p.PersonEntityKey
LEFT JOIN (
	SELECT *
	FROM DIM.DRIVERPOOLS QUALIFY row_number() OVER (
			PARTITION BY driverpoolkey ORDER BY EFFECTIVEENDDATE DESC
			) = 1
	)  AS dp
	ON vda.driverpoolkey = dp.driverpoolkey
LEFT JOIN (
	SELECT *
	FROM MDM.PERSONENTITY QUALIFY row_number() OVER (
			PARTITION BY tv_personid ORDER BY EffectiveEndDate DESC
			) = 1
	) AS p1
	ON dp.poolcontactid = p1.tv_personid
LEFT JOIN COMMON.PIVOTED_CLIENTDATAFIELDS AS cdf
	ON ce.CUSTOMERENTITYKEY = cdf.customerentitykey
	AND ve.VEHICLEENTITYKEY = cdf.vehicleentitykey
LEFT JOIN ( SELECT *
	FROM COMMON."Employee Data Field List" QUALIFY row_number() OVER (
			PARTITION BY "TV Person ID", "TV Customer ID" ORDER BY "Effective End Date" DESC
			) = 1 ) AS edf
	ON pe.TV_PERSONID = edf."TV Person ID"
	AND chl."TV CustomerEntityId" = edf."TV Customer ID"
/*LEFT JOIN
    (
      SELECT * FROM
      (
        SELECT vrl.*,
    RANK() OVER (PARTITION BY "Vehicle Entity Key","Customer Entity Key" ORDER BY "Registration Start Date" DESC) RN
     FROM COMMON."Vehicle Registration List" vrl
     )
     WHERE RN=1
    )vrl
    ON ve.VEHICLEENTITYKEY = vrl."Vehicle Entity Key"*/
LEFT JOIN FACT.LATESTODOMETER AS lo
ON ve.VEHICLEENTITYKEY = lo.VEHICLEENTITYKEY
LEFT JOIN ENGINE_HRS_LATEST_CTE EHL
ON EHL.VIN=v.VIN
order by 1,2;