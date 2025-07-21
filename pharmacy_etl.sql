-- example of one of the sql scripts that should be run withing main py code -> "pharmacy_etl_example.py"


declare max_snapshot_date date default(select max(_snapshot_date) from `dwh.rx_procare_append`);

-- step 1
create or replace temp table rx_procare_base
as
select distinct
   *,
   declare max_snapshot_date date default(select max(_snapshot_date) from `dwh.rx_procare_append`);

-- step 1
create or replace temp table rx_procare_base
as
select distinct
   *,
   CLOSED_STATUS as substatus,
   X - Copay as by_copay,
   coalesce(Serial__, 'Unknown') as Serial_coalesce,
   coalesce(Dispense_Date, '1000-01-01') as Dispense_Date_coalesce
from `dwh.rx_procare_append`
where _snapshot_date=max_snapshot_date
and  concat(De_identified_Patient_ID,"-",Rx_Number,"-",coalesce(Serial__, 'Unknown'),"-", coalesce(Dispense_Date, '1000-01-01'))!='1466866-4470639-Unknown-1000-01-01'
and Script_Status in ('OPEN', 'CLOSED', 'TRANSFERRED', 'SHIPPED');

-- step 2 add dup_count and rn
create or replace temp table rx_procare_ranked as
select *,
  count(*) over (
    partition by concat(de_identified_patient_id, '-', rx_number, '-', serial_coalesce, '-', dispense_date_coalesce)
  ) as dup_count,
  row_number() over (
    partition by concat(de_identified_patient_id, '-', rx_number, '-', serial_coalesce, '-', dispense_date_coalesce)
    order by dispense_date desc
  ) as rn
from rx_procare_base;

-- step 3 - exclude duplicates
create or replace temp table rx_procare_temp as
select *
from rx_procare_ranked
where rn = 1;


-- step 4
merge `staging.rx_procare` r
using (select * from rx_procare_temp) as t
on  r.De_identified_Patient_ID=t.De_identified_Patient_ID and r.Rx_Number=t.Rx_Number and r.Serial_coalesce=t.Serial_coalesce and r.Dispense_Date_coalesce = t.Dispense_Date_coalesce
when matched then
update set
    Received_Date=t.Received_Date,
    Dispense_Date = t.Dispense_Date,
    Serial__ = t.Serial__,
    Total_Fills=t.Total_Fills,
    Fills_Dispensed=t.Fills_Dispensed,
    Fill_Remaining=t.Fill_Remaining,
    Provider_Last_Name=t.Provider_Last_Name,
    Provider_First_Name=t.Provider_First_Name,
    Provider_Address=t.Provider_Address,
    Provider_City=t.Provider_City,
    Provider_State__=t.Provider_State__,
    Provider_Zip_Code=t.Provider_Zip_Code,
    Provider_NPI=t.Provider_NPI,
    Region=t.Region,
    Script_Status=t.Script_Status,
    Patient_OOP=t.Patient_OOP,
    Payor_Name=t.Payor_Name,
    Plan_Name=t.Plan_Name,
    Copay = t.Copay,
    Source = t.Source,
    Fill_Type_Recieved = t.Fill_Type_Recieved,
    Fill_Type_Shipped = t.Fill_Type_Shipped,
    Date_Written = t.Date_Written,
    CLOSED_STATUS = t.CLOSED_STATUS,
    Insurance_Type = t.Insurance_Type,
    PA_STATUS = t.PA_STATUS,
    Order_PA_Status = t.Order_PA_Status,
    REMINDERSTATUS_PAT = t.REMINDERSTATUS_PAT,
    Plan_Name_Claim = t.Plan_Name_Claim,
    by_copay = t.by_copay,
    age = t.age,
    NDC = t.NDC,
    USAGE = t.USAGE,
    modified_serial_id = t.modified_serial_id,
    dup_count = t.dup_count,
    substatus = t.substatus,
    _snapshot_date=t._snapshot_date
when not matched by target then
insert (
    De_identified_Patient_ID,
    Rx_Number,
    Received_Date,
    Dispense_Date,
    Dispense_Date_coalesce,
    Serial__,
    Serial_coalesce,
    Total_Fills,
    Fills_Dispensed,
    Fill_Remaining,
    Provider_Last_Name,
    Provider_First_Name,
    Provider_Address,
    Provider_City,
    Provider_State__,
    Provider_Zip_Code,
    Provider_NPI,
    Region,
    Script_Status,
    Patient_OOP,
    Payor_Name,
    Plan_Name,
    Copay,
    Source,
    Fill_Type_Recieved,
    Fill_Type_Shipped,
    Date_Written,
    CLOSED_STATUS,
    Insurance_Type,
    PA_STATUS,
    Order_PA_Status,
    REMINDERSTATUS_PAT,
    Plan_Name_Claim,
    by_copay,
    age,
    NDC,
    USAGE,
    modified_serial_id,
    dup_count,
    substatus,
    _snapshot_date
)
values(
    t.De_identified_Patient_ID,
    t.Rx_Number,
    t.Received_Date,
    t.Dispense_Date,
    t.Dispense_Date_coalesce,
    t.Serial__,
    t.Serial_coalesce,
    t.Total_Fills,
    t.Fills_Dispensed,
    t.Fill_Remaining,
    t.Provider_Last_Name,
    t.Provider_First_Name,
    t.Provider_Address,
    t.Provider_City,
    t.Provider_State__,
    t.Provider_Zip_Code,
    t.Provider_NPI,
    t.Region,
    t.Script_Status,
    t.Patient_OOP,
    t.Payor_Name,
    t.Plan_Name,
    t.Copay,
    t.Source,
    t.Fill_Type_Recieved,
    t.Fill_Type_Shipped,
    t.Date_Written,
    t.CLOSED_STATUS,
    t.Insurance_Type,
    t.PA_STATUS,
    t.Order_PA_Status,
    t.REMINDERSTATUS_PAT,
    t.Plan_Name_Claim,
    t.by_copay,
    t.age,
    t.NDC,
    t.USAGE,
    t.modified_serial_id,
    t.dup_count,
    t.substatus,
    t._snapshot_date
);
