-- Auto Generated (Do not modify) 51DDE908A5D77BF1B49C0C18FB8375E754D0B721BA062C17376CCC94D9B42FA0
CREATE VIEW [dbo].[sales_amout_gt_500] AS (select [_].[DateID],
    [_].[MedallionID],
    [_].[HackneyLicenseID],
    [_].[PickupTimeID],
    [_].[DropoffTimeID],
    [_].[PassengerCount],
    [_].[TripDurationSeconds],
    [_].[TripDistanceMiles],
    [_].[PaymentType],
    [_].[FareAmount],
    [_].[SurchargeAmount],
    [_].[TaxAmount],
    [_].[TipAmount],
    [_].[TollsAmount],
    [_].[TotalAmount]
from 
(
    select [DateID],
        [MedallionID],
        [HackneyLicenseID],
        [PickupTimeID],
        [DropoffTimeID],
        [PassengerCount],
        [TripDurationSeconds],
        [TripDistanceMiles],
        [PaymentType],
        [FareAmount],
        [SurchargeAmount],
        [TaxAmount],
        [TipAmount],
        [TollsAmount],
        [TotalAmount]
    from [MyFirstWarehouse].[dbo].[Trip] as [$Table]
) as [_]
where [_].[TotalAmount] > 500)