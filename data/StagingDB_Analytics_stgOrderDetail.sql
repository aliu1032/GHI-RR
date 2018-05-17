select *
from StagingDB.Analytics.stgOrderDetail
where OrderLineItemID in (
        select distinct A.OrderLineItemID
        from (
                SELECT OrderLineItemID
                FROM StagingDB.Analytics.stgOrderDetail
                where OrderstartDate >= '2016-01-01'
                UNION
                Select OrderLineItemID
                FROM StagingDB.Analytics.mvwRevenue
                where AccountingPeriodDate >= '2016-01-01'
            ) A
        )
