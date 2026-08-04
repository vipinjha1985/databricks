%sql
;with cte1
As
(Select * From Calendar  Where Day Not In ("2020-08-01", "2020-08-10", "2020-08-15")) --- Main table
, cte2 --- Main table join with Calendar
as
(
Select O.Day O_Day, C.Day C_Day
From cte1 O
Right Join calendar C On O.Day = C.Day 
Where left(YearMonthDay, 4) = '2020' and MonthOfYear = '08'
)
, cte3 --- filter with not available dates i.e. "2020-08-01", "2020-08-10", "2020-08-15" and applied ranking
(
Select * , row_Number () Over (Order by C_Day) id
From cte2 where O_Day is null 
), cte4 --- Applied cross join with Cte2 (main + calendar) and used Max to get the latest date mapping between range. 
(
Select A.O_Day, Max(B.ID) BID
From cte2 A
Cross Join cte3 B
Where A.C_Day > B.C_Day
And A.O_Day Is Not Null 
Group By A.O_Day
),
cte5 -- get the min date wise count
(
  Select Min(O_Day) min_Day, Count(BID) cnt, BID From cte4 Group By BID
)
Select A.*, B.min_Day  , B.cnt -- Final result
 , Case when B.cnt >= 5 Then 1 
         When B.cnt < 5 And A.O_Day = B.min_Day Then 1 Else 0 End as PRCount
From cte4 A
Inner Join cte5 B On B.BID = A.BID
order by 1

--Select * , case When O_Day is Null then 0 else row_Number() Over ( Order By C_Day) End rank 
--From cte2 order by C_Day