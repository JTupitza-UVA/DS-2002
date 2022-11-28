/***********************************************************************************/
-- This Script Creates a Populates a Date Dimension Table in SQL Server
 https://www.mssqltips.com/sqlservertip/5553/create-an-extended-date-dimension-for-a-sql-server-data-warehouse/
/***********************************************************************************/

BEGIN TRY
	DROP TABLE [dbo].[DimDate]
END TRY

BEGIN CATCH
	/*No Action*/
END CATCH

/**********************************************************************************/

CREATE TABLE [dbo].[DimDate] (
   [DateKey] [int] NOT NULL,
   [Date] [date] NOT NULL,
   [Day] [tinyint] NOT NULL,
   [DaySuffix] [char](2) NOT NULL,
   [Weekday] [tinyint] NOT NULL,
   [WeekDayName] [varchar](10) NOT NULL,
   [WeekDayName_Short] [char](3) NOT NULL,
   [WeekDayName_FirstLetter] [char](1) NOT NULL,
   [DOWInMonth] [tinyint] NOT NULL,
   [DayOfYear] [smallint] NOT NULL,
   [WeekOfMonth] [tinyint] NOT NULL,
   [WeekOfYear] [tinyint] NOT NULL,
   [Month] [tinyint] NOT NULL,
   [MonthName] [varchar](10) NOT NULL,
   [MonthName_Short] [char](3) NOT NULL,
   [MonthName_FirstLetter] [char](1) NOT NULL,
   [Quarter] [tinyint] NOT NULL,
   [QuarterName] [varchar](6) NOT NULL,
   [Year] [int] NOT NULL,
   [MMYYYY] [char](6) NOT NULL,
   [MonthYear] [char](7) NOT NULL,
   IsWeekend BIT NOT NULL,
   IsHoliday BIT NOT NULL,
   HolidayName VARCHAR(20) NULL,
   SpecialDays VARCHAR(20) NULL,
   [FinancialYear] [int] NULL,
   [FinancialQuater] [int] NULL,
   [FinancialMonth] [int] NULL,
   [FirstDateofYear] DATE NULL,
   [LastDateofYear] DATE NULL,
   [FirstDateofQuater] DATE NULL,
   [LastDateofQuater] DATE NULL,
   [FirstDateofMonth] DATE NULL,
   [LastDateofMonth] DATE NULL,
   [FirstDateofWeek] DATE NULL,
   [LastDateofWeek] DATE NULL,
   CurrentYear SMALLINT NULL,
   CurrentQuater SMALLINT NULL,
   CurrentMonth SMALLINT NULL,
   CurrentWeek SMALLINT NULL,
   CurrentDay SMALLINT NULL,
   PRIMARY KEY CLUSTERED ([DateKey] ASC)
)
GO

/********************************************************************************************/
--Specify Start Date and End date here
--Value of Start Date Must be Less than Your End Date 

SET NOCOUNT ON

TRUNCATE TABLE DimDate

DECLARE @CurrentDate DATE = '2000-01-01'
DECLARE @EndDate DATE = '2010-12-31'

WHILE @CurrentDate < @EndDate
BEGIN
   INSERT INTO [dbo].[DimDate] (
      [DateKey],
      [Date],
      [Day],
      [DaySuffix],
      [Weekday],
      [WeekDayName],
      [WeekDayName_Short],
      [WeekDayName_FirstLetter],
      [DOWInMonth],
      [DayOfYear],
      [WeekOfMonth],
      [WeekOfYear],
      [Month],
      [MonthName],
      [MonthName_Short],
      [MonthName_FirstLetter],
      [Quarter],
      [QuarterName],
      [Year],
      [MMYYYY],
      [MonthYear],
      [IsWeekend],
      [IsHoliday],
      [FirstDateofYear],
      [LastDateofYear],
      [FirstDateofQuater],
      [LastDateofQuater],
      [FirstDateofMonth],
      [LastDateofMonth],
      [FirstDateofWeek],
      [LastDateofWeek]
      )
   SELECT DateKey = YEAR(@CurrentDate) * 10000 + MONTH(@CurrentDate) * 100 + DAY(@CurrentDate),
      DATE = @CurrentDate,
      Day = DAY(@CurrentDate),
      [DaySuffix] = CASE 
         WHEN DAY(@CurrentDate) = 1
            OR DAY(@CurrentDate) = 21
            OR DAY(@CurrentDate) = 31
            THEN 'st'
         WHEN DAY(@CurrentDate) = 2
            OR DAY(@CurrentDate) = 22
            THEN 'nd'
         WHEN DAY(@CurrentDate) = 3
            OR DAY(@CurrentDate) = 23
            THEN 'rd'
         ELSE 'th'
         END,
      WEEKDAY = DATEPART(dw, @CurrentDate),
      WeekDayName = DATENAME(dw, @CurrentDate),
      WeekDayName_Short = UPPER(LEFT(DATENAME(dw, @CurrentDate), 3)),
      WeekDayName_FirstLetter = LEFT(DATENAME(dw, @CurrentDate), 1),
      [DOWInMonth] = DAY(@CurrentDate),
      [DayOfYear] = DATENAME(dy, @CurrentDate),
      [WeekOfMonth] = DATEPART(WEEK, @CurrentDate) - DATEPART(WEEK, DATEADD(MM, DATEDIFF(MM, 0, @CurrentDate), 0)) + 1,
      [WeekOfYear] = DATEPART(wk, @CurrentDate),
      [Month] = MONTH(@CurrentDate),
      [MonthName] = DATENAME(mm, @CurrentDate),
      [MonthName_Short] = UPPER(LEFT(DATENAME(mm, @CurrentDate), 3)),
      [MonthName_FirstLetter] = LEFT(DATENAME(mm, @CurrentDate), 1),
      [Quarter] = DATEPART(q, @CurrentDate),
      [QuarterName] = CASE 
         WHEN DATENAME(qq, @CurrentDate) = 1
            THEN 'First'
         WHEN DATENAME(qq, @CurrentDate) = 2
            THEN 'second'
         WHEN DATENAME(qq, @CurrentDate) = 3
            THEN 'third'
         WHEN DATENAME(qq, @CurrentDate) = 4
            THEN 'fourth'
         END,
      [Year] = YEAR(@CurrentDate),
      [MMYYYY] = RIGHT('0' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)), 2) + CAST(YEAR(@CurrentDate) AS VARCHAR(4)),
      [MonthYear] = CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + UPPER(LEFT(DATENAME(mm, @CurrentDate), 3)),
      [IsWeekend] = CASE 
         WHEN DATENAME(dw, @CurrentDate) = 'Sunday'
            OR DATENAME(dw, @CurrentDate) = 'Saturday'
            THEN 1
         ELSE 0
         END,
      [IsHoliday] = 0,
      [FirstDateofYear] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-01-01' AS DATE),
      [LastDateofYear] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-12-31' AS DATE),
      [FirstDateofQuater] = DATEADD(qq, DATEDIFF(qq, 0, GETDATE()), 0),
      [LastDateofQuater] = DATEADD(dd, - 1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + 1, 0)),
      [FirstDateofMonth] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)) + '-01' AS DATE),
      [LastDateofMonth] = EOMONTH(@CurrentDate),
      [FirstDateofWeek] = DATEADD(dd, - (DATEPART(dw, @CurrentDate) - 1), @CurrentDate),
      [LastDateofWeek] = DATEADD(dd, 7 - (DATEPART(dw, @CurrentDate)), @CurrentDate)

   SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

--Update Holiday information
UPDATE DimDate
SET [IsHoliday] = 1,
   [HolidayName] = 'Christmas'
WHERE [Month] = 12
   AND [DAY] = 25

UPDATE DimDate
SET SpecialDays = 'Valentines Day'
WHERE [Month] = 2
   AND [DAY] = 14

--Update current date information
UPDATE DimDate
SET CurrentYear = DATEDIFF(yy, GETDATE(), DATE),
   CurrentQuater = DATEDIFF(q, GETDATE(), DATE),
   CurrentMonth = DATEDIFF(m, GETDATE(), DATE),
   CurrentWeek = DATEDIFF(ww, GETDATE(), DATE),
   CurrentDay = DATEDIFF(dd, GETDATE(), DATE)



SELECT * FROM [dbo].[DimDate]