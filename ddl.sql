-- prep user account
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root';

-- table definition for ingestion target
-- assume that attribute other than InsertDate are nullable

CREATE DATABASE credit;

USE credit;

CREATE TABLE credit.loan_application (
	No int,
	SeriousDlqin2yrs int,
	RevolvingUtilizationOfUnsecuredLines float,
	Age float,
	NumberOfTime30To59DaysPastDueNotWorse int,
	DebtRatio float,
	MonthlyIncome double,
	NumberOfOpenCreditLinesAndLoans int,
	NumberOfTimes90DaysLate int,
	NumberRealEstateLoansOrLines int,
	NumberOfTime60To89DaysPastDueNotWorse int,
	NumberOfDependents int,
	InsertDate datetime NOT NULL
);

CREATE INDEX loan_application_insert_date ON credit.loan_application(InsertDate);