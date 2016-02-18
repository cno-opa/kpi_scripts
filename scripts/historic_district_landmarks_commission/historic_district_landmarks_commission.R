# Load dependencies
library(dplyr)
library(lubridate)
library(RODBC)
library(RSocrata)
library(zoo)

# Initialize department information
dept <- "19"
org <- "historic_district_landmarks_commission"

# Set working directory
setwd(paste0("O:\\Projects\\kpi_scripts\\clean_data\\", org))

# Initialize database information
connection <- "Driver={SQL Server};Server=cno-sqlreport01;Database=Lama_Rpt"

# Calculate KPI: "Avg. days to review staff approvable applications"
historic_district_landmarks_commission_19_1 <- function() {
    
    # Initialize measure information
    measure_number <- "1"
    ind <- paste(dept, measure_number, sep = "-")
    measure_name <- "Avg. days to review staff approvable applications"
    
    # Query database
    channel <- odbcDriverConnect(connection = connection)
    df <- sqlQuery(channel, "SELECT	Prmt.Num,
		Prmt.NumString,
                   Prmt.RefCode,
                   Prmt.IsClosed,
                   Prmt.D_Exp,
                   PrmtGridData.Division,
                   Prmt.Code M_S,
                   Prmt.SubmittalType,
                   PrmtGridData.FilingDate,
                   PrmtGridData.IssueDate AS [IssueDate],
                   DATEDIFF(D,Prmt.D_Filed,PrmtGridData.IssueDate) DaysToApprove,
                   PrmtGridData.Type
                   
                   FROM	Prmt INNER JOIN 
                   PrmtGridData ON Prmt.PrmtID = PrmtGridData.PrmtID INNER JOIN
                   PrmtEvent ON Prmt.PrmtID = PrmtEvent.PrmtID AND
                   -- ensure that the Permit Issued event is actually complete (i.e. not a projected date)
                   PrmtEvent.EventName = 'Permit Issued' AND
                   PrmtEvent.IsComplete = 1 AND
                   PrmtEvent.IsDeleted = 0
                   
                   WHERE	PrmtGridData.Division = 'HDLC' AND
                   -- exclude every permit that has any meeting associated with it (e.g. ARC, Commission) - i.e. anything with a matching record in PrmtMeeting
                   Prmt.PrmtID NOT IN (SELECT	P2.PrmtID
                   FROM	Prmt P2 INNER JOIN
                   PrmtMeeting ON P2.PrmtID = PrmtMeeting.PrmtID
                   WHERE	PrmtMeeting.IsDeleted = 0) AND
                   -- exclude every permit that includes the event 'Returned for revision'
                   Prmt.PrmtID NOT IN (SELECT	P3.PrmtID
                   FROM	Prmt P3 INNER JOIN
                   PrmtEvent PE3 ON P3.PrmtID = PE3.PrmtID 
                   WHERE	PE3.IsDeleted = 0 AND
                   PE3.EventName = 'Returned for revision') AND
                   Prmt.IsDeleted = 0 AND
                   -- exclude permits that were created as tests of LAMA functionality
                   NOT Prmt.Descr LIKE '%###TEST###%'")
    close(channel)
    
    # Change column headings to lowercase
    names(df) <- tolower(names(df))
    
    # Clean data
    clean <- df %>%
        filter(daystoapprove >= 0)
    
    # Write clean data table to share with department
    write.csv(clean, file = paste(ind, Sys.Date(),".csv", sep = "_"))
    
    # Calculate performance metric by quarter
    kpi_table <- clean %>%
        mutate(yearqtr = as.yearqtr(issuedate)) %>%
        group_by(yearqtr) %>%
        summarize(value = round(mean(daystoapprove), 1), count = n()) %>%
        mutate(dept = dept,
               measure_number = measure_number,
               index = ind,
               org = org,
               measure_name = measure_name) %>%
        select(dept, measure_number, index, org, measure_name, yearqtr, value, count)
    
    # Display summary table
    print.data.frame(kpi_table)
}
historic_district_landmarks_commission_19_1()



# Calculate KPI: "Percent of closed enforcement cases closed due to voluntary compliance"
historic_district_landmarks_commission_19_2 <- function() {
    
}