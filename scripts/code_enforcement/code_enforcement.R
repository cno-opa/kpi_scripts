# Load dependencies
library(dplyr)
library(lubridate)
library(RODBC)
library(RSocrata)
library(zoo)

setwd("O:\\Projects\\STAT KPIs\\KPI Scripts\\Code Enforcement")

# Calculate KPI: "Avg. days to complete a new, initial inspection request"
code_enforcement_6_1 <- function() {

    # Set KPI name
    kpi_name <- "Avg. days to complete a new, initial inspection request"
    
    channel <- odbcDriverConnect(connection = "Driver={SQL Server};Server=cno-sqlreport01;Database=Lama_Rpt")
    
    # Query database
    insp <- sqlQuery(channel, "--DECLARE @Start datetime = '1/1/2014',
--		@End datetime = '1/31/2016'
                     
                     SELECT	CAST(FirstInsp.D_Insp AS DATE) [Inspection.Completed],
                     CAST(CodeIncid.D_Filed AS DATE) [Case.Established],
                     CodeIncid.NumString,
                     CodeIncid.Location,
                     CodeIncid.IsClosed,
                     DATEDIFF(D,CodeIncid.D_Filed,FirstInsp.D_Insp) AgeInDays
                     
                     FROM	CodeIncid OUTER APPLY (
                     
                     SELECT      TOP 1 CodeInsp.D_Insp, CodeInsp.CodeIncidID
                     
                     FROM        CodeInsp  
                     
                     WHERE       CodeInsp.IsDeleted = 0 AND
                     CodeInsp.IsComplete = 1 AND
                     CodeInsp.CodeIncidID = CodeIncid.CodeIncidID
                     
                     ORDER BY    CodeInsp.D_Insp ASC
                     
                     ) FirstInsp 
                     
                     WHERE	FirstInsp.CodeIncidID = CodeIncid.CodeIncidID AND
                     CodeIncid.IsDeleted = 0 AND
                     CodeIncid.LU_DivisionID = 3 AND
                     CodeIncid.Descr NOT LIKE '%###TEST###%' --AND
                     --FirstInsp.D_Insp BETWEEN @Start AND @End")
    
    # Close database connection
    close(channel)
    
    # Coerce date columns to "Date" class
    insp$Inspection.Completed <- as.POSIXct(insp$Inspection.Completed)
    insp$Case.Established <- as.POSIXct(insp$Case.Established)
    
    # Remove cases with age less than zero
    insp <- insp %>%
        filter(AgeInDays >= 0 &
                   Case.Established >= "2010-01-01" &
                   Inspection.Completed <= as.POSIXct(today()))
    
    # Add column with year and quarter
    insp$yrqtr <- as.yearqtr(insp$Inspection.Completed)
    
    # Group by quarter
    avg_age_by_quarter <- insp %>%
        group_by(yrqtr) %>%
        summarize(value = mean(AgeInDays), count = n())
    
    # Add column with KPI name
    avg_age_by_quarter$kpi_name <- kpi_name
    
    # Return summary table
    return(print.data.frame(avg_age_by_quarter))
}
code_enforcement_6_1()



# Calculate KPI: "Number of inspections"
code_enforcement_6_2 <- function() {
    
    # Set KPI name
    kpi_name <- "Number of inspections"
    
    # Open database connection
    channel <- odbcDriverConnect(connection = "Driver={SQL Server};Server=cno-sqlreport01;Database=Lama_Rpt")
    
    # Query database
    df <- sqlQuery(channel, "SELECT	CodeIncid.NumString,
		CodeIncid.Location,
                   CodeInsp.D_Insp,
                   CodeEvent.EventName
                   
                   FROM	CodeInsp INNER JOIN
                   CodeIncid ON CodeInsp.CodeIncidID = CodeIncid.CodeIncidID INNER JOIN
                   CodeEvent ON CodeInsp.CodeInspID = CodeEvent.SpawnID AND CodeEvent.SpawnType = 1
                   
                   WHERE	CodeIncid.IsDeleted = 0 AND
                   CodeInsp.IsDeleted = 0 AND
                   CodeEvent.IsDeleted = 0 AND
                   CodeIncid.Descr NOT LIKE '%###TEST###$' AND
                   CodeInsp.IsComplete = 1 AND
                   CodeIncid.LU_DivisionID = 3")
    
    # Close database connection
    close(channel)
    
    # Aggregate data by month
    inspections <- df %>%
        filter(D_Insp >= "2010-01-01" &
                   D_Insp <= as.POSIXct(today())) %>%
        mutate(yearmon = as.yearmon(D_Insp)) %>%
        group_by(yearmon) %>%
        summarize(inspections = n())
    
    # Load demolition inspections table from shared drive
    demo_inspections <- read.csv("Demo-Inspections.csv",
                                 col.names = c("yearmon", "demo_inspections"),
                                 colClasses = c("POSIXct", "integer"))
    demo_inspections <- demo_inspections %>%
        mutate(yearmon = as.yearmon(yearmon))
    
    # Merge inspections with demolition inspections and aggregate by quarter
    kpi_table <- merge(inspections, demo_inspections, by = "yearmon") %>%
        mutate(total_inspections = inspections + demo_inspections,
               yearqtr = yearqtr(yearmon)) %>%
        group_by(yearqtr) %>%
        summarize(value = sum(total_inspections)) %>%
        mutate(kpi_name = kpi_name)
    
    # Return summary table for measure
    return(print.data.frame(kpi_table))
}
code_enforcement_6_2()



# Calculate KPI: "Number of properties brought to hearing"
code_enforcement_6_3 <- function() {
    
    # Set KPI name
    kpi_name <- "Number of properties brought to hearing"
    
    channel <- odbcDriverConnect(connection = "Driver={SQL Server};Server=cno-sqlreport01;Database=Lama_Rpt")
    
    # Query database
    df <- sqlQuery(channel, "SELECT		CASE
				WHEN CodeIncid.IsClosed = 0 THEN 'Open'
                   ELSE 'Closed'
                   END [O/C],
                   CodeIncid.NumString CaseNumber,
                   CodeIncid.Location StreetAddress,
                   CASE
                   WHEN PrevHearing.D_Prev IS NULL THEN 'Initial Hearing'
                   ELSE 'Reset Hearing'
                   END CaseHistory,
                   CAST(CodeEvent.D_Event AS DATE) HearingDate,
                   CAST(CodeEvent.D_Event AS TIME) HearingTime,
                   CodeEvent.Status Judgment,
                   CAST(CodeIncid.D_Create AS DATE) CaseEstablished,
                   CAST(CGD.InitInspection AS DATE) Inspected,
                   CGD.InitInspectionStatus AS InspectionResult,
                   CAST(PrevHearing.D_Prev AS DATE) LastHearing,
                   PrevHearing.Status LastHearingStat,
                   DATEDIFF(d,COALESCE(PrevHearing.D_Prev, CGD.FirstInspection, CodeIncid.D_Create), CodeEvent.D_Event) DaysToHearing,
                   CGD.CouncilDistrict,
                   CodeIncid.X,
                   CodeIncid.Y
                   FROM		CodeIncid INNER JOIN
                   CodeIncidGridData CGD ON CodeIncid.CodeIncidID = CGD.CodeIncidID INNER JOIN
                   CodeEvent ON CodeIncid.CodeIncidID = CodeEvent.CodeIncidID INNER JOIN
                   LU_CodeEvent ON CodeEvent.LU_CodeEventID = LU_CodeEvent.LU_CodeEventID AND
                   LU_CodeEvent.Type = 3 AND
                   CodeEvent.IsDeleted = 0 AND
                   CodeEvent.IsComplete = 1
                   -- use APPLY rather than JOIN so the sub-query can refer 
                   -- to the specific case in the parent query
                   OUTER APPLY (
                   -- use TOP 1 to pick the most recent hearing with a 
                   -- completion date LESS THAN the hearing in the master query
                   SELECT	TOP 1 CE.D_Event D_Prev, CE.Status
                   FROM	CodeEvent CE INNER JOIN
                   LU_CodeEvent LCE ON CE.LU_CodeEventID = LCE.LU_CodeEventID AND
                   LCE.Type = 3
                   WHERE	CE.D_Event < CodeEvent.D_Event AND
                   CE.CodeIncidID = CodeEvent.CodeIncidID AND
                   CE.IsDeleted = 0 AND
                   CE.IsComplete = 1
                   ORDER BY	D_Event DESC) PrevHearing
                   WHERE		CodeIncid.LU_DivisionID = 3 AND
                   CodeIncid.IsDeleted = 0
                   ORDER BY	CodeIncid.NumString")
    
    # Close database connection
    close(channel)
    
    names(df) <- tolower(names(df))
    
    # Clean data
    kpi_table <- df %>%
        select(casenumber, casehistory, judgment, hearingdate) %>%
        filter(casehistory == "Initial Hearing") %>%
        mutate(hearingdate = as.POSIXct(hearingdate), yearqtr = as.yearqtr(hearingdate)) %>%
        group_by(yearqtr) %>%
        summarize(value = n()) %>%
        mutate(kpi_name = kpi_name)
    
    # Return quarterly summary table for measure
    return(print.data.frame(kpi_table))
}
code_enforcement_6_3()



# Calculate KPI: "Percent of hearings reset due to failure to properly notify the owner"
code_enforcement_6_4 <- function() {
    
    # Set KPI name
    kpi_name <- "Percent of hearings reset due to failure to properly notify the owner"
    
    channel <- odbcDriverConnect(connection = "Driver={SQL Server};Server=cno-sqlreport01;Database=Lama_Rpt")
    
    # Query database
    df <- sqlQuery(channel, "SELECT		CASE
                   WHEN CodeIncid.IsClosed = 0 THEN 'Open'
                   ELSE 'Closed'
                   END [O/C],
                   CodeIncid.NumString CaseNumber,
                   CodeIncid.Location StreetAddress,
                   CASE
                   WHEN PrevHearing.D_Prev IS NULL THEN 'Initial Hearing'
                   ELSE 'Reset Hearing'
                   END CaseHistory,
                   CAST(CodeEvent.D_Event AS DATE) HearingDate,
                   CAST(CodeEvent.D_Event AS TIME) HearingTime,
                   CodeEvent.Status Judgment,
                   CAST(CodeIncid.D_Create AS DATE) CaseEstablished,
                   CAST(CGD.InitInspection AS DATE) Inspected,
                   CGD.InitInspectionStatus AS InspectionResult,
                   CAST(PrevHearing.D_Prev AS DATE) LastHearing,
                   PrevHearing.Status LastHearingStat,
                   DATEDIFF(d,COALESCE(PrevHearing.D_Prev, CGD.FirstInspection, CodeIncid.D_Create), CodeEvent.D_Event) DaysToHearing,
                   CGD.CouncilDistrict,
                   CodeIncid.X,
                   CodeIncid.Y
                   FROM		CodeIncid INNER JOIN
                   CodeIncidGridData CGD ON CodeIncid.CodeIncidID = CGD.CodeIncidID INNER JOIN
                   CodeEvent ON CodeIncid.CodeIncidID = CodeEvent.CodeIncidID INNER JOIN
                   LU_CodeEvent ON CodeEvent.LU_CodeEventID = LU_CodeEvent.LU_CodeEventID AND
                   LU_CodeEvent.Type = 3 AND
                   CodeEvent.IsDeleted = 0 AND
                   CodeEvent.IsComplete = 1
                   -- use APPLY rather than JOIN so the sub-query can refer 
                   -- to the specific case in the parent query
                   OUTER APPLY (
                   -- use TOP 1 to pick the most recent hearing with a 
                   -- completion date LESS THAN the hearing in the master query
                   SELECT	TOP 1 CE.D_Event D_Prev, CE.Status
                   FROM	CodeEvent CE INNER JOIN
                   LU_CodeEvent LCE ON CE.LU_CodeEventID = LCE.LU_CodeEventID AND
                   LCE.Type = 3
                   WHERE	CE.D_Event < CodeEvent.D_Event AND
                   CE.CodeIncidID = CodeEvent.CodeIncidID AND
                   CE.IsDeleted = 0 AND
                   CE.IsComplete = 1
                   ORDER BY	D_Event DESC) PrevHearing
                   WHERE		CodeIncid.LU_DivisionID = 3 AND
                   CodeIncid.IsDeleted = 0
                   ORDER BY	CodeIncid.NumString")
    
    # Close database connection
    close(channel)
    
    names(df) <- tolower(names(df))
    
    # Clean data
    kpi_table <- df %>%
        select(casenumber,
               casehistory,
               judgment,
               hearingdate) %>%
        filter(casehistory == "Initial Hearing") %>%
        mutate(hearingdate = as.POSIXct(hearingdate),
               yearqtr = as.yearqtr(hearingdate)) %>%
        group_by(yearqtr) %>%
        summarize(value = sum(judgment == "Reset: Insufficient Notice"),
                  count = n()) %>%
        mutate(kpi_name = kpi_name)
    
    # Return quarterly summary table for measure
    return(print.data.frame(kpi_table))
}
code_enforcement_6_4()



# Calculate KPI: "Percent of hearings reset due to failure to re-inspect the property"
code_enforcement_6_5 <- function() {
    
    # Set KPI name
    kpi_name <- "Percent of hearings reset due to failure to re-inspect the property"
    
    channel <- odbcDriverConnect(connection = "Driver={SQL Server};Server=cno-sqlreport01;Database=Lama_Rpt")
    
    # Query database
    df <- sqlQuery(channel, "SELECT		CASE
                   WHEN CodeIncid.IsClosed = 0 THEN 'Open'
                   ELSE 'Closed'
                   END [O/C],
                   CodeIncid.NumString CaseNumber,
                   CodeIncid.Location StreetAddress,
                   CASE
                   WHEN PrevHearing.D_Prev IS NULL THEN 'Initial Hearing'
                   ELSE 'Reset Hearing'
                   END CaseHistory,
                   CAST(CodeEvent.D_Event AS DATE) HearingDate,
                   CAST(CodeEvent.D_Event AS TIME) HearingTime,
                   CodeEvent.Status Judgment,
                   CAST(CodeIncid.D_Create AS DATE) CaseEstablished,
                   CAST(CGD.InitInspection AS DATE) Inspected,
                   CGD.InitInspectionStatus AS InspectionResult,
                   CAST(PrevHearing.D_Prev AS DATE) LastHearing,
                   PrevHearing.Status LastHearingStat,
                   DATEDIFF(d,COALESCE(PrevHearing.D_Prev, CGD.FirstInspection, CodeIncid.D_Create), CodeEvent.D_Event) DaysToHearing,
                   CGD.CouncilDistrict,
                   CodeIncid.X,
                   CodeIncid.Y
                   FROM		CodeIncid INNER JOIN
                   CodeIncidGridData CGD ON CodeIncid.CodeIncidID = CGD.CodeIncidID INNER JOIN
                   CodeEvent ON CodeIncid.CodeIncidID = CodeEvent.CodeIncidID INNER JOIN
                   LU_CodeEvent ON CodeEvent.LU_CodeEventID = LU_CodeEvent.LU_CodeEventID AND
                   LU_CodeEvent.Type = 3 AND
                   CodeEvent.IsDeleted = 0 AND
                   CodeEvent.IsComplete = 1
                   -- use APPLY rather than JOIN so the sub-query can refer 
                   -- to the specific case in the parent query
                   OUTER APPLY (
                   -- use TOP 1 to pick the most recent hearing with a 
                   -- completion date LESS THAN the hearing in the master query
                   SELECT	TOP 1 CE.D_Event D_Prev, CE.Status
                   FROM	CodeEvent CE INNER JOIN
                   LU_CodeEvent LCE ON CE.LU_CodeEventID = LCE.LU_CodeEventID AND
                   LCE.Type = 3
                   WHERE	CE.D_Event < CodeEvent.D_Event AND
                   CE.CodeIncidID = CodeEvent.CodeIncidID AND
                   CE.IsDeleted = 0 AND
                   CE.IsComplete = 1
                   ORDER BY	D_Event DESC) PrevHearing
                   WHERE		CodeIncid.LU_DivisionID = 3 AND
                   CodeIncid.IsDeleted = 0
                   ORDER BY	CodeIncid.NumString")
    
    # Close database connection
    close(channel)
    
    names(df) <- tolower(names(df))
    
    # Clean data, filter for relevant case types, and aggregate by quarter
    kpi_table <- df %>%
        select(casenumber,
               casehistory,
               judgment,
               hearingdate) %>%
        filter(casehistory == "Initial Hearing") %>%
        mutate(hearingdate = as.POSIXct(hearingdate),
               yearqtr = as.yearqtr(hearingdate)) %>%
        group_by(yearqtr) %>%
        summarize(value = sum(judgment == "Reset: Not Re-Inspected"),
                  count = n()) %>%
        mutate(kpi_name = kpi_name)
    
    # Return quarterly summary table for measure
    return(print.data.frame(kpi_table))
}
code_enforcement_6_5()



# Calculate KPI: "Number of blighted units demolished"
code_enforcement_6_6 <- function() {
    
    # Set KPI name
    kpi_name <- "Number of blighted units demolished"
    
    # Read table from data.nola.gov
    df <- read.socrata("https://data.nola.gov/Housing-Land-Use-and-Blight/BlightStatus-Demolitions/e3wd-h7q2")
    
    names(df) <- tolower(names(df))
    
    # Clean data and aggregate by quarter
    kpi_table <- df %>%
        select(units,
               demolition_start) %>%
        mutate(demolition_start = as.POSIXct(demolition_start),
            yearqtr = as.yearqtr(demolition_start)) %>%
        group_by(yearqtr) %>%
        summarize(value = sum(units, na.rm = TRUE)) %>%
        mutate(kpi_name = kpi_name)
    
    # Return quarterly summary table for measure
    return(print.data.frame(kpi_table))
}
code_enforcement_6_6()



# Calculate KPI: "Number of blighted properties brought into compliance by property owners"
code_enforcement_6_7 <- function() {
    
    # Set KPI name
    kpi_name <- "Number of blighted properties brought into compliance by property owners"
    
    channel <- odbcDriverConnect(connection = "Driver={SQL Server};Server=cno-sqlreport01;Database=Lama_Rpt")
    
    # Query database
    df <- sqlQuery(channel, "SELECT		CASE
                   WHEN CodeIncid.IsClosed = 0 THEN 'Open'
                   ELSE 'Closed'
                   END [O/C],
                   CodeIncid.NumString CaseNumber,
                   CodeIncid.Location StreetAddress,
                   CASE
                   WHEN PrevHearing.D_Prev IS NULL THEN 'Initial Hearing'
                   ELSE 'Reset Hearing'
                   END CaseHistory,
                   CAST(CodeEvent.D_Event AS DATE) HearingDate,
                   CAST(CodeEvent.D_Event AS TIME) HearingTime,
                   CodeEvent.Status Judgment,
                   CAST(CodeIncid.D_Create AS DATE) CaseEstablished,
                   CAST(CGD.InitInspection AS DATE) Inspected,
                   CGD.InitInspectionStatus AS InspectionResult,
                   CAST(PrevHearing.D_Prev AS DATE) LastHearing,
                   PrevHearing.Status LastHearingStat,
                   DATEDIFF(d,COALESCE(PrevHearing.D_Prev, CGD.FirstInspection, CodeIncid.D_Create), CodeEvent.D_Event) DaysToHearing,
                   CGD.CouncilDistrict,
                   CodeIncid.X,
                   CodeIncid.Y
                   FROM		CodeIncid INNER JOIN
                   CodeIncidGridData CGD ON CodeIncid.CodeIncidID = CGD.CodeIncidID INNER JOIN
                   CodeEvent ON CodeIncid.CodeIncidID = CodeEvent.CodeIncidID INNER JOIN
                   LU_CodeEvent ON CodeEvent.LU_CodeEventID = LU_CodeEvent.LU_CodeEventID AND
                   LU_CodeEvent.Type = 3 AND
                   CodeEvent.IsDeleted = 0 AND
                   CodeEvent.IsComplete = 1
                   -- use APPLY rather than JOIN so the sub-query can refer 
                   -- to the specific case in the parent query
                   OUTER APPLY (
                   -- use TOP 1 to pick the most recent hearing with a 
                   -- completion date LESS THAN the hearing in the master query
                   SELECT	TOP 1 CE.D_Event D_Prev, CE.Status
                   FROM	CodeEvent CE INNER JOIN
                   LU_CodeEvent LCE ON CE.LU_CodeEventID = LCE.LU_CodeEventID AND
                   LCE.Type = 3
                   WHERE	CE.D_Event < CodeEvent.D_Event AND
                   CE.CodeIncidID = CodeEvent.CodeIncidID AND
                   CE.IsDeleted = 0 AND
                   CE.IsComplete = 1
                   ORDER BY	D_Event DESC) PrevHearing
                   WHERE		CodeIncid.LU_DivisionID = 3 AND
                   CodeIncid.IsDeleted = 0
                   ORDER BY	CodeIncid.NumString")
    
    # Close database connection
    close(channel)
    
    names(df) <- tolower(names(df))
    
    # Clean data, filter for relevant cases, and aggregate by month
    hearings <- df %>%
        select(casenumber, casehistory, judgment, hearingdate) %>%
        filter(grepl(".*compli.*", judgment, ignore.case = TRUE) |
                   grepl(".*abate.*", judgment, ignore.case = TRUE)) %>%
        mutate(hearingdate = as.POSIXct(hearingdate),
               yearmon = as.yearmon(hearingdate),
               yearqtr = as.yearqtr(hearingdate)) %>%
        group_by(yearmon) %>%
        summarize(compliance = n()) 
    
    # Load lien waivers table from shared drive
    lien_waivers <- read.csv("LienWaivers.csv",
                                 col.names = c("yearmon", "lien_waivers"),
                                 colClasses = c("POSIXct", "integer"))
    lien_waivers <- lien_waivers %>%
        mutate(yearmon = as.yearmon(yearmon))
    
    # Merge inspections with lien waivers and aggregate by quarter
    kpi_table <- merge(hearings, lien_waivers, by = "yearmon") %>%
        mutate(total_compliance = compliance + lien_waivers,
               yearqtr = yearqtr(yearmon)) %>%
        group_by(yearqtr) %>%
        summarize(value = sum(total_compliance)) %>%
        mutate(kpi_name = kpi_name)
    
    # Return quarterly summary table for measure
    return(print.data.frame(kpi_table))
}
code_enforcement_6_7()


