# Load SparkR
spark_path <- '/usr/local/spark'
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(ggplot2)

# Initialise the sparkR session
sparkR.session(master = "yarn-client", sparkConfig = list(spark.driver.memory = "1g"))

# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")

# Read the data
nyc_parking_data <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", "csv", header = "true", inferSchema = "true")

################ Examine the data ###########
nrow(nyc_parking_data) ##10803028  

# Fix column names
names(nyc_parking_data) <- stringr::str_replace_all(names(nyc_parking_data),c(" "="_",","  = ""))

# Include data only for 2017
tickets_2017 <- filter(nyc_parking_data, year(nyc_parking_data$Issue_Date) == 2017)


# 1. Find the total number of tickets for the year.
nrow(tickets_2017) ## 5431918 

# A view needs to be created before a SparkDataFrame is used with SQL
createOrReplaceTempView(tickets_2017, "ny_parking_2017_tbl")

# 2. Find out the number of unique states from where the cars that got parking tickets came from. 
Unique_States <- SparkR::sql("SELECT COUNT (DISTINCT Registration_State) FROM ny_parking_2017_tbl")
head(Unique_States) # 65
Max_Entries <- SparkR::sql("SELECT Registration_State,COUNT(Registration_State) AS Num_Of_Entries from ny_parking_2017_tbl GROUP BY Registration_State ORDER BY Num_Of_Entries desc")
head(Max_Entries)

##      Registration_State Num_Of_Entries                                             
##1                 NY        4273951
##2                 NJ         475825
##3                 PA         140286
##4                 CT          70403
##5                 FL          69468
##6                 IN          45525

#There is a numeric entry '99' in the column which should be corrected.
#Replace it with the state having maximum entries. Give the number of unique states again.

Entries_with_99 <- SparkR::sql("SELECT COUNT(Registration_State) AS Num_Of_Entries from ny_parking_2017_tbl where Registration_State= 99 ") #Answer is 16055
head(Entries_with_99) ##  16055

tickets_2017$Registration_State <- ifelse(tickets_2017$Registration_State == "99", "NY" ,tickets_2017$Registration_State)
collect(select(tickets_2017, countDistinct(tickets_2017$Registration_State))) ## 64

# A view needs to be created before a SparkDataFrame is used with SQL
createOrReplaceTempView(tickets_2017, "ny_parking_2017_tbl")

# Registration_state after replacing 99 with NY
After_replacing_99_with_NY <- SparkR::sql("SELECT Registration_State,COUNT(Registration_State) AS Num_Of_Entries from ny_parking_2017_tbl GROUP BY Registration_State ORDER BY Num_Of_Entries desc")
head(After_replacing_99_with_NY)

##      Registration_State Num_Of_Entries                                             
##1                 NY        4290006
##2                 NJ         475825
##3                 PA         140286
##4                 CT          70403
##5                 FL          69468
##6                 IN          45525


######### Aggregations Tasks ########
# 1.How often does each violation code occur? Display the frequency of the top five violation codes.

Occur_Of_Violation_Codes <- SparkR::sql("SELECT Violation_Code, COUNT(Violation_Code) AS Occur_Of_Violation_Codes from ny_parking_2017_tbl GROUP BY Violation_Code ORDER BY Occur_Of_Violation_Codes desc LIMIT 5")
head(Occur_Of_Violation_Codes)

##        Violation_Code Occur_Of_Violation_Codes                                       
#1             21                   768087
#2             36                   662765
#3             38                   542079
#4             14                   476664
#5             20                   319646

Top5_violation_code <- data.frame(head(Occur_Of_Violation_Codes,5))
ggplot(Top5_violation_code, aes(x=as.factor(Violation_Code), y=Occur_Of_Violation_Codes))+ geom_col() + xlab("Violation Code") + ylab("Frequency of Tickets") + ggtitle("Violation Code vs Frequency of Ticket") + geom_text(aes(label=Occur_Of_Violation_Codes),vjust=-0.3)

#2.How often does each 'vehicle body type' get a parking ticket? How about the 'vehicle make'? (Hint: find the top 5 for both)

Vehicle_Make <- SparkR::sql("SELECT Vehicle_Make, COUNT(Vehicle_Make) AS Num_Of_Entries from ny_parking_2017_tbl GROUP BY Vehicle_Make ORDER BY COUNT(Vehicle_Make) desc LIMIT 5")
Vehicle_Body_Type <- SparkR::sql("SELECT Vehicle_Body_Type, COUNT(Vehicle_Body_Type) AS Num_Of_Entries from ny_parking_2017_tbl GROUP BY Vehicle_Body_Type ORDER BY Num_Of_Entries desc LIMIT 5")
head(Vehicle_Body_Type)

##    Vehicle_Body_Type Num_Of_Entries                                              
#1              SUBN        1883954
#2              4DSD        1547312
#3               VAN         724029
#4              DELV         358984
#5               SDN         194197

top5_Vehicle_Body_Type<-data.frame(head(Vehicle_Body_Type,5))
ggplot(top5_Vehicle_Body_Type, aes(x=as.factor(Vehicle_Body_Type), y=Num_Of_Entries))+ geom_col() + xlab("Vehicle Body Type") + ylab("Number of Entries") + ggtitle("Vehicle Body Type vs Number of Entries") + geom_text(aes(label=Num_Of_Entries),vjust=-0.3)

#3.A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequency of tickets for each of the following:
#'Violation Precinct' (this is the precinct of the zone where the violation occurred). Using this, can you make any insights for parking violations in any specific areas of the city?
# 'Issuer Precinct' (this is the precinct that issued the ticket)
#Here you would have noticed that the dataframe has 'Violating Precinct' or 'Issuing Precinct' as '0'. These are the erroneous entries. Hence, provide the record for five correct precincts. (Hint: Print top six entries after sorting)
## Violating Precinct
V_Precinct <- SparkR::sql("SELECT Violation_Precinct, COUNT(Violation_Precinct) AS No_Of_Violations from ny_parking_2017_tbl WHERE Violation_Precinct <> 0 GROUP BY Violation_Precinct ORDER BY No_Of_Violations desc LIMIT 6")
head(V_Precinct)

##  Violation_Precinct No_Of_Violations                                           
#1                 19           274445
#2                 14           203553
#3                  1           174702
#4                 18           169131
#5                114           147444
#6                 13           125113

top5_V_Precinct<- data.frame(head(V_Precinct,5))
ggplot(top5_V_Precinct, aes(x=as.factor(Violation_Precinct), y=No_Of_Violations))+ geom_col() + xlab("Violation Precinct") + ylab("Number of Violations") + ggtitle("Violation Precinct vs Number of Violations") + geom_text(aes(label=No_Of_Violations),vjust=-0.3)

## Issuing Precinc
I_Precinct <- SparkR::sql("SELECT Issuer_Precinct, COUNT(Issuer_Precinct) AS Precinct_Issuers from ny_parking_2017_tbl WHERE Issuer_Precinct <> 0 GROUP BY Issuer_Precinct ORDER BY Precinct_Issuers desc LIMIT 5")
head(I_Precinct)

##  Issuer_Precinct Precinct_Issuers                                              
#1              19           266961
#2              14           200495
#3               1           168740
#4              18           162994
#5             114           144054

top5_I_Precinct<- data.frame(head(I_Precinct,5))
ggplot(top5_I_Precinct, aes(x=as.factor(Issuer_Precinct), y=Precinct_Issuers))+ geom_col() + xlab("Issuer Precinct") + ylab("Number of Issuers") + ggtitle("Issuer Precinct vs Number of Ticket") + geom_text(aes(label=Precinct_Issuers),vjust=-0.3)

##4.Find the violation code frequency across three precincts which have issued the most number of tickets - do these precinct zones 
#have an exceptionally high frequency of certain violation codes? Are these codes common across precincts? 
#Hint: In the SQL view, use the 'where' attribute to filter among three precincts.

# In Year 2017 [Top Three Issuer Precinct's : 19, 14 and 1]

# #Violation Code Distribution in Issuer Precinct 19
issuer_precinct_19 <- SparkR::sql("SELECT Violation_Code, count(*)as Frequency_of_Tickets, Issuer_Precinct
                                  from ny_parking_2017_tbl 
                                  where Issuer_Precinct = 19
                                  group by Violation_Code, Issuer_Precinct
                                  order by Frequency_of_Tickets desc")
head(issuer_precinct_19, 5)
#Violation_Code Frequency_of_Tickets Issuer_Precinct                           
#1             46                48445              19
#2             38                36386              19
#3             37                36056              19
#4             14                29797              19
#5             21                28415              19

issuer_precinct_19_top5<- data.frame(head(issuer_precinct_19, 5))

# #Violation Code Distribution in Issuer Precinct 14
issuer_precinct_14 <- SparkR::sql("SELECT Violation_Code, count(*)as Frequency_of_Tickets, Issuer_Precinct
                                  from ny_parking_2017_tbl 
                                  where Issuer_Precinct = 14
                                  group by Violation_Code, Issuer_Precinct
                                  order by Frequency_of_Tickets desc")
head(issuer_precinct_14, 5)
#Violation_Code Frequency_of_Tickets Issuer_Precinct                           
#1             14                45036              14
#2             69                30464              14
#3             31                22555              14
#4             47                18364              14
#5             42                10027              14

issuer_precinct_14_top5<- data.frame(head(issuer_precinct_14, 5))

# Violation Code Distribution in Issuer Precinct 1
issuer_precinct_1 <- SparkR::sql("SELECT Violation_Code, count(*)as Frequency_of_Tickets, Issuer_Precinct
                                 from ny_parking_2017_tbl 
                                 where Issuer_Precinct = 1
                                 group by Violation_Code, Issuer_Precinct
                                 order by Frequency_of_Tickets desc")
head(issuer_precinct_1, 5)
#Violation_Code Frequency_of_Tickets Issuer_Precinct                           
#1             14                38354               1
#2             16                19081               1
#3             20                15408               1
#4             46                12745               1
#5             38                 8535               1

issuer_precinct_1_top5<- data.frame(head(issuer_precinct_1, 5))

#Combining Violation Code Distribution vs Issuer Precincts in 2017
Combined_violation_issuer <- rbind(issuer_precinct_19_top5, issuer_precinct_14_top5, issuer_precinct_1_top5)
head(Combined_violation_issuer,15)

##         Violation_Code Frequency_of_Tickets Issuer_Precinct
#1              46                48445              19
#2              38                36386              19
#3              37                36056              19
#4              14                29797              19
#5              21                28415              19
#6              14                45036              14
#7              69                30464              14
#8              31                22555              14
#9              47                18364              14
#10             42                10027              14
#11             14                38354               1
#12             16                19081               1
#13             20                15408               1
#14             46                12745               1
#15             38                 8535               1

ggplot(Combined_violation_issuer, aes(x= as.factor(Violation_Code), y=Frequency_of_Tickets))+ geom_col()+ facet_grid(~Issuer_Precinct) + xlab("Violation Code") + ylab("Frequency of Tickets") + ggtitle("Violation Code Distribution vs. Top Issuer Precinct") + geom_text(aes(label=Frequency_of_Tickets),vjust=-0.3)

###5. You'd want to find out the properties of parking violations across different times of the day:

# Q 5.1 Find a way to deal with missing values, if any.

violation_Code_Na_precinct_1 <- SparkR::sql("SELECT Violation_Time
                                            from ny_parking_2017_tbl
                                            Where isNull(Violation_Time) = true")
nrow(violation_Code_Na_precinct_1)
#0

# Q 5.2The Violation Time field is specified in a strange format. 
# Find a way to make this into a time attribute that you can use to divide into groups.

# Violation time:"0143A", "0400P", "1223P" etc
# from summary of data we see that violation time is stored as characters having alphabets A and P denoted as AM and PM respectively
Subset_tickets_2017 <- subset(tickets_2017,isNotNull(tickets_2017$Violation_Time))
Subset_tickets_2017$Violation_hr <- substr(Subset_tickets_2017$Violation_Time,1,2)
Subset_tickets_2017$Violation_AMPM <- substr(Subset_tickets_2017$Violation_Time, 5,5)
Subset_tickets_2017$Violation_bin <- ifelse(Subset_tickets_2017$Violation_hr != 12 & Subset_tickets_2017$Violation_AMPM =="P",
                                            Subset_tickets_2017$Violation_hr +12,Subset_tickets_2017$Violation_hr)

head(Subset_tickets_2017,20)
## Can not add results here as output data is huge

# A view needs to be created before a SparkDataFrame is used with SQL
createOrReplaceTempView(Subset_tickets_2017, "ny_parking_2017_tbl")

# Q 5.3 Divide 24 hours into six equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups,
# find the three most commonly occurring violations.
# Hint: Use the CASE-WHEN in SQL view to segregate into bins. For finding the most commonly occurring violations, 
# a similar approach can be used as mention in the hint for question 4

Subset_tickets_2017_bins <- SparkR::sql("SELECT Violation_hr,
                                        Violation_Code,
                                        CASE WHEN Violation_bin >=0 AND Violation_bin <=3
                                        THEN 'Late_Night'
                                        WHEN Violation_bin >=4 AND Violation_bin <=7
                                        THEN 'Early_Morning'
                                        WHEN Violation_bin >=8 AND Violation_bin <=11
                                        THEN 'Morning'
                                        WHEN Violation_bin >=12 AND Violation_bin <=15
                                        THEN 'Afternoon' 
                                        WHEN Violation_bin >=16 AND Violation_bin <=19
                                        THEN 'Evening' 
                                        ELSE 'Night' 
                                        END AS Violation_Hour_Bin
                                        FROM ny_parking_2017_tbl")
head(Subset_tickets_2017_bins,20)

##       Violation_hr Violation_Code Violation_Hour_Bin                               
##1            11             47            Morning
##2            08              7              Night
##3            00             78         Late_Night
##4            05             40      Early_Morning
##5            02             64          Afternoon
##6            12             20          Afternoon
##7            10             36            Morning
##8            10             38            Morning
##9            07             14      Early_Morning
##10           09             75            Morning
##11           12             10          Afternoon
##12           10             69            Morning
##13           01             21         Late_Night
##14           12             38          Afternoon
##15           01             48          Afternoon
##16           08             21            Morning
##17           08             21            Morning
##18           10             68            Morning
##19           02             51          Afternoon
##20           08              9            Morning


df_Subset_tickets_2017_bins <- data.frame(head(Subset_tickets_2017_bins,20))
ggplot(df_Subset_tickets_2017_bins, aes(x= as.factor(Violation_Code), y=Violation_hr))+ geom_col()+ facet_grid(~Violation_Hour_Bin) + xlab("Violation Code") + ylab("Violation Hour") + ggtitle("Violation Code Distribution vs. Violation_Hour_Bin") + geom_text(aes(label=Violation_hr),vjust=-0.3)

# A view needs to be created before a SparkDataFrame is used with SQL
createOrReplaceTempView(Subset_tickets_2017_bins, "ny_parking_2017_tbl")

# 5.4 Now, try another direction. For the three most commonly occurring violation codes, 
# find the most common time of the day (in terms of the bins from the previous part)

# Top 3 Violation code VS violation time bin

Top_3_violations_code <- SparkR::sql("SELECT Violation_Code,
                                     count(*) Number_of_tickets
                                     FROM ny_parking_2017_tbl
                                     GROUP BY Violation_Code
                                     ORDER BY Number_of_tickets desc")
head(Top_3_violations_code)
##     Violation_Code Number_of_tickets                                              
##1             21            768087
##2             36            662765
##3             38            542079
##4             14            476664
##5             20            319646
##6             46            312330
#Top-3 Violation Codes for 2017 are 21, 36 and 38

common_time_bin <- SparkR::sql("SELECT Violation_Code,
                               Violation_Hour_Bin,
                               count(*) Number_of_tickets
                               FROM ny_parking_2017_tbl
                               WHERE violation_code IN (21,36,38)
                               GROUP BY Violation_Code, 
                               Violation_Hour_Bin
                               ORDER BY Violation_Code, 
                               Violation_Hour_Bin,
                               Number_of_tickets desc")	
head(common_time_bin,20)

##      Violation_Code Violation_Hour_Bin Number_of_tickets                          
#1              21          Afternoon             76949
#2              21      Early_Morning             57897
#3              21            Evening               259
#4              21         Late_Night             34704
#5              21            Morning            598070
#6              21              Night               208
#7              36          Afternoon            286284
#8              36      Early_Morning             14782
#9              36            Evening             13534
#10             36            Morning            348165
#11             38          Afternoon            240795
#12             38      Early_Morning              1273
#13             38            Evening            102855
#14             38         Late_Night               238
#15             38            Morning            176570
#16             38              Night             20348

df_common_times_bin_violation <- data.frame(head(common_time_bin, nrow(common_time_bin)))
ggplot(df_common_times_bin_violation, aes(x= as.factor(Violation_Hour_Bin), y=Number_of_tickets))+ geom_col()+ facet_grid(~Violation_Code) + xlab("Violation Hour Bin") + ylab("Number of Tickets") + ggtitle("Violation Hour Bin vs. Number of Tickets") + geom_text(aes(label=Number_of_tickets),vjust=-0.3)

#6.Let's try and find some seasonality in this data
#First, divide the year into some number of seasons, and find frequencies of tickets for each season. (Hint: Use Issue Date to segregate into seasons)

##Let us create Additional Columns in the Dataset that Correspond to the Year and Month of Ticket Issue
tickets_2017$Issue_Year<- year(tickets_2017$Issue_Date)
tickets_2017$Issue_Month <- month(tickets_2017$Issue_Date)
head(tickets_2017)
#Cannot write output as it is huge

# A view needs to be created before a SparkDataFrame is used with SQL
createOrReplaceTempView(tickets_2017, "ny_parking_2017_tbl")

Seasonality_count_2017 <- SparkR::sql("SELECT Summons_Number, Violation_Code,
                                      CASE WHEN Issue_Month IN (1,12,11)
                                      THEN 'Winter'
                                      WHEN Issue_Month BETWEEN 2 AND 4
                                      THEN 'Spring'
                                      WHEN Issue_Month BETWEEN 5 AND 7
                                      THEN 'Summer'
                                      WHEN Issue_Month BETWEEN 8 AND 10
                                      THEN 'Fall'
                                      END AS Seasonality
                                      FROM ny_parking_2017_tbl")
head(Seasonality_count_2017)

##  Summons_Number Violation_Code Seasonality                                     
#1     8478629828             47      Summer
#2     5096917368              7      Summer
#3     1407740258             78      Winter
#4     1413656420             40      Spring
#5     8480309064             64      Winter
#6     1416638830             20      Spring

# A view needs to be created before a SparkDataFrame is used with SQL
createOrReplaceTempView(Seasonality_count_2017, "ny_parking_2017_tbl")

Sesonality_ticket_count_2017 <- SparkR::sql("SELECT Seasonality,Count(*)as ticket_count
                                            FROM ny_parking_2017_tbl
                                            GROUP BY Seasonality
                                            ORDER BY ticket_count desc")
head(Sesonality_ticket_count_2017)

#     Seasonality ticket_count                                                      
#1      Spring      2680106
#2      Summer      1872801
#3      Winter       878061
#4        Fall          950

Frequency_Seasonality<- data.frame(head(Sesonality_ticket_count_2017),5)
ggplot(Frequency_Seasonality, aes(x= as.factor(Seasonality), y=ticket_count)) +geom_col() + xlab("Seasons") + ylab("Frequency of Tickets") + ggtitle("Seasons vs. Frequency of Tickets") + geom_text(aes(label=ticket_count),vjust=-0.3)
### Season vs. Violation Code Analysis ###

Seasonality_violation_ticket_2017 <- SparkR::sql("SELECT  Seasonality, Violation_Code,ticket_count
                                                 FROM (SELECT dense_rank() over (partition by Seasonality order by ticket_count desc) rk,
                                                 Seasonality,
                                                 Violation_Code,
                                                 ticket_count
                                                 FROM (SELECT Seasonality,
                                                 Violation_Code,
                                                 Count(*) ticket_count
                                                 FROM ny_parking_2017_tbl
                                                 GROUP BY Seasonality, Violation_Code))
                                                 WHERE rk <= 3
                                                 ORDER BY Seasonality, ticket_count desc")

head(Seasonality_violation_ticket_2017,20)

##      Seasonality Violation_Code ticket_count                                      
#1         Fall             46          214
#2         Fall             21          175
#3         Fall             40          109
#4       Spring             21       365843
#5       Spring             36       297452
#6       Spring             38       278059
#7       Summer             21       282138
#8       Summer             36       235544
#9       Summer             38       168562
#10      Winter             36       129769
#11      Winter             21       119931
#12      Winter             38        95451 


################################ 7 Question Solution 1 ######################

##7A
Violation_code_frequency <- SparkR::sql("SELECT Violation_Code, count(*) as Frequency_of_Tickets
                                        from ny_parking_2017_tbl 
                                        group by Violation_Code
                                        order by Frequency_of_Tickets desc
                                        limit 3")
head(Violation_code_frequency)

#         Violation_Code Frequency_of_Tickets                                           
#1             21               768087
#2             36               662765
#3             38               542079

df_Violation_code_frequency <- data.frame(head(Violation_code_frequency))
ggplot(df_Violation_code_frequency, aes(x= as.factor(Violation_Code), y=Frequency_of_Tickets)) +geom_col() + xlab("Violation Code") + ylab("Frequency of Tickets") + ggtitle("Violation Code vs. Frequency of Tickets") + geom_text(aes(label=Frequency_of_Tickets),vjust=-0.3)

##7B

############ This question is not clear, If I understood that we need to take the average of all the fines simply by two areas then the code is below ####
Manhattan_below_area <- c(515,515,515,115,115,265,50,115,115,115,115,0,115,115,NA,95,95,115,115,65,
                          65,115,0,0,115,115,180,95,515,515,115,65,NA,NA,65,50,65,65,65,115,
                          NA,65,NA,65,115,115,115,115,95,115,115,115,115,115,115,115,65,65,115,65,
                          65,65,95,95,95,65,165,65,65,65, 65,65,65,65,65,NA,65,65,115,60,
                          95,115,65,65,65,115,NA,NA,115,NA, 65,65,65,100,NA,95,65,95,0)

Other_area <- c(515,515,515,NA,115,265,50,115,115,115,115,0,115,115,NA,95,95,115,115,60,
                45,115,0,0,115,115,180,95,515,515,115,35,NA,NA,35,50,35,35,60,115,
                NA,35,NA,35,115,115,115,115,95,115,115,115,115,115,115,115,65,45,115,45,
                45,45,95,95,95,45,165,65,65,65, 65,65,65,65,65,NA,45,65,115,45,
                95,115,65,45,65, 115,NA,NA,115,NA, 45,45,65,200,NA,95,45,95,0)

fines_Dataset <- data.frame(c(1:99),Manhattan_below_area,Other_area)

names(fines_Dataset)[1] <- "Violation_Code"

mean(fines_Dataset[,1],na.rm = TRUE)
#Average value of fines in Manhattan 96th St. & below area is 50

mean(fines_Dataset[,2],na.rm = TRUE)
#Average value of other areas are 112.8

## 7 C
# sum of the fines for Manhattan 96th St. & below
head(SparkR::sql("SELECT SUM(CASE WHEN Violation_Code = 21 THEN 65 
                 WHEN Violation_Code = 36 THEN 50 
                 WHEN Violation_Code = 38 THEN 65 END) as Violation_Fine_Sum,Violation_Code
                 from ny_parking_2017_tbl
                 GROUP BY Violation_Code 
                 ORDER BY Violation_Fine_Sum desc LIMIT 3"))

#   Violation_Fine_Sum Violation_Code                                             
#1           49925655             21
#2           35235135             38
#3           33138250             36

# sum of the fines for other areas
head(SparkR::sql("SELECT SUM(CASE WHEN Violation_Code = 21 THEN 45 
                 WHEN Violation_Code = 36 THEN 50 
                 WHEN Violation_Code = 38 THEN 35 END) as Violation_Fine_Sum,Violation_Code
                 from ny_parking_2017_tbl
                 GROUP BY Violation_Code 
                 ORDER BY Violation_Fine_Sum desc LIMIT 3"))

#  Violation_Fine_Sum Violation_Code                                             
#1           34563915             21
#2           33138250             36
#3           18972765             38


########################## 7 Question Solution 2 #################

## 7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let's take an example of estimating that 
##  for the three most commonly occurring codes.
# 7.1 Find total occurrences of the three most common violation codes

Violation_code_frequency<- SparkR::sql("SELECT Violation_Code, count(*)as Frequency_of_Tickets
                                       from ny_parking_2017_tbl 
                                       group by Violation_Code
                                       order by Frequency_of_Tickets desc")
head(Violation_code_frequency,3)

#       Violation_Code Frequency_of_Tickets                                           
#1             21               768087
#2             36               662765
#3             38               542079


# A view needs to be created before a SparkDataFrame is used with SQL
createOrReplaceTempView(Violation_code_frequency, "ny_parking_2017_tbl")

## 7.2 It lists the fines associated with different violation codes. They’re divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. For simplicity, take an average of the two.
top3_fines_2017<- data.frame(head(Violation_code_frequency,3))
top3_fines_2017$Average_Fine_PerTicket<- c((65+45)/2,(50+50)/2,(65+65)/2)

## 7.3 Using this information, find the total amount collected for the three violation codes with maximum tickets. State the code which has the highest total collection.
top3_fines_2017$Total_Fine_Amount<- top3_fines_2017$Frequency_of_Tickets * top3_fines_2017$Average_Fine_PerTicket
top3_fines_2017

#        Violation_Code Frequency_of_Tickets Average_Fine_PerTicket Total_Fine_Amount
#1             21               768087                     55          42244785
#2             36               662765                     50          33138250
#3             38               542079                     65          35235135

ggplot(top3_fines_2017, aes(x= as.factor(Violation_Code), y=Total_Fine_Amount)) +geom_col() + xlab("Violation Code") + ylab("Total Fine Amount") + ggtitle("Violation Code vs. Total Fine Amount") + geom_text(aes(label=Total_Fine_Amount),vjust=-0.3)


# Stop the Spark Session
sparkR.stop
