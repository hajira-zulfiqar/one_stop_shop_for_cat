library(dplyr)
library(RMySQL)
library(writexl)
library(taskscheduleR)
library(mailR)
library(anytime)
library(zoo)
library(reshape2)
library(tidyverse)
library(tidyr)
#library(plyr)
memory.limit(size=56000)

#setwd("enter file path here")

# connecting to the db
jugnu_db <- dbConnect(MySQL(), 
                      user = '###########', 
                      password = '#########',
                      dbname = '#######',
                      host = '######################################')


#fetching data for the report
sale_loss_data <- dbGetQuery(jugnu_db, 
                             "SELECT DATE_FORMAT(sls.`DeliveryDate`, '%Y-%m') as DeliveryMonth,
sls.`DistributorId`,
sls.CategoryId,
sls.ManufacturerId,
sls.CategoryName,
sls.`ManufacturerName`,
SUM(sls.`DeliveredValueIncTax`) AS GMV,
SUM(sls.`OrderedValueIncTax`) AS OrderedValue,
SUM(sls.`OrderedValueIncTax`) /COUNT(DISTINCT(sls.ordernumber)) AS AOV,
COUNT(DISTINCT(sls.orderid))/COUNT(DISTINCT(sls.storecode)) AS PF,
COUNT(DISTINCT(sls.storecode)) AS StoreCount,
COUNT(DISTINCT(sls.ordernumber)) AS OrderCount,
SUM(sls.`DeliveredClaimableDiscountET`) AS ClaimableDiscount,
SUM(sls.`DeliveredNonClaimableDiscountET`) AS NonClaimableDiscount,
(SUM(sls.DeliveredValueExcTaxBeforeDiscount )- sum(sls.`WUnitPriceET` * sls.`DeliveredQTY`))/SUM(sls.DeliveredValueExcTaxBeforeDiscount) AS GrossMargin,
sls1.YesterdaysOrderedValue,
sls2.YesterdaysDeliveredValue
FROM
sale_loss_summary sls
LEFT JOIN (
SELECT
(sls.orderdate),
sls.categoryid,
sls.distributorid,
sls.manufacturerid,
SUM(sls.orderedvalueinctax) AS YesterdaysOrderedValue
FROM
sale_loss_summary AS sls
WHERE MONTH(sls.orderdate)>=2
AND YEAR(sls.orderdate)=2022
AND DAY(sls.orderdate)=(DAY(CURDATE())-1)
AND sls.orderstatus NOT IN (6,10)
AND sls.`DistributorId` IN (82,83,86,87,88,90)
GROUP BY sls.orderdate,sls.`DistributorId`,sls.`CategoryId`,sls.`ManufacturerId`) AS sls1
ON sls.categoryid=sls1.categoryid
AND sls.manufacturerid=sls1.manufacturerid
AND sls.distributorid=sls1.distributorid
AND MONTH(sls.deliverydate)=MONTH(sls1.orderdate)
LEFT JOIN (
SELECT
(sls.deliverydate),
sls.categoryid,
sls.distributorid,
sls.manufacturerid,
SUM(sls.deliveredvalueinctax) AS YesterdaysDeliveredValue
FROM
sale_loss_summary AS sls
WHERE MONTH(sls.deliverydate)>=2
AND YEAR(sls.deliverydate)=2022
AND DAY(sls.deliverydate)=(DAY(CURDATE())-1)
AND sls.orderstatus NOT IN (6,10)
AND sls.`DistributorId` IN (82,83,86,87,88,90)
GROUP BY sls.deliverydate,sls.`DistributorId`,sls.`CategoryId`,sls.`ManufacturerId`) AS sls2
ON sls.categoryid=sls2.categoryid
AND sls.manufacturerid=sls2.manufacturerid
AND sls.distributorid=sls2.distributorid
AND MONTH(sls.deliverydate)=MONTH(sls2.deliverydate)
WHERE sls.DeliveryDate >= '2022-02-01'
AND sls.`OrderStatus` NOT IN (6,10)
AND sls.`DistributorId` IN (82,83,86,87,88,90)
GROUP BY MONTH(sls.`DeliveryDate`),sls.`DistributorId`,sls.`CategoryId`,sls.`ManufacturerId`")

supplier_targets <- dbGetQuery(jugnu_db, "SELECT
st.CategoryId,
st.DistributorId,
st.ManufacturerId,
st.Month,
st.Year,
SUM(st.GMVTarget) AS GMVTarget
FROM
supplier_targets AS st
WHERE st.month>1
AND st.Year = 2022
GROUP BY st.`DistributorId`,st.`CategoryId`,st.`ManufacturerId`, st.Month")

end_stock_inventory <- dbGetQuery(jugnu_db, "SELECT
DATE_FORMAT(des.endstockdate, '%Y-%m') as EndStockMonth,
s.`CategoryId`,
s.`ManufacturerId`,
des.`DistributorId`,
SUM(des.WUnitPriceIT*des.closinginventory) AS ClosingInventoryValue
FROM daily_end_stock des
INNER JOIN skus AS s
ON des.`SKUId`=s.`SKUId`
WHERE MONTH(des.`EndStockDate`) >=2
AND YEAR(des.endstockdate)=2022
AND DAY(des.endstockdate)=(DAY(CURDATE())-1)
AND des.`DistributorId` IN (82,83,86,87,88,90)
AND des.`DistributorLocationId` IN (166,170,179,183,185,190)
GROUP BY des.endstockdate,des.`DistributorId`,s.`CategoryId`,s.`ManufacturerId`")


#converting date column into date format
#sale_loss_data$DeliveryMonth <- anydate(raw_data$DeliveryMonth)
#end_stock_inventory$endstockdate <- anydate(end_stock_inventory$endstockdate)

sale_loss_data$DistributorId <- as.character(sale_loss_data$DistributorId)
sale_loss_data$CategoryId <- as.character(sale_loss_data$CategoryId)
sale_loss_data$ManufacturerId <- as.character(sale_loss_data$ManufacturerId)

#supplier_targets$DistributorId <- as.character(supplier_targets$DistributorId)
#supplier_targets$CategoryId <- as.character(supplier_targets$CategoryId)
#supplier_targets$ManufacturerId <- as.character(supplier_targets$ManufacturerId)

#end_stock_inventory$DistributorId <- as.character(end_stock_inventory$DistributorId)
#end_stock_inventory$CategoryId <- as.character(end_stock_inventory$CategoryId)
#end_stock_inventory$ManufacturerId <- as.character(end_stock_inventory$ManufacturerId)

#creating a data frame for storing monthly average numbers
averages_data <- sale_loss_data %>%
  arrange(DeliveryMonth) %>%
  group_by(CategoryId, ManufacturerId, DistributorId) %>%
  #  filter(CategoryId == 12) %>%
  #  filter(DistributorId == 83) %>%
  #  filter(ManufacturerId %in% c(46,35)) %>%
  #  subset(DeliveryMonth, CategoryId, DistributorId, ManufacturerId, GMV) %>%
  dplyr::mutate(avg_gmv = (dplyr::lag(GMV) + dplyr::lag(GMV, n=2) + dplyr::lag(GMV,n=3))/3,
                avg_ordered_value = (dplyr::lag(OrderedValue) + dplyr::lag(OrderedValue, n=2) + dplyr::lag(OrderedValue,n=3))/3,
                avg_AOV = (dplyr::lag(AOV) + dplyr::lag(AOV, n=2) + dplyr::lag(AOV,n=3))/3,
                avg_PF = (dplyr::lag(PF) + dplyr::lag(PF, n=2) + dplyr::lag(PF,n=3))/3,
                avg_store_count = (dplyr::lag(StoreCount) + dplyr::lag(StoreCount, n=2) + dplyr::lag(StoreCount,n=3))/3,
                avg_order_count = (dplyr::lag(OrderCount) + dplyr::lag(OrderCount, n=2) + dplyr::lag(OrderCount,n=3))/3,
                avg_claimable_discount = (dplyr::lag(ClaimableDiscount) + dplyr::lag(ClaimableDiscount, n=2) + dplyr::lag(ClaimableDiscount,n=3))/3,
                avg_non_claimable_discount = (dplyr::lag(NonClaimableDiscount) + dplyr::lag(NonClaimableDiscount, n=2) + dplyr::lag(NonClaimableDiscount,n=3))/3,
                avg_gross_margin = (dplyr::lag(GrossMargin) + dplyr::lag(GrossMargin, n=2) + dplyr::lag(GrossMargin,n=3))/3,
                avg_yesterday_order_value = (dplyr::lag(YesterdaysOrderedValue) + dplyr::lag(YesterdaysOrderedValue, n=2) + dplyr::lag(YesterdaysOrderedValue,n=3))/3,
                avg_yesterday_delivered_value = (dplyr::lag(YesterdaysDeliveredValue) + dplyr::lag(YesterdaysDeliveredValue, n=2) + dplyr::lag(YesterdaysDeliveredValue,n=3))/3,
                timestamps = ifelse(is.na(avg_gmv), as.character(DeliveryMonth), "3 Month Average"))

#mutate(dplyr::lag.value = dplyr::dplyr::lag(value, n = 1, default = NA))

#writexl::write_xlsx(averages_data, "average_testing.xlsx")

#removing unwanted rows and columns
averages_data <- averages_data[,c("timestamps", "DistributorId", "CategoryId",  
                                  "ManufacturerId", "CategoryName", "ManufacturerName","avg_gmv",
                                  "avg_ordered_value", "avg_AOV", "avg_PF", "avg_store_count",
                                  "avg_order_count", "avg_claimable_discount", "avg_non_claimable_discount",
                                  "avg_gross_margin", "avg_yesterday_order_value",
                                  "avg_yesterday_delivered_value")]
#keeping results for averages only
averages_data <- averages_data %>%
  filter(timestamps == "3 Month Average")


#colnames(sale_loss_data)

colnames(averages_data) <- colnames(sale_loss_data)
#  c("DeliveryMonth", "DistributorId", "CategoryId", "ManufacturerId", "GMV",
#                             "OrderedValue", "AOV", "PF", "StoreCount", "OrderCount", "ClaimableDiscount",
#                             "NonClaimableDiscount", "NetSelling", "TotalBuying", "YesterdaysOrderedValue",
#                             "YesterdaysDeliveredValue")

sale_loss_final <- rbind.fill(sale_loss_data, averages_data)

#sale_loss_final <- sale_loss_final %>%
#  mutate(DeliveryDate = ifelse(is.na(DeliveryMonths), as.character(DeliveryMonth), as.character(DeliveryMonths)))

#sale_loss_final <- sale_loss_final[,c(-1)]

#exporting raw data to excel
#writexl::write_xlsx(sale_loss_final, "sale_loss_final.xlsx")


# merging supplier targets and closing inventory data below #

#extracting month from end stock inventory
#end_stock_inventory <- end_stock_inventory %>%
#  dplyr::mutate(end_stock_month = paste0(format(Sys.Date(), "%Y"),"-",format(as.Date(endstockdate), "%m")))

supplier_targets <- supplier_targets %>%
  mutate(target_month = paste0(supplier_targets$Year,"-0",supplier_targets$Month))


#end_stock_inventory$end_stock_month <- anydate(end_stock_inventory$end_stock_month)
#supplier_targets$target_month <- anydate(supplier_targets$target_month)

#adjusting column numbers for matching
#supplier_targets <- supplier_targets[,-5]

#merging datasets
supplier_inventory_data <- left_join(x = end_stock_inventory, y = supplier_targets, 
                                     by = c("EndStockMonth" = "target_month",
                                            "DistributorId" = "DistributorId",
                                            "CategoryId" = "CategoryId", 
                                            "ManufacturerId" = "ManufacturerId"))

#converting data types
supplier_inventory_data$CategoryId <- as.character(supplier_inventory_data$CategoryId)
supplier_inventory_data$ManufacturerId <- as.character(supplier_inventory_data$ManufacturerId)
supplier_inventory_data$DistributorId <- as.character(supplier_inventory_data$DistributorId)
#supplier_inventory_data$target_month <- as.character(supplier_inventory_data$target_month)

#merging final data
data_for_pivot <- left_join(x = sale_loss_final, y = supplier_inventory_data,
                            by = c("DeliveryMonth" = "EndStockMonth",
                                   "DistributorId" = "DistributorId",
                                   "CategoryId" = "CategoryId",
                                   "ManufacturerId" = "ManufacturerId"))

data_for_pivot <- data_for_pivot[,-c(3,4,19,20)]


writexl::write_xlsx(data_for_pivot, "data_for_pivot.xlsx")
#melting down the dataset for pivot creation
#view_for_pivot = melt(transformed_data, id = c('DeliveryMonth', 'timestamp', 'DistributorId', 'ManufacturerId', 'CategoryId'))
#view_for_pivot = view_for_pivot[,c(-1)]

#?anydate
#changing column names
#colnames(view_for_pivot) <- c("Timestamp", "DistributorId", "ManufacturerId", "CategoryId", "Measure", "Numerics")

#exporting excel
#writexl::write_xlsx(view_for_pivot, "view_for_pivot.xlsx")
