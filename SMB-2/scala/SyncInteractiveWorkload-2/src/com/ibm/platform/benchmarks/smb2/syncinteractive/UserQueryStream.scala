package com.ibm.platform.benchmarks.smb2.syncinteractive

import org.apache.spark._
import java.io._
import java.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext

object UserQueryStream extends App {
  
  override def main(args: Array[String]): Unit = {
    
       
    var masterURL: String = null
    var seqIterations: Int = 0
    var queryDelay: Int = 0
    var seqIterationDelay: Int = 0
    var timingFilePath: String = null
    var tpcdsDbPath: String = null
    
    if (args.length == 5) {
      // Initialize parameter variable
      //masterURL = args(0)  // No URL format checking for now
      seqIterations = args(0).toInt  
      queryDelay = args(1).toInt         // Ignored for now due to scala sync errors on wait
      seqIterationDelay = args(2).toInt  // Ignored for now due to scala sync errors on wait
      timingFilePath = args(3)
      tpcdsDbPath = args(4)
      
      // Starting user query stream
      //println("Spark master URL: " + masterURL)
      println("Number of query sequence iterations: " + seqIterations)
      println("Delay between queries within each sequence: " + queryDelay)
      println("Delay between each query sequence: " + seqIterationDelay)
      println("Timing file path: " + timingFilePath)
      println("Path to TPC-DS database: " + tpcdsDbPath)
      
    } else {
      
      // Print out the correct usage of the application and exit
      println("Incorrect number of parameters!")
      println("Parameter sequence:")
      // println("0 - Spark master URL")
      println("0 - Number of query sequence iterations")
      println("1 - Delay between queries within each sequence")
      println("2 - Delay between each query sequence")
      println("3 - Timing file path")
      println("4 - Path to the TPC-DS database")
      
      return
      
    }
    
    
    // Queries used as static variables for now - store in an array
    // One array for queries and the other for sequences
    var queries:Array[String] = new Array[String](8)
    
    // Q19
    queries(0) = """select
  'i_brand_id',
  'i_brand',
  'i_manufact_id',
  'i_manufact',
  sum('ss_ext_sales_price') --ext_price
from
  date_dim,
  store_sales,
  item,
  customer,
  customer_address,
  store
where
  'd_date_sk' = 'ss_sold_date_sk'
  and 'ss_item_sk' = 'i_item_sk'
  and 'i_manager_id' = 7
  and 'd_moy' = 11
  and 'd_year' = 1999
  and 'ss_customer_sk' = 'c_customer_sk'
  and 'c_current_addr_sk' = 'ca_address_sk'
  and substr('ca_zip', 1, 5) <> substr('s_zip', 1, 5)
  and 'ss_store_sk' = 's_store_sk'
  and 'ss_sold_date_sk' between 2451484 and 2451513  -- partition key filter
group by
  'i_brand',
  'i_brand_id',
  'i_manufact_id',
  'i_manufact'
order by
  -- ext_price desc,
  'i_brand',
  'i_brand_id',
  'i_manufact_id',
  'i_manufact'
limit 100"""
    
    // Q42
    queries(1) = """select
  'd_year',
  'i_category_id',
  'i_category',
  sum('ss_ext_sales_price')
from
  date_dim,
  store_sales,
  item
where
  'd_date_sk' = 'ss_sold_date_sk'
  'd_date_sk' = 'ss_sold_date_sk'
  and 'ss_item_sk' = 'i_item_sk'
  and 'i_manager_id' = 1
  and 'd_moy' = 12
  and 'd_year' = 1998
  and 'ss_sold_date_sk' between 2451149 and 2451179  -- partition key filter
group by
  'd_year',
  'd_year',
  'i_category_id',
  'i_category'
order by
  sum('ss_ext_sales_price') desc,
  'd_year',
  'd_year',
  'i_category_id',
  'i_category'
limit 100"""
    
    // Q52
    queries(2) = """select
  'd_year',
  'i_brand_id',
  'i_brand',
  sum('ss_ext_sales_price') -- ext_price
from
  date_dim,
  store_sales,
  item
where
  'd_date_sk' = 'ss_sold_date_sk'
  and 'ss_item_sk' = 'i_item_sk'
  and 'i_manager_id' = 1
  and 'd_moy' = 12
  and 'd_year' = 1998
  and 'ss_sold_date_sk' between 2451149 and 2451179 -- added for partition pruning
group by
  'd_year',
  'i_brand',
  'i_brand_id'
order by
  'd_year',
  -- ext_price desc,
  'i_brand_id'
limit 100"""
    
    // Q55
    queries(3) = """select
  'i_brand_id',
  'i_brand brand',
  sum('ss_ext_sales_price') -- ext_price
from
  date_dim,
  store_sales,
  item
where
  'd_date_sk' = 'ss_sold_date_sk'
  and 'ss_item_sk' = 'i_item_sk'
  and 'i_manager_id' = 48
  and 'd_moy' = 11
  and 'd_year' = 2001
  and 'ss_sold_date_sk' between 2452215 and 2452244
group by
  'i_brand',
  'i_brand_id'
order by
  -- ext_price desc,
  'i_brand_id'
limit 100"""
    
    // Q63 - had to comment out contentious case statements and aliases from Cloudera version
    queries(4) = """select  * 
from (select 'i_manager_id'
             ,sum('ss_sales_price') -- 'sum_sales'
             ,avg(sum('ss_sales_price')) over (partition by 'i_manager_id') -- 'avg_monthly_sales'
      from item
          ,store_sales
          ,date_dim
          ,store
      where 'ss_item_sk' = 'i_item_sk'
        and 'ss_sold_date_sk' = 'd_date_sk'
	and 'ss_sold_date_sk' between 2452123 and	2452487
        and 'ss_store_sk' = 's_store_sk'
        and 'd_month_seq' in (1219,1219+1,1219+2,1219+3,1219+4,1219+5,1219+6,1219+7,1219+8,1219+9,1219+10,1219+11)
        and ((    'i_category' in ('Books','Children','Electronics')
              and 'i_class' in ('personal','portable','reference','self-help')
              and 'i_brand' in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    'i_category' in ('Women','Music','Men')
              and 'i_class' in ('accessories','classical','fragrances','pants')
              and 'i_brand' in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
group by 'i_manager_id', 'd_moy') tmp1
-- where case when avg(sum('ss_sales_price')) over (partition by 'i_manager_id') > 0 then abs (sum('ss_sales_price') - avg(sum('ss_sales_price')) over (partition by 'i_manager_id')) / avg(sum('ss_sales_price')) over (partition by 'i_manager_id') else null end > 0.1
order by 'i_manager_id'
        -- ,avg(sum('ss_sales_price')) over (partition by 'i_manager_id')
        -- ,sum('ss_sales_price')
limit 100"""
    
    // Q68
    queries(5) = """select
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  extended_price,
  extended_tax,
  list_price
from
  (select
    ss_ticket_number,
    ss_customer_sk,
    ca_city bought_city,
    sum(ss_ext_sales_price) extended_price,
    sum(ss_ext_list_price) list_price,
    sum(ss_ext_tax) extended_tax
  from
    store_sales,
    date_dim,
    store,
    household_demographics,
    customer_address
  where
    store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and date_dim.d_dom between 1 and 2
    and (household_demographics.hd_dep_count = 5
      or household_demographics.hd_vehicle_count = 3)
    and date_dim.d_year in (1999, 1999 + 1, 1999 + 2)
    and store.s_city in ('Midway', 'Fairview')
    -- partition key filter
    and ss_sold_date_sk in (2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301, 2451331, 
                             2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484, 2451485, 
                             2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637, 2451666, 
                             2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819, 2451820, 
                             2451850, 2451851, 2451880, 2451881, 2451911, 2451912, 2451942, 2451943, 2451970, 2451971, 2452001, 
                             2452002, 2452031, 2452032, 2452062, 2452063, 2452092, 2452093, 2452123, 2452124, 2452154, 2452155, 
                             2452184, 2452185, 2452215, 2452216, 2452245, 2452246) 
    --and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
    --and d_date between '1999-01-01' and '1999-03-31'
  group by
    ss_ticket_number,
    ss_customer_sk,
    ss_addr_sk,
    ca_city
  ) dn,
  customer,
  customer_address current_addr
where
  ss_customer_sk = c_customer_sk
  and customer.c_current_addr_sk = current_addr.ca_address_sk
  and current_addr.ca_city <> bought_city
order by
  c_last_name,
  ss_ticket_number
limit 100"""
    
    // Q73 - had to comment out pertition key filter statement
    queries(6) = """select
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
from
  (select
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  from
    store_sales,
    date_dim,
    store,
    household_demographics
  where
    store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and date_dim.d_dom between 1 and 2
    and (household_demographics.hd_buy_potential = '>10000'
      or household_demographics.hd_buy_potential = 'unknown')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then household_demographics.hd_dep_count / household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (1998, 1998 + 1, 1998 + 2)
    and store.s_county in ('Fairfield County','Ziebach County','Bronx County','Barrow County')
    -- partition key filter
    and ss_sold_date_sk in (2450815, 2450816, 2450846, 2450847, 2450874, 2450875, 2450905, 2450906, 2450935, 2450936, 2450966, 2450967, 
                            2450996, 2450997, 2451027, 2451028, 2451058, 2451059, 2451088, 2451089, 2451119, 2451120, 2451149, 
                            2451150, 2451180, 2451181, 2451211, 2451212, 2451239, 2451240, 2451270, 2451271, 2451300, 2451301, 
                            2451331, 2451332, 2451361, 2451362, 2451392, 2451393, 2451423, 2451424, 2451453, 2451454, 2451484, 
                            2451485, 2451514, 2451515, 2451545, 2451546, 2451576, 2451577, 2451605, 2451606, 2451636, 2451637, 
                            2451666, 2451667, 2451697, 2451698, 2451727, 2451728, 2451758, 2451759, 2451789, 2451790, 2451819, 
                            2451820, 2451850, 2451851, 2451880, 2451881)    
    --and ss_sold_date_sk between 2451180 and 2451269 -- partition key filter (3 months)
  group by
    ss_ticket_number,
    ss_customer_sk
  ) dj,
  customer
where
  ss_customer_sk = c_customer_sk
  and cnt between 1 and 5
order by
  cnt desc"""
    
    // Q98
    queries(7) = """select
  'i_item_desc',
  'i_category',
  'i_class',
  'i_current_price',
  sum('ss_ext_sales_price') as itemrevenue,
  sum('ss_ext_sales_price') * 100 / sum(sum('ss_ext_sales_price')) over (partition by 'i_class') as revenueratio
from
  store_sales,
  date_dim,
  item
where
  'ss_item_sk' = 'i_item_sk'
  and 'i_category' in ('Jewelry', 'Sports', 'Books')
  and 'ss_sold_date_sk' = 'd_date_sk'
  and 'ss_sold_date_sk' between 2451911 and 2451941  -- partition key filter (1 calendar month)
  and 'd_date' between '2001-01-01' and '2001-01-31'
group by
  'i_item_id',
  'i_item_desc',
  'i_category',
  'i_class',
  'i_current_price'
order by
  'i_category',
  'i_class',
  'i_item_id',
  'i_item_desc',
  revenueratio"""
    
    // Store query labels for better log readability and easier data processing
    val queryLabels = Array.ofDim[String](8)
    
    queryLabels(0) = "Q19"
    queryLabels(1) = "Q42"
    queryLabels(2) = "Q52"
    queryLabels(3) = "Q55"
    queryLabels(4) = "Q63"
    queryLabels(5) = "Q68"
    queryLabels(6) = "Q73"
    queryLabels(7) = "Q98"
    
    // Store sequences as two dimensional array of integers
    val sequences = Array.ofDim[Int](10, 8)
    
    sequences (0)(0) = 0
    sequences (0)(1) = 1
    sequences (0)(2) = 2
    sequences (0)(3) = 3
    sequences (0)(4) = 4
    sequences (0)(5) = 5
    sequences (0)(6) = 6
    sequences (0)(7) = 7
      
    sequences (1)(0) = 1
    sequences (1)(1) = 2
    sequences (1)(2) = 3
    sequences (1)(3) = 0
    sequences (1)(4) = 5
    sequences (1)(5) = 6
    sequences (1)(6) = 7
    sequences (1)(7) = 4
    
    sequences (2)(0) = 2
    sequences (2)(1) = 0
    sequences (2)(2) = 3
    sequences (2)(3) = 5
    sequences (2)(4) = 6
    sequences (2)(5) = 7
    sequences (2)(6) = 4
    sequences (2)(7) = 1
    
    sequences (3)(0) = 5
    sequences (3)(1) = 0
    sequences (3)(2) = 3
    sequences (3)(3) = 6
    sequences (3)(4) = 7
    sequences (3)(5) = 4
    sequences (3)(6) = 1
    sequences (3)(7) = 2
    
    sequences (4)(0) = 4
    sequences (4)(1) = 0
    sequences (4)(2) = 3
    sequences (4)(3) = 7
    sequences (4)(4) = 6
    sequences (4)(5) = 1
    sequences (4)(6) = 2
    sequences (4)(7) = 5
    
    sequences (5)(0) = 6
    sequences (5)(1) = 4
    sequences (5)(2) = 0
    sequences (5)(3) = 3
    sequences (5)(4) = 7
    sequences (5)(5) = 1
    sequences (5)(6) = 5
    sequences (5)(7) = 2
    
    sequences (6)(0) = 7
    sequences (6)(1) = 5
    sequences (6)(2) = 4
    sequences (6)(3) = 0
    sequences (6)(4) = 3
    sequences (6)(5) = 1
    sequences (6)(6) = 6
    sequences (6)(7) = 2
    
    sequences (7)(0) = 3
    sequences (7)(1) = 2
    sequences (7)(2) = 5
    sequences (7)(3) = 4
    sequences (7)(4) = 0
    sequences (7)(5) = 1
    sequences (7)(6) = 6
    sequences (7)(7) = 7
    
    sequences (8)(0) = 0
    sequences (8)(1) = 7
    sequences (8)(2) = 1
    sequences (8)(3) = 2
    sequences (8)(4) = 3
    sequences (8)(5) = 4
    sequences (8)(6) = 5
    sequences (8)(7) = 6
    
    sequences (9)(0) = 1
    sequences (9)(1) = 0
    sequences (9)(2) = 5
    sequences (9)(3) = 2
    sequences (9)(4) = 3
    sequences (9)(5) = 6
    sequences (9)(6) = 7
    sequences (9)(7) = 4
    
    // Used for debugging - create a local spark instance
    // val conf = new SparkConf().setAppName("UserQueryStream").setMaster("local")
    // val sc = new SparkContext(conf)
    
    // Initialize Spark Context
    val sc = SparkContext.getOrCreate()
    
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    /////////////customer////////////////////////////////
    
    val customerDataPath = tpcdsDbPath + "/customer/customer.dat"
    val customer = sc.textFile(customerDataPath)
    
    /* Cloudera schema definition for Customer for reference
    
    create external table et_customer
(
  c_customer_sk             bigint,
  c_customer_id             string,
  c_current_cdemo_sk        bigint,
  c_current_hdemo_sk        bigint,
  c_current_addr_sk         bigint,
  c_first_shipto_date_sk    bigint,
  c_first_sales_date_sk     bigint,
  c_salutation              string,
  c_first_name              string,
  c_last_name               string,
  c_preferred_cust_flag     string,
  c_birth_day               int,
  c_birth_month             int,
  c_birth_year              int,
  c_birth_country           string,
  c_login                   string,
  c_email_address           string,
  c_last_review_date        string
)
    */
    
    val customerSchema =
    StructType(
     StructField("c_customer_sk", IntegerType, false) ::
     StructField("c_customer_id", StringType, true) ::
     StructField("c_current_cdemo_sk", IntegerType, true) ::
     StructField("c_current_hdemo_sk", IntegerType, true) ::
     StructField("c_current_addr_sk", IntegerType, true) ::
     StructField("c_first_shipto_date_sk", IntegerType, true) ::
     StructField("c_first_sales_date_sk", IntegerType, true) ::
     StructField("c_salutation", StringType, true) ::
     StructField("c_first_name", StringType, true) ::
     StructField("c_last_name", StringType, true) ::
     StructField("c_preferred_cust_flag", StringType, true) ::
     StructField("c_birth_day", IntegerType, true) ::
     StructField("c_birth_month", IntegerType, true) ::
     StructField("c_birth_year", IntegerType, true) ::
     StructField("c_birth_country", StringType, true) ::
     StructField("c_login", StringType, true) ::
     StructField("c_email_address", StringType, true) ::
     StructField("c_last_review_date", StringType, true) :: Nil)
    
    val customerRowRDD = customer.map(_.split("\\|", -1)).map(p => Row(
          p(0).trim.toInt, 
          p(1).trim, 
          if (p(2).trim == null || (p(2).trim == "")) 0 else p(2).trim.toInt,
          if (p(3).trim == null || (p(3).trim == "")) 0 else p(3).trim.toInt, 
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt, 
          if (p(5).trim == null || (p(5).trim == "")) 0 else p(5).trim.toInt,
          if (p(6).trim == null || (p(6).trim == "")) 0 else p(6).trim.toInt,
          p(7).trim,
          p(8).trim,
          p(9).trim,
          p(10).trim,
          if (p(11).trim == null || (p(11).trim == "")) 0 else p(11).trim.toInt,
          if (p(12).trim == null || (p(12).trim == "")) 0 else p(12).trim.toInt,
          if (p(13).trim == null || (p(13).trim == "")) 0 else p(13).trim.toInt,
          p(14).trim,
          p(15).trim,
          p(16).trim,
          p(17).trim))
  
    val customerDataFrame = sqlContext.createDataFrame(customerRowRDD, customerSchema)
    customerDataFrame.registerTempTable("customer")  
    
    /////////////customer////////////////////////////////
    
    
    /////////////customer_address////////////////////////////////
    
    val customerAddressDataPath = tpcdsDbPath + "/customer_address/customer_address.dat"
    val customer_address = sc.textFile(customerAddressDataPath)
    
    /* Cloudera schema definition for customer_address for reference
    create external table et_customer_address
(
  ca_address_sk             bigint,
  ca_address_id             string,
  ca_street_number          string,
  ca_street_name            string,
  ca_street_type            string,
  ca_suite_number           string,
  ca_city                   string,
  ca_county                 string,
  ca_state                  string,
  ca_zip                    string,
  ca_country                string,
  ca_gmt_offset             decimal(5,2),
  ca_location_type          string
)
    */
    
   val customerAddressSchema =
    StructType(
     StructField("ca_address_sk", IntegerType, false) ::
     StructField("ca_address_id", StringType, true) ::
     StructField("ca_street_number", StringType, true) ::
     StructField("ca_street_name", StringType, true) ::
     StructField("ca_street_type", StringType, true) ::
     StructField("ca_suite_number", StringType, true) ::
     StructField("ca_city", StringType, true) ::
     StructField("ca_county", StringType, true) ::
     StructField("ca_state", StringType, true) ::
     StructField("ca_zip", StringType, true) ::
     StructField("ca_country", StringType, true) ::
     StructField("ca_gmt_offset", DoubleType, true) ::
     StructField("ca_location_type", StringType, true) :: Nil)
     
     val customerAddressRowRDD = customer.map(_.split("\\|", -1)).map(p => Row(
          p(0).trim.toInt, 
          p(1).trim, 
          p(2).trim,
          p(3).trim, 
          p(4).trim, 
          p(5).trim,
          p(6).trim,
          p(7).trim,
          p(8).trim,
          p(9).trim,
          p(10).trim,
          if (p(11).trim == null || (p(11).trim == "")) "0".toDouble else p(11).trim.toDouble,
          p(12).trim))
     
    val customerAddressDataFrame = sqlContext.createDataFrame(customerAddressRowRDD, customerAddressSchema)
    customerAddressDataFrame.registerTempTable("customer_address")
    
     /////////////customer_address////////////////////////////////
    
    /////////////customer_demographics////////////////////////////////
   
    val customerDemographicsDataPath = tpcdsDbPath + "/customer_demographics/customer_demographics.dat"
    val customer_demographics = sc.textFile(customerDemographicsDataPath)
    
    /* Cloudera schema for customer_demographics for reference
    create external table et_customer_demographics
(
  cd_demo_sk                bigint,
  cd_gender                 string,
  cd_marital_status         string,
  cd_education_status       string,
  cd_purchase_estimate      int,
  cd_credit_rating          string,
  cd_dep_count              int,
  cd_dep_employed_count     int,
  cd_dep_college_count      int
)
* */
    
     val customerDemographicsSchema =
    StructType(
     StructField("cd_demo_sk", IntegerType, false) ::
     StructField("cd_gender", StringType, true) ::
     StructField("cd_marital_status", StringType, true) ::
     StructField("cd_education_status", StringType, true) ::
     StructField("cd_purchase_estimate", IntegerType, true) ::
     StructField("cd_credit_rating", StringType, true) ::
     StructField("cd_dep_count", IntegerType, true) ::
     StructField("cd_dep_employed_count", IntegerType, true) ::
     StructField("cd_dep_college_count", IntegerType, true) :: Nil)
     
     val customerDemographicsRowRDD = customer_demographics.map(_.split("\\|", -1)).map(p => Row(
          p(0).trim.toInt, 
          p(1).trim, 
          p(2).trim,
          p(3).trim, 
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt, 
          p(5).trim,
          if (p(6).trim == null || (p(6).trim == "")) 0 else p(6).trim.toInt,
          if (p(7).trim == null || (p(7).trim == "")) 0 else p(7).trim.toInt,
          if (p(8).trim == null || (p(8).trim == "")) 0 else p(8).trim.toInt))
     
    val customerDemographicsDataFrame = sqlContext.createDataFrame(customerDemographicsRowRDD, customerDemographicsSchema)
    customerDemographicsDataFrame.registerTempTable("customer_demographics")  
    
    /////////////customer_demographics////////////////////////////////
    
    /////////////date_dim////////////////////////////////
    
    val dateDimDataPath = tpcdsDbPath + "/date_dim/date_dim.dat"
    val date_dim = sc.textFile(dateDimDataPath)
 
    /* Cloudera schema for date_dim for reference
    create external table et_date_dim
(
  d_date_sk                 bigint,
  d_date_id                 string,
  d_date                    string, -- YYYY-MM-DD format
  d_month_seq               int,
  d_week_seq                int,
  d_quarter_seq             int,
  d_year                    int,
  d_dow                     int,
  d_moy                     int,
  d_dom                     int,
  d_qoy                     int,
  d_fy_year                 int,
  d_fy_quarter_seq          int,
  d_fy_week_seq             int,
  d_day_name                string,
  d_quarter_name            string,
  d_holiday                 string,
  d_weekend                 string,
  d_following_holiday       string,
  d_first_dom               int,
  d_last_dom                int,
  d_same_day_ly             int,
  d_same_day_lq             int,
  d_current_day             string,
  d_current_week            string,
  d_current_month           string,
  d_current_quarter         string,
  d_current_year            string
)
*/
    val dateDimensionSchema =
    StructType(
     StructField("d_date_sk", IntegerType, false) ::
     StructField("d_date_id", StringType, true) ::
     StructField("d_date", StringType, true) ::
     StructField("d_month_seq", IntegerType, true) ::
     StructField("d_week_seq", IntegerType, true) ::
     StructField("d_quarter_seq", IntegerType, true) ::
     StructField("d_year", IntegerType, true) ::
     StructField("d_dow", IntegerType, true) ::
     StructField("d_moy", IntegerType, true) ::
     StructField("d_dom", IntegerType, true) ::
     StructField("d_qoy", IntegerType, true) ::
     StructField("d_fy_year", IntegerType, true) ::
     StructField("d_fy_quarter_seq", IntegerType, true) ::
     StructField("d_fy_week_seq", IntegerType, true) ::
     StructField("d_day_name", StringType, true) ::
     StructField("d_quarter_name", StringType, true) ::
     StructField("d_holiday", StringType, true) ::
     StructField("d_weekend", StringType, true) ::
     StructField("d_following_holiday", StringType, true) ::
     StructField("d_first_dom", IntegerType, true) ::
     StructField("d_last_dom", IntegerType, true) ::
     StructField("d_same_day_ly", IntegerType, true) ::
     StructField("d_same_day_lq", IntegerType, true) ::
     StructField("d_current_day", StringType, true) ::
     StructField("d_current_week", StringType, true) ::
     StructField("d_current_month", StringType, true) ::
     StructField("d_current_quarter", StringType, true) ::
     StructField("d_current_year", StringType, true) :: Nil)
       
     
     val dateDimRowRDD = date_dim.map(_.split("\\|", -1)).map(p => Row(
          p(0).trim.toInt, 
          p(1).trim,
          p(2).trim,
          if (p(3).trim == null || (p(3).trim == "")) 0 else p(3).trim.toInt,
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt,
          if (p(5).trim == null || (p(5).trim == "")) 0 else p(5).trim.toInt,
          if (p(6).trim == null || (p(6).trim == "")) 0 else p(6).trim.toInt,
          if (p(7).trim == null || (p(7).trim == "")) 0 else p(7).trim.toInt,
          if (p(8).trim == null || (p(8).trim == "")) 0 else p(8).trim.toInt,
          if (p(9).trim == null || (p(9).trim == "")) 0 else p(9).trim.toInt,
          if (p(10).trim == null || (p(10).trim == "")) 0 else p(10).trim.toInt,
          if (p(11).trim == null || (p(11).trim == "")) 0 else p(11).trim.toInt,
          if (p(12).trim == null || (p(12).trim == "")) 0 else p(12).trim.toInt,
          if (p(13).trim == null || (p(13).trim == "")) 0 else p(13).trim.toInt,
          p(14).trim,
          p(15).trim,
          p(16).trim,
          p(17).trim,
          p(18).trim,
          if (p(19).trim == null || (p(19).trim == "")) 0 else p(19).trim.toInt,
          if (p(20).trim == null || (p(20).trim == "")) 0 else p(20).trim.toInt,
          if (p(21).trim == null || (p(21).trim == "")) 0 else p(21).trim.toInt,
          if (p(22).trim == null || (p(22).trim == "")) 0 else p(22).trim.toInt,
          p(23).trim,
          p(24).trim,
          p(25).trim,
          p(26).trim,
          p(27).trim))
     
    val dateDimDataFrame = sqlContext.createDataFrame(dateDimRowRDD, dateDimensionSchema)
    dateDimDataFrame.registerTempTable("date_dim")
     
    /////////////date_dim////////////////////////////////
    
    
    /////////////household_demographics////////////////////////////////
    
    val householdDemographicsDataPath = tpcdsDbPath + "/household_demographics/household_demographics.dat"
    val household_demographics = sc.textFile(householdDemographicsDataPath)
    
    /* - Cloudera definition of household_demographics for reference
    create external table et_household_demographics
    (
      hd_demo_sk                bigint,
      hd_income_band_sk         bigint,
      hd_buy_potential          string,
      hd_dep_count              int,
      hd_vehicle_count          int
    )
    */
    
    val hdSchema =
    StructType(
     StructField("hd_demo_sk", IntegerType, false) ::
     StructField("hd_income_band_sk", IntegerType, false) ::
     StructField("hd_buy_potential", StringType, false) ::
     StructField("hd_dep_count", IntegerType, false) ::
     StructField("hd_vehicle_count", IntegerType, false) :: Nil)
    
    val hdrowRDD = household_demographics.map(_.split("\\|", -1)).map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toInt, p(4).trim.toInt))
    val hdDataFrame = sqlContext.createDataFrame(hdrowRDD, hdSchema)
    hdDataFrame.registerTempTable("household_demographics")
    
    /////////////household_demographics////////////////////////////////
    
    
    /////////////item////////////////////////////////
    
    val itemDataPath = tpcdsDbPath + "/item/item.dat"
    val item = sc.textFile(itemDataPath)
    
    /*
    create external table et_item
(
  i_item_sk                 bigint,
  i_item_id                 string,
  i_rec_start_date          string,
  i_rec_end_date            string,
  i_item_desc               string,
  i_current_price           decimal(7,2),
  i_wholesale_cost          decimal(7,2),
  i_brand_id                int,
  i_brand                   string,
  i_class_id                int,
  i_class                   string,
  i_category_id             int,
  i_category                string,
  i_manufact_id             int,
  i_manufact                string,
  i_size                    string,
  i_formulation             string,
  i_color                   string,
  i_units                   string,
  i_container               string,
  i_manager_id              int,
  i_product_name            string
)*/
   
    val itemSchema =
    StructType(
     StructField("i_item_sk", IntegerType, false) ::
     StructField("i_item_id", StringType, false) ::
     StructField("i_rec_start_date", StringType, false) ::
     StructField("i_rec_end_date", StringType, false) ::
     StructField("i_item_desc", StringType, false) ::
     StructField("i_current_price", DoubleType, false) ::
     StructField("i_wholesale_cost", DoubleType, false) ::
     StructField("i_brand_id", IntegerType, false) ::
     StructField("i_brand", StringType, false) ::
     StructField("i_class_id", IntegerType, false) ::
     StructField("i_class", StringType, false) ::
     StructField("i_category_id", IntegerType, false) ::
     StructField("i_category", StringType, false) ::
     StructField("i_manufact_id", IntegerType, false) ::
     StructField("i_manufact", StringType, false) ::
     StructField("i_size", StringType, false) ::
     StructField("i_formulation", StringType, false) ::
     StructField("i_color", StringType, false) ::
     StructField("i_units", StringType, false) ::
     StructField("i_container", StringType, false) ::
     StructField("i_manager_id", IntegerType, false) ::
     StructField("i_product_name", StringType, true) :: Nil)
    
    val itemRowRDD = item.map(_.split("\\|", -1)).map(p => Row(
          p(0).trim.toInt,
          p(1).trim,
          p(2).trim,
          p(3).trim,
          p(4).trim,
          if (p(5).trim == null || (p(5).trim == "")) "0".toDouble else p(5).trim.toDouble,
          if (p(6).trim == null || (p(6).trim == "")) "0".toDouble else p(6).trim.toDouble,
          if (p(7).trim == null || (p(7).trim == "")) 0 else p(7).trim.toInt,
          p(8).trim,
          if (p(9).trim == null || (p(9).trim == "")) 0 else p(9).trim.toInt,
          p(10).trim,
          if (p(11).trim == null || (p(11).trim == "")) 0 else p(11).trim.toInt,
          p(12).trim,
          if (p(13).trim == null || (p(13).trim == "")) 0 else p(13).trim.toInt,
          p(14).trim,
          p(15).trim,
          p(16).trim,
          p(17).trim,
          p(18).trim,
          p(19).trim,
          if (p(20).trim == null || (p(20).trim == "")) 0 else p(20).trim.toInt,
          p(21).trim))
     
    val itemDataFrame = sqlContext.createDataFrame(itemRowRDD, itemSchema)
    itemDataFrame.registerTempTable("item")
     
    /////////////item////////////////////////////////
    
    /////////////promotion////////////////////////////////
    
    val promotionDataPath = tpcdsDbPath + "/promotion/promotion.dat"
    val promotion = sc.textFile(promotionDataPath)
    
    /* Cloudera definition of promotion schema for reference
    create external table et_promotion
(
  p_promo_sk                bigint,
  p_promo_id                string,
  p_start_date_sk           bigint,
  p_end_date_sk             bigint,
  p_item_sk                 bigint,
  p_cost                    decimal(15,2),
  p_response_target         int,
  p_promo_name              string,
  p_channel_dmail           string,
  p_channel_email           string,
  p_channel_catalog         string,
  p_channel_tv              string,
  p_channel_radio           string,
  p_channel_press           string,
  p_channel_event           string,
  p_channel_demo            string,
  p_channel_details         string,
  p_purpose                 string,
  p_discount_active         string
)*/
    
    val promotionSchema =
    StructType(
     StructField("p_promo_sk", IntegerType, false) ::
     StructField("p_promo_id", StringType, true) ::
     StructField("p_start_date_sk", IntegerType, true) ::
     StructField("p_end_date_sk", IntegerType, true) ::
     StructField("p_item_sk", IntegerType, true) ::
     StructField("p_cost", DoubleType, true) ::
     StructField("p_response_target", IntegerType, true) ::
     StructField("p_promo_name", StringType, true) ::
     StructField("p_channel_dmail", StringType, true) ::
     StructField("p_channel_email", StringType, true) ::
     StructField("p_channel_catalog", StringType, true) ::
     StructField("p_channel_tv", StringType, true) ::
     StructField("p_channel_radio", StringType, true) ::
     StructField("p_channel_press", StringType, true) ::
     StructField("p_channel_event", StringType, true) ::
     StructField("p_channel_demo", StringType, true) ::
     StructField("p_channel_details", StringType, true) ::
     StructField("p_purpose", StringType, true) ::
     StructField("p_discount_active", StringType, true) :: Nil)
    
    val promotionRowRDD = promotion.map(_.split("\\|", -1)).map(p => Row(
          p(0).trim.toInt,
          p(1).trim,
          if (p(2).trim == null || (p(2).trim == "")) 0 else p(2).trim.toInt,
          if (p(3).trim == null || (p(3).trim == "")) 0 else p(3).trim.toInt,
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt,
          if (p(5).trim == null || (p(5).trim == "")) "0".toDouble else p(5).trim.toDouble,
          if (p(6).trim == null || (p(6).trim == "")) 0 else p(6).trim.toInt,
          p(8).trim,
          p(9).trim,
          p(10).trim,
          p(11).trim,
          p(12).trim,
          p(13).trim,
          p(14).trim,
          p(15).trim,
          p(16).trim,
          p(17).trim,
          p(18).trim))
     
    val promotionDataFrame = sqlContext.createDataFrame(promotionRowRDD, promotionSchema)
    promotionDataFrame.registerTempTable("promotion")
    
    /////////////promotion////////////////////////////////
    
    /////////////store////////////////////////////////
    
    val storeDataPath = tpcdsDbPath + "/store/store.dat"
    val store = sc.textFile(storeDataPath)
    
    /*
    create external table et_store
(
  s_store_sk                bigint,
  s_store_id                string,
  s_rec_start_date          string,
  s_rec_end_date            string,
  s_closed_date_sk          bigint,
  s_store_name              string,
  s_number_employees        int,
  s_floor_space             int,
  s_hours                   string,
  s_manager                 string,
  s_market_id               int,
  s_geography_class         string,
  s_market_desc             string,
  s_market_manager          string,
  s_division_id             int,
  s_division_name           string,
  s_company_id              int,
  s_company_name            string,
  s_street_number           string,
  s_street_name             string,
  s_street_type             string,
  s_suite_number            string,
  s_city                    string,
  s_county                  string,
  s_state                   string,
  s_zip                     string,
  s_country                 string,
  s_gmt_offset              decimal(5,2),
  s_tax_precentage          decimal(5,2)
)
    */
    
    val storeSchema =
    StructType(
     StructField("s_store_sk", IntegerType, false) ::
     StructField("s_store_id", StringType, true) ::
     StructField("s_rec_start_date", StringType, true) ::
     StructField("s_rec_end_date", StringType, true) ::
     StructField("s_closed_date_sk", IntegerType, true) ::
     StructField("s_store_name", StringType, true) ::
     StructField("s_number_employees", IntegerType, true) ::
     StructField("s_floor_space", IntegerType, true) ::
     StructField("s_hours", StringType, true) ::
     StructField("s_manager", StringType, true) ::
     StructField("s_market_id", IntegerType, true) ::
     StructField("s_geography_class", StringType, true) ::
     StructField("s_market_desc", StringType, true) ::
     StructField("s_market_manager", StringType, true) ::
     StructField("s_division_id", IntegerType, true) ::
     StructField("s_division_name", StringType, true) ::
     StructField("s_company_id", IntegerType, true) ::
     StructField("s_company_name", StringType, true) ::
     StructField("s_street_number", StringType, true) ::
     StructField("s_street_name", StringType, true) ::
     StructField("s_street_type", StringType, true) ::
     StructField("s_suite_number", StringType, true) ::
     StructField("s_city", StringType, true) ::
     StructField("s_county", StringType, true) ::
     StructField("s_state", StringType, true) ::
     StructField("s_zip", StringType, true) ::
     StructField("s_country", StringType, true) ::
     StructField("s_gmt_offset", DoubleType, true) ::
     StructField("s_tax_precentage", DoubleType, true) :: Nil)
    
    val storeRowRDD = store.map(_.split("\\|", -1)).map(p => Row(
          p(0).trim.toInt,
          p(1).trim,
          p(2).trim,
          p(3).trim,
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt,          
          p(5).trim,
          if (p(6).trim == null || (p(6).trim == "")) 0 else p(6).trim.toInt,
          if (p(7).trim == null || (p(7).trim == "")) 0 else p(7).trim.toInt,
          p(8).trim,
          p(9).trim,
          if (p(10).trim == null || (p(10).trim == "")) 0 else p(10).trim.toInt,
          p(11).trim,
          p(12).trim,
          p(13).trim,
          if (p(14).trim == null || (p(14).trim == "")) 0 else p(14).trim.toInt,
          p(15).trim,
          if (p(16).trim == null || (p(16).trim == "")) 0 else p(16).trim.toInt,
          p(17).trim,
          p(18).trim,
          p(19).trim,
          p(20).trim,
          p(21).trim,
          p(22).trim,
          p(23).trim,
          p(24).trim,
          p(25).trim,
          p(26).trim,
          if (p(27).trim == null || (p(27).trim == "")) "0".toDouble else p(27).trim.toDouble,
          if (p(28).trim == null || (p(28).trim == "")) "0".toDouble else p(28).trim.toDouble))
          
    val storeDataFrame = sqlContext.createDataFrame(storeRowRDD, storeSchema)
    storeDataFrame.registerTempTable("store")
    
    /////////////store////////////////////////////////
    
    /////////////store_sales////////////////////////////////
    
    val storeSalesDataPath = tpcdsDbPath + "/store_sales"
    val store_sales = sc.textFile(storeSalesDataPath)
    
    /* Cloudera definition of store_sales
    create external table et_store_sales
(
  ss_sold_date_sk           bigint,
  ss_sold_time_sk           bigint,
  ss_item_sk                bigint,
  ss_customer_sk            bigint,
  ss_cdemo_sk               bigint,
  ss_hdemo_sk               bigint,
  ss_addr_sk                bigint,
  ss_store_sk               bigint,
  ss_promo_sk               bigint,
  ss_ticket_number          bigint,
  ss_quantity               int,
  ss_wholesale_cost         decimal(7,2),
  ss_list_price             decimal(7,2),
  ss_sales_price            decimal(7,2),
  ss_ext_discount_amt       decimal(7,2),
  ss_ext_sales_price        decimal(7,2),
  ss_ext_wholesale_cost     decimal(7,2),
  ss_ext_list_price         decimal(7,2),
  ss_ext_tax                decimal(7,2),
  ss_coupon_amt             decimal(7,2),
  ss_net_paid               decimal(7,2),
  ss_net_paid_inc_tax       decimal(7,2),
  ss_net_profit             decimal(7,2)
)*/
    
    val storeSalesSchema =
    StructType(
     StructField("ss_sold_date_sk", IntegerType, false) ::
     StructField("ss_sold_time_sk", IntegerType, true) ::
     StructField("ss_item_sk", IntegerType, true) ::
     StructField("ss_customer_sk", IntegerType, true) ::
     StructField("ss_cdemo_sk", IntegerType, true) ::
     StructField("ss_hdemo_sk", IntegerType, true) ::
     StructField("ss_addr_sk", IntegerType, true) ::
     StructField("ss_store_sk", IntegerType, true) ::
     StructField("ss_promo_sk", IntegerType, true) ::
     StructField("ss_ticket_number", IntegerType, true) ::
     StructField("ss_quantity", IntegerType, true) ::
     StructField("ss_wholesale_cost", DoubleType, true) ::
     StructField("ss_list_price", DoubleType, true) ::
     StructField("ss_sales_price", DoubleType, true) ::
     StructField("ss_ext_discount_amt", DoubleType, true) ::
     StructField("ss_ext_sales_price", DoubleType, true) ::
     StructField("ss_ext_wholesale_cost", DoubleType, true) ::
     StructField("ss_ext_list_price", DoubleType, true) ::
     StructField("ss_ext_tax", DoubleType, true) ::
     StructField("ss_coupon_amt", DoubleType, true) ::
     StructField("ss_net_paid", DoubleType, true) ::
     StructField("ss_net_paid_inc_tax", DoubleType, true) ::
     StructField("ss_net_profit", DoubleType, true) :: Nil)
   
    val storeSalesRowRDD = store_sales.map(_.split("\\|", -1)).map(p => Row(
          if (p(0).trim == null || (p(0).trim == "")) 0 else p(0).trim.toInt,
          if (p(1).trim == null || (p(1).trim == "")) 0 else p(1).trim.toInt,
          if (p(2).trim == null || (p(2).trim == "")) 0 else p(2).trim.toInt,
          if (p(3).trim == null || (p(3).trim == "")) 0 else p(3).trim.toInt,
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt,
          if (p(5).trim == null || (p(5).trim == "")) 0 else p(5).trim.toInt,
          if (p(6).trim == null || (p(6).trim == "")) 0 else p(6).trim.toInt,
          if (p(7).trim == null || (p(7).trim == "")) 0 else p(7).trim.toInt,
          if (p(8).trim == null || (p(8).trim == "")) 0 else p(8).trim.toInt,
          if (p(9).trim == null || (p(9).trim == "")) 0 else p(9).trim.toInt,
          if (p(10).trim == null || (p(10).trim == "")) 0 else p(10).trim.toInt,
          if (p(11).trim == null || (p(11).trim == "")) "0".toDouble else p(11).trim.toDouble,
          if (p(12).trim == null || (p(12).trim == "")) "0".toDouble else p(12).trim.toDouble,
          if (p(13).trim == null || (p(13).trim == "")) "0".toDouble else p(13).trim.toDouble,
          if (p(14).trim == null || (p(14).trim == "")) "0".toDouble else p(14).trim.toDouble,
          if (p(15).trim == null || (p(15).trim == "")) "0".toDouble else p(15).trim.toDouble,
          if (p(16).trim == null || (p(16).trim == "")) "0".toDouble else p(16).trim.toDouble,
          if (p(17).trim == null || (p(17).trim == "")) "0".toDouble else p(17).trim.toDouble,
          if (p(18).trim == null || (p(18).trim == "")) "0".toDouble else p(18).trim.toDouble,
          if (p(19).trim == null || (p(19).trim == "")) "0".toDouble else p(19).trim.toDouble,
          if (p(20).trim == null || (p(20).trim == "")) "0".toDouble else p(20).trim.toDouble,
          if (p(21).trim == null || (p(21).trim == "")) "0".toDouble else p(21).trim.toDouble,
          if (p(22).trim == null || (p(22).trim == "")) "0".toDouble else p(22).trim.toDouble))
    
    
    val storeSalesDataFrame = sqlContext.createDataFrame(storeSalesRowRDD, storeSalesSchema)
    storeSalesDataFrame.registerTempTable("store_sales")
    
    /////////////stostorere_sales////////////////////////////////
    
    
    /////////////time_dim////////////////////////////////
    
    val timeDimDataPath = tpcdsDbPath + "/time_dim/time_dim.dat"
    val time_dim = sc.textFile(timeDimDataPath)
 
    /*   
    create external table et_time_dim
(
  t_time_sk                 bigint,
  t_time_id                 string,
  t_time                    int,
  t_hour                    int,
  t_minute                  int,
  t_second                  int,
  t_am_pm                   string,
  t_shift                   string,
  t_sub_shift               string,
  t_meal_time               string
)*/
    
    val timeDimSchema =
    StructType(
     StructField("t_time_sk", IntegerType, false) ::
     StructField("t_time_id", StringType, true) ::
     StructField("t_time", IntegerType, true) ::
     StructField("t_hour", IntegerType, true) ::
     StructField("t_minute", IntegerType, true) ::
     StructField("t_second", IntegerType, true) ::
     StructField("t_am_pm", StringType, true) ::
     StructField("t_shift", StringType, true) ::
     StructField("t_sub_shift", StringType, true) ::
     StructField("t_meal_time", StringType, true) :: Nil)
    
    val timeDimRowRDD = store_sales.map(_.split("\\|", -1)).map(p => Row(
          if (p(0).trim == null || (p(0).trim == "")) 0 else p(0).trim.toInt,
          p(1).trim,
          if (p(2).trim == null || (p(2).trim == "")) 0 else p(2).trim.toInt,
          if (p(3).trim == null || (p(3).trim == "")) 0 else p(3).trim.toInt,
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt,
          if (p(5).trim == null || (p(5).trim == "")) 0 else p(5).trim.toInt,
          p(6).trim,
          p(7).trim,
          p(8).trim,
          p(9).trim))
    
    
    val timeDimDataFrame = sqlContext.createDataFrame(timeDimRowRDD, timeDimSchema)
    timeDimDataFrame.registerTempTable("time_dim")
     
    /////////////time_dim////////////////////////////////
    
    // Benchmark logic section
    
    // Create the timing file
    val pw = new PrintWriter(new File(timingFilePath))
    
    val i = 0
    val j = 0
    
    for (i <- 1 to seqIterations) {
      
      // Iterate sequences(seqID) to execute the queries
      // pick a sequence at random
      val r = scala.util.Random
      val seqID = r.nextInt(10)
      println("Executing query sequence: " + seqID)
     
      for (j <- 0 to 7) { 
      
        // run all the queries from the sequence with seqID
        val queryID = sequences(seqID)(j)
        val query = queries(queryID)
        val startTime = System.currentTimeMillis()
        
        // Execute query here
        println("Executing query: " + queryID)
        val queryDF = sqlContext.sql(query)
        val rowCount = queryDF.count  // touch the data to make sure the query actually executes
        var firstColValue : Any = "NONE"
        
        // if rows returned retreive the value of first column of first row
        if (rowCount > 0) {
          val rows =  queryDF.collect()
          firstColValue = rows(0).getString(0)
          if (firstColValue == "") firstColValue = "EMPTY"
          
        }
       
        // Log the query and the number of rows returned
        println(queryID + " " + " returned rows: " + rowCount)
         
        val finishTime = System.currentTimeMillis()
        val queryDuration = ((((finishTime - startTime)/1000)*10).round)/10  // Query duration is recorded in seconds
        
        // We don't need the full date, just the hours, minutes and seconds
        val date = new Date(startTime) 
        val sb = new StringBuffer(date.toString())
        val startDate = sb.substring(0, 10)
        val timeStamp = sb.substring(11, 19)
        
        // Write query time-stamp and duration into the timing file
        println(queryID + "," + queryLabels(queryID) + "," + startDate + "," + timeStamp + "," + queryDuration + "," + rowCount + "," + firstColValue)
        pw.println(queryID + "," + queryLabels(queryID) + "," + startDate + "," + timeStamp + "," + queryDuration + "," + rowCount + "," + firstColValue)
        
        pw.flush()
        
        
      }
 
      
    }

    // Close the timing file
    pw.close
   
    println("User stream complete!")
    
   }
  
}