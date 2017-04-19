package com.ibm.platform.benchmarks.smb2.asyncbatch

import org.apache.spark._
import java.io._
import java.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object AsyncBatchQueries extends App {
  
  override def main(args: Array[String]): Unit = {
    
    var queryID: String = null
    var tpcdsDbPath: String = null
    var intdataDirPath: String = null
    var userID: String = null
    var iterID: String = null
    
    var timingFilePath: String = null
    
    if (args.length == 5) {      // Initialize parameter variable
  
      queryID = args(0)
      tpcdsDbPath = args(1)
      intdataDirPath = args(2)
      userID = args(3)
      iterID = args(4)
     
      println("Starting async query execution with the following parameters:")
      println("Query ID: " + queryID)
      println("Path to TPC-DS database: " + tpcdsDbPath)
      println("Intermediate data dir path: " + intdataDirPath)
      println("User ID: " + userID)
      println("Iteration ID: " + iterID)
      
    } else {
      
      // Print out the correct usage of the application and exit
     
      println("1 - Query ID")
      println("2 - Path to the TPC-DS database")
      println("3 - Path to the intermediate data directory")
      println("4 - User ID")
      println("5 - Iteration ID")
      
      return
      
    }
    
    // Create the intermediate timing file path
    timingFilePath = intdataDirPath + "query-stream-results_" + userID + "_" + iterID + "_" + queryID + "_int.txt" 
    
    // Queries used as static variables for now - store in an array
    // One array for queries and the other for sequences
    var queries:Array[String] = new Array[String](4)
    
    // Q3
    queries(0) = """
      select
  dt.d_year,
  item.i_brand_id brand_id,
  item.i_brand brand,
  sum(ss_net_profit) sum_agg
from
  date_dim dt,
  store_sales,
  item
where
  dt.d_date_sk = store_sales.ss_sold_date_sk
  and store_sales.ss_item_sk = item.i_item_sk
  and item.i_manufact_id = 436
  and dt.d_moy = 12
  -- partition key filters
  and ( 
ss_sold_date_sk between 2415355 and 2415385
or ss_sold_date_sk between 2415720 and 2415750
or ss_sold_date_sk between 2416085 and 2416115
or ss_sold_date_sk between 2416450 and 2416480
or ss_sold_date_sk between 2416816 and 2416846
or ss_sold_date_sk between 2417181 and 2417211
or ss_sold_date_sk between 2417546 and 2417576
or ss_sold_date_sk between 2417911 and 2417941
or ss_sold_date_sk between 2418277 and 2418307
or ss_sold_date_sk between 2418642 and 2418672
or ss_sold_date_sk between 2419007 and 2419037
or ss_sold_date_sk between 2419372 and 2419402
or ss_sold_date_sk between 2419738 and 2419768
or ss_sold_date_sk between 2420103 and 2420133
or ss_sold_date_sk between 2420468 and 2420498
or ss_sold_date_sk between 2420833 and 2420863
or ss_sold_date_sk between 2421199 and 2421229
or ss_sold_date_sk between 2421564 and 2421594
or ss_sold_date_sk between 2421929 and 2421959
or ss_sold_date_sk between 2422294 and 2422324
or ss_sold_date_sk between 2422660 and 2422690
or ss_sold_date_sk between 2423025 and 2423055
or ss_sold_date_sk between 2423390 and 2423420
or ss_sold_date_sk between 2423755 and 2423785
or ss_sold_date_sk between 2424121 and 2424151
or ss_sold_date_sk between 2424486 and 2424516
or ss_sold_date_sk between 2424851 and 2424881
or ss_sold_date_sk between 2425216 and 2425246
or ss_sold_date_sk between 2425582 and 2425612
or ss_sold_date_sk between 2425947 and 2425977
or ss_sold_date_sk between 2426312 and 2426342
or ss_sold_date_sk between 2426677 and 2426707
or ss_sold_date_sk between 2427043 and 2427073
or ss_sold_date_sk between 2427408 and 2427438
or ss_sold_date_sk between 2427773 and 2427803
or ss_sold_date_sk between 2428138 and 2428168
or ss_sold_date_sk between 2428504 and 2428534
or ss_sold_date_sk between 2428869 and 2428899
or ss_sold_date_sk between 2429234 and 2429264
or ss_sold_date_sk between 2429599 and 2429629
or ss_sold_date_sk between 2429965 and 2429995
or ss_sold_date_sk between 2430330 and 2430360
or ss_sold_date_sk between 2430695 and 2430725
or ss_sold_date_sk between 2431060 and 2431090
or ss_sold_date_sk between 2431426 and 2431456
or ss_sold_date_sk between 2431791 and 2431821
or ss_sold_date_sk between 2432156 and 2432186
or ss_sold_date_sk between 2432521 and 2432551
or ss_sold_date_sk between 2432887 and 2432917
or ss_sold_date_sk between 2433252 and 2433282
or ss_sold_date_sk between 2433617 and 2433647
or ss_sold_date_sk between 2433982 and 2434012
or ss_sold_date_sk between 2434348 and 2434378
or ss_sold_date_sk between 2434713 and 2434743
or ss_sold_date_sk between 2435078 and 2435108
or ss_sold_date_sk between 2435443 and 2435473
or ss_sold_date_sk between 2435809 and 2435839
or ss_sold_date_sk between 2436174 and 2436204
or ss_sold_date_sk between 2436539 and 2436569
or ss_sold_date_sk between 2436904 and 2436934
or ss_sold_date_sk between 2437270 and 2437300
or ss_sold_date_sk between 2437635 and 2437665
or ss_sold_date_sk between 2438000 and 2438030
or ss_sold_date_sk between 2438365 and 2438395
or ss_sold_date_sk between 2438731 and 2438761
or ss_sold_date_sk between 2439096 and 2439126
or ss_sold_date_sk between 2439461 and 2439491
or ss_sold_date_sk between 2439826 and 2439856
or ss_sold_date_sk between 2440192 and 2440222
or ss_sold_date_sk between 2440557 and 2440587
or ss_sold_date_sk between 2440922 and 2440952
or ss_sold_date_sk between 2441287 and 2441317
or ss_sold_date_sk between 2441653 and 2441683
or ss_sold_date_sk between 2442018 and 2442048
or ss_sold_date_sk between 2442383 and 2442413
or ss_sold_date_sk between 2442748 and 2442778
or ss_sold_date_sk between 2443114 and 2443144
or ss_sold_date_sk between 2443479 and 2443509
or ss_sold_date_sk between 2443844 and 2443874
or ss_sold_date_sk between 2444209 and 2444239
or ss_sold_date_sk between 2444575 and 2444605
or ss_sold_date_sk between 2444940 and 2444970
or ss_sold_date_sk between 2445305 and 2445335
or ss_sold_date_sk between 2445670 and 2445700
or ss_sold_date_sk between 2446036 and 2446066
or ss_sold_date_sk between 2446401 and 2446431
or ss_sold_date_sk between 2446766 and 2446796
or ss_sold_date_sk between 2447131 and 2447161
or ss_sold_date_sk between 2447497 and 2447527
or ss_sold_date_sk between 2447862 and 2447892
or ss_sold_date_sk between 2448227 and 2448257
or ss_sold_date_sk between 2448592 and 2448622
or ss_sold_date_sk between 2448958 and 2448988
or ss_sold_date_sk between 2449323 and 2449353
or ss_sold_date_sk between 2449688 and 2449718
or ss_sold_date_sk between 2450053 and 2450083
or ss_sold_date_sk between 2450419 and 2450449
or ss_sold_date_sk between 2450784 and 2450814
or ss_sold_date_sk between 2451149 and 2451179
or ss_sold_date_sk between 2451514 and 2451544
or ss_sold_date_sk between 2451880 and 2451910
or ss_sold_date_sk between 2452245 and 2452275
or ss_sold_date_sk between 2452610 and 2452640
or ss_sold_date_sk between 2452975 and 2453005
or ss_sold_date_sk between 2453341 and 2453371
or ss_sold_date_sk between 2453706 and 2453736
or ss_sold_date_sk between 2454071 and 2454101
or ss_sold_date_sk between 2454436 and 2454466
or ss_sold_date_sk between 2454802 and 2454832
or ss_sold_date_sk between 2455167 and 2455197
or ss_sold_date_sk between 2455532 and 2455562
or ss_sold_date_sk between 2455897 and 2455927
or ss_sold_date_sk between 2456263 and 2456293
or ss_sold_date_sk between 2456628 and 2456658
or ss_sold_date_sk between 2456993 and 2457023
or ss_sold_date_sk between 2457358 and 2457388
or ss_sold_date_sk between 2457724 and 2457754
or ss_sold_date_sk between 2458089 and 2458119
or ss_sold_date_sk between 2458454 and 2458484
or ss_sold_date_sk between 2458819 and 2458849
or ss_sold_date_sk between 2459185 and 2459215
or ss_sold_date_sk between 2459550 and 2459580
or ss_sold_date_sk between 2459915 and 2459945
or ss_sold_date_sk between 2460280 and 2460310
or ss_sold_date_sk between 2460646 and 2460676
or ss_sold_date_sk between 2461011 and 2461041
or ss_sold_date_sk between 2461376 and 2461406
or ss_sold_date_sk between 2461741 and 2461771
or ss_sold_date_sk between 2462107 and 2462137
or ss_sold_date_sk between 2462472 and 2462502
or ss_sold_date_sk between 2462837 and 2462867
or ss_sold_date_sk between 2463202 and 2463232
or ss_sold_date_sk between 2463568 and 2463598
or ss_sold_date_sk between 2463933 and 2463963
or ss_sold_date_sk between 2464298 and 2464328
or ss_sold_date_sk between 2464663 and 2464693
or ss_sold_date_sk between 2465029 and 2465059
or ss_sold_date_sk between 2465394 and 2465424
or ss_sold_date_sk between 2465759 and 2465789
or ss_sold_date_sk between 2466124 and 2466154
or ss_sold_date_sk between 2466490 and 2466520
or ss_sold_date_sk between 2466855 and 2466885
or ss_sold_date_sk between 2467220 and 2467250
or ss_sold_date_sk between 2467585 and 2467615
or ss_sold_date_sk between 2467951 and 2467981
or ss_sold_date_sk between 2468316 and 2468346
or ss_sold_date_sk between 2468681 and 2468711
or ss_sold_date_sk between 2469046 and 2469076
or ss_sold_date_sk between 2469412 and 2469442
or ss_sold_date_sk between 2469777 and 2469807
or ss_sold_date_sk between 2470142 and 2470172
or ss_sold_date_sk between 2470507 and 2470537
or ss_sold_date_sk between 2470873 and 2470903
or ss_sold_date_sk between 2471238 and 2471268
or ss_sold_date_sk between 2471603 and 2471633
or ss_sold_date_sk between 2471968 and 2471998
or ss_sold_date_sk between 2472334 and 2472364
or ss_sold_date_sk between 2472699 and 2472729
or ss_sold_date_sk between 2473064 and 2473094
or ss_sold_date_sk between 2473429 and 2473459
or ss_sold_date_sk between 2473795 and 2473825
or ss_sold_date_sk between 2474160 and 2474190
or ss_sold_date_sk between 2474525 and 2474555
or ss_sold_date_sk between 2474890 and 2474920
or ss_sold_date_sk between 2475256 and 2475286
or ss_sold_date_sk between 2475621 and 2475651
or ss_sold_date_sk between 2475986 and 2476016
or ss_sold_date_sk between 2476351 and 2476381
or ss_sold_date_sk between 2476717 and 2476747
or ss_sold_date_sk between 2477082 and 2477112
or ss_sold_date_sk between 2477447 and 2477477
or ss_sold_date_sk between 2477812 and 2477842
or ss_sold_date_sk between 2478178 and 2478208
or ss_sold_date_sk between 2478543 and 2478573
or ss_sold_date_sk between 2478908 and 2478938
or ss_sold_date_sk between 2479273 and 2479303
or ss_sold_date_sk between 2479639 and 2479669
or ss_sold_date_sk between 2480004 and 2480034
or ss_sold_date_sk between 2480369 and 2480399
or ss_sold_date_sk between 2480734 and 2480764
or ss_sold_date_sk between 2481100 and 2481130
or ss_sold_date_sk between 2481465 and 2481495
or ss_sold_date_sk between 2481830 and 2481860
or ss_sold_date_sk between 2482195 and 2482225
or ss_sold_date_sk between 2482561 and 2482591
or ss_sold_date_sk between 2482926 and 2482956
or ss_sold_date_sk between 2483291 and 2483321
or ss_sold_date_sk between 2483656 and 2483686
or ss_sold_date_sk between 2484022 and 2484052
or ss_sold_date_sk between 2484387 and 2484417
or ss_sold_date_sk between 2484752 and 2484782
or ss_sold_date_sk between 2485117 and 2485147
or ss_sold_date_sk between 2485483 and 2485513
or ss_sold_date_sk between 2485848 and 2485878
or ss_sold_date_sk between 2486213 and 2486243
or ss_sold_date_sk between 2486578 and 2486608
or ss_sold_date_sk between 2486944 and 2486974
or ss_sold_date_sk between 2487309 and 2487339
or ss_sold_date_sk between 2487674 and 2487704
or ss_sold_date_sk between 2488039 and 2488069
)
group by
  dt.d_year,
  item.i_brand,
  item.i_brand_id
order by
  dt.d_year,
  sum_agg desc,
  brand_id
limit 100"""
    
    
    // Q8
    queries(1) = """
    select  s_store_name
      ,sum(ss_net_profit)
 from store_sales
     ,date_dim
     ,store,
     (select distinct a01.ca_zip
     from
     (SELECT substr(ca_zip,1,5) ca_zip
      FROM customer_address
      WHERE substr(ca_zip,1,5) IN ('89436', '30868', '65085', '22977', '83927', '77557', '58429', '40697', '80614', '10502', '32779',
      '91137', '61265', '98294', '17921', '18427', '21203', '59362', '87291', '84093', '21505', '17184', '10866', '67898', '25797',
      '28055', '18377', '80332', '74535', '21757', '29742', '90885', '29898', '17819', '40811', '25990', '47513', '89531', '91068',
      '10391', '18846', '99223', '82637', '41368', '83658', '86199', '81625', '26696', '89338', '88425', '32200', '81427', '19053',
      '77471', '36610', '99823', '43276', '41249', '48584', '83550', '82276', '18842', '78890', '14090', '38123', '40936', '34425',
      '19850', '43286', '80072', '79188', '54191', '11395', '50497', '84861', '90733', '21068', '57666', '37119', '25004', '57835',
      '70067', '62878', '95806', '19303', '18840', '19124', '29785', '16737', '16022', '49613', '89977', '68310', '60069', '98360',
      '48649', '39050', '41793', '25002', '27413', '39736', '47208', '16515', '94808', '57648', '15009', '80015', '42961', '63982',
      '21744', '71853', '81087', '67468', '34175', '64008', '20261', '11201', '51799', '48043', '45645', '61163', '48375', '36447',
      '57042', '21218', '41100', '89951', '22745', '35851', '83326', '61125', '78298', '80752', '49858', '52940', '96976', '63792',
      '11376', '53582', '18717', '90226', '50530', '94203', '99447', '27670', '96577', '57856', '56372', '16165', '23427', '54561',
      '28806', '44439', '22926', '30123', '61451', '92397', '56979', '92309', '70873', '13355', '21801', '46346', '37562', '56458',
      '28286', '47306', '99555', '69399', '26234', '47546', '49661', '88601', '35943', '39936', '25632', '24611', '44166', '56648',
      '30379', '59785', '11110', '14329', '93815', '52226', '71381', '13842', '25612', '63294', '14664', '21077', '82626', '18799',
      '60915', '81020', '56447', '76619', '11433', '13414', '42548', '92713', '70467', '30884', '47484', '16072', '38936', '13036',
      '88376', '45539', '35901', '19506', '65690', '73957', '71850', '49231', '14276', '20005', '18384', '76615', '11635', '38177',
      '55607', '41369', '95447', '58581', '58149', '91946', '33790', '76232', '75692', '95464', '22246', '51061', '56692', '53121',
      '77209', '15482', '10688', '14868', '45907', '73520', '72666', '25734', '17959', '24677', '66446', '94627', '53535', '15560',
      '41967', '69297', '11929', '59403', '33283', '52232', '57350', '43933', '40921', '36635', '10827', '71286', '19736', '80619',
      '25251', '95042', '15526', '36496', '55854', '49124', '81980', '35375', '49157', '63512', '28944', '14946', '36503', '54010',
      '18767', '23969', '43905', '66979', '33113', '21286', '58471', '59080', '13395', '79144', '70373', '67031', '38360', '26705',
      '50906', '52406', '26066', '73146', '15884', '31897', '30045', '61068', '45550', '92454', '13376', '14354', '19770', '22928',
      '97790', '50723', '46081', '30202', '14410', '20223', '88500', '67298', '13261', '14172', '81410', '93578', '83583', '46047',
      '94167', '82564', '21156', '15799', '86709', '37931', '74703', '83103', '23054', '70470', '72008', '35709', '91911', '69998',
      '20961', '70070', '63197', '54853', '88191', '91830', '49521', '19454', '81450', '89091', '62378', '31904', '61869', '51744',
      '36580', '85778', '36871', '48121', '28810', '83712', '45486', '67393', '26935', '42393', '20132', '55349', '86057', '21309',
      '80218', '10094', '11357', '48819', '39734', '40758', '30432', '21204', '29467', '30214', '61024', '55307', '74621', '11622',
      '68908', '33032', '52868', '99194', '99900', '84936', '69036', '99149', '45013', '32895', '59004', '32322', '14933', '32936',
      '33562', '72550', '27385', '58049', '58200', '16808', '21360', '32961', '18586', '79307', '15492'
                          )) a01
     inner join
     (select ca_zip
      from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
            FROM customer_address, customer
            WHERE ca_address_sk = c_current_addr_sk and
                  c_preferred_cust_flag='Y'
            group by ca_zip
            having count(*) > 10)A1
      ) b11
      on (a01.ca_zip = b11.ca_zip )) A2
 where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and ss_sold_date_sk between 2451271 and 2451361 
  and d_qoy = 2 and d_year = 1999
  and (substr(s_zip,1,2) = substr(a2.ca_zip,1,2))
 group by s_store_name
 order by s_store_name
limit 100"""
    
    
    
    // Q53
    queries(2) = """
      select
  *
from
  (select
    i_manufact_id,
    sum(ss_sales_price) sum_sales,
    avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
  from
    item,
    store_sales,
    date_dim,
    store
  where
    ss_item_sk = i_item_sk
    and ss_sold_date_sk = d_date_sk
    and ss_store_sk = s_store_sk
    and d_month_seq in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)
    and ((i_category in ('Books', 'Children', 'Electronics')
      and i_class in ('personal', 'portable', 'reference', 'self-help')
      and i_brand in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
    or (i_category in ('Women', 'Music', 'Men')
      and i_class in ('accessories', 'classical', 'fragrances', 'pants')
      and i_brand in ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))
    and ss_sold_date_sk between 2451911 and 2452275 -- partition key filter
  group by
    i_manufact_id,
    d_qoy
  ) tmp1
where
  case when avg_quarterly_sales > 0 then abs (sum_sales - avg_quarterly_sales) / avg_quarterly_sales else null end > 0.1
order by
  avg_quarterly_sales,
  sum_sales,
  i_manufact_id
limit 100"""
    
    // Q89
    queries(3) = """
      select
  *
from
  (select
    i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy,
    sum(ss_sales_price) sum_sales,
    avg(sum(ss_sales_price)) over (partition by i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
  from
    item,
    store_sales,
    date_dim,
    store
  where
    ss_item_sk = i_item_sk
    and ss_sold_date_sk = d_date_sk
    and ss_store_sk = s_store_sk
    and d_year in (2000)
    and ((i_category in ('Home', 'Books', 'Electronics')
        and i_class in ('wallpaper', 'parenting', 'musical'))
      or (i_category in ('Shoes', 'Jewelry', 'Men')
        and i_class in ('womens', 'birdal', 'pants')))
    and ss_sold_date_sk between 2451545 and 2451910  -- partition key filter
  group by
    i_category,
    i_class,
    i_brand,
    s_store_name,
    s_company_name,
    d_moy
  ) tmp1
where
  case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by
  sum_sales - avg_monthly_sales,
  s_store_name
limit 100"""
    
    // Used for debugging - create a local spark instance
    // val conf = new SparkConf().setAppName("UserQueryStream").setMaster("local").set("spark.sql.crossJoin.enabled", "true")
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
    
    // Retreive query statement from array
    // Create the timing file
    val pw = new PrintWriter(new File(timingFilePath)) 

    var query:String = null
    
    var queryNum: Int = 10
    
    if (queryID.trim.equals("Q3")){
      query = queries(0)
      queryNum = 8
    }
    else if (queryID.trim.equals("Q8")) {
      query = queries(1)
      queryNum = 9
    }
    else if (queryID.trim.equals("Q53")) {
      query = queries(2)
      queryNum = 10
    }
    else if (queryID.trim.equals("Q89")) {
      query = queries(3)
      queryNum = 11
    }
    
    // Execute query here
    println("Executing query: " + queryID)
    
    val startTime = System.currentTimeMillis()
    
    var firstColValue : Any = "NONE"
    var rowCount: Long = 0
    
    if (query != null) {
      
      val queryDF = sqlContext.sql(query)
      rowCount = queryDF.count  // touch the data to make sure the query actually executes
        
      // if rows returned retreive the value of first column of first row
      if (rowCount > 0) {
      val rows =  queryDF.collect()
         firstColValue = rows(0).get(0).toString()
         if (firstColValue == "") firstColValue = "EMPTY"
          
       }
      println("Completed query: " + queryID)
    
    } else { // We are executing KMeans
      
      queryNum = 12
      
      // Load and parse the data
      val data = sc.textFile(tpcdsDbPath+"/kmeans_data.txt")
      val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

      // Cluster the data into two classes using KMeans
      val numClusters = 4
      val numIterations = 40
      val clusters = KMeans.train(parsedData, numClusters, numIterations)

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = clusters.computeCost(parsedData)
      
      println("Finished executing KMeans - Within Set Sum of Squared Errors = " + WSSSE)
      firstColValue = WSSSE.toString()

      // We do not save Save and load model to avoid filling up the disks
      // clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
      // val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
      // $example off$
      
    }
       
    val finishTime = System.currentTimeMillis()
    val queryDuration = ((((finishTime - startTime)/1000)*10).round)/10  // Query duration is recorded in seconds
        
    // We don't need the full date, just the hours, minutes and seconds
    val date = new Date(startTime) 
    val sb = new StringBuffer(date.toString())
    val startDate = sb.substring(0, 10)
    val timeStamp = sb.substring(11, 19)
        
    // Write query time-stamp and duration into the timing file
    println(queryNum + "," + queryID + "," + startDate + "," + timeStamp + "," + queryDuration + "," + rowCount + "," + firstColValue)
    pw.println(queryNum + "," + queryID + "," + startDate + "," + timeStamp + "," + queryDuration + "," + rowCount + "," + firstColValue)
       
    pw.close()
   
   }
  
}