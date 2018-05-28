package com.spark.sql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
 
public class SparkSqlTest {

	public void creatRDDFromJDBC() {

		// SparkConf conf = new SparkConf().setAppName("JDBCDataSource");//
		// .setMaster("local");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		// SparkSession.builder().sparkContext(sc);
		// SQLContext sqlContext = new SQLContext(sc);

		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.sql.crossJoin.enabled", "true").getOrCreate();
		// Dataset<Row> jdbcDF = spark.read()
		// .format("jdbc")
		// .option("url", "jdbc:postgresql:dbserver")
		// .option("dbtable", "schema.tablename")
		// .option("user", "username")
		// .option("password", "password")
		// .load();

		// Properties connectionProperties = new Properties();
		// connectionProperties.put("user", "username");
		// connectionProperties.put("password", "password");
		// Dataset<Row> jdbcDF = spark.read()
		// .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
		// connectionProperties);

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "username");
		connectionProperties.put("password", "password");
		Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:mysql://10.45.136.102:3306/wms_plat", "", connectionProperties);
		Dataset<Row> accs = jdbcDF.sqlContext().sql("select count(*) from wms_log");
		List<Row> list = accs.collectAsList();
		for (Row r : list) {
			System.out.println("********************" + r.getInt(0));
		}

	}

	public void sparkSQLTest() {
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "rkylin");
		connectionProperties.put("password", "RkyLin0^2&");
		System.out.println((new Date()).toLocaleString());
		Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:mysql://10.25.115.27:3306/wms_plat", "wms_log",
				connectionProperties);
		jdbcDF = jdbcDF.selectExpr("sum(type)");
		System.out.println((new Date()).toLocaleString());
		// Dataset<Row> accs = jdbcDF.sqlContext().sql("select max(type) from
		// wms_log");
		List<Row> list = jdbcDF.takeAsList(1);
		System.out.println((new Date()).toLocaleString());
		for (Row r : list) {
			System.out.println("********************" + r.getLong(0));
		}
		System.out.println((new Date()).toLocaleString());
		jdbcDF = jdbcDF.selectExpr("sum(type)");
		System.out.println((new Date()).toLocaleString());
		// Dataset<Row> accs = jdbcDF.sqlContext().sql("select max(type) from
		// wms_log");
		list = jdbcDF.takeAsList(1);
		System.out.println((new Date()).toLocaleString());
		for (Row r : list) {
			System.out.println("********************" + r.getInt(0));
		}
	}

	/**
	 * 连表查询
	 */
	public void unionTable() {
		System.out.println("start running:" + (new Date()).toLocaleString());
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").getOrCreate();
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "rkylin");
		connectionProperties.put("password", "RkyLin0^2&");
		String url = "jdbc:mysql://10.45.136.102:3306/wms_storage";
		System.out.println("start maping table:" + (new Date()).toLocaleString());
		Dataset<Row> jdbcDF = spark.read().jdbc(url, "t_order_goods", connectionProperties);
		jdbcDF.createOrReplaceTempView("t_order_goods");

		Dataset<Row> jdbcDF2 = spark.read().jdbc(url, "t_order", connectionProperties);
		jdbcDF2.createOrReplaceTempView("t_order");

		Dataset<Row> jdbcDF3 = spark.read().jdbc(url, "t_ordre_sour", connectionProperties);
		jdbcDF3.createOrReplaceTempView("t_ordre_sour");

		Dataset<Row> jdbcDF4 = spark.read().jdbc(url, "t_online_shop", connectionProperties);
		jdbcDF4.createOrReplaceTempView("t_online_shop");

		Dataset<Row> jdbcDF5 = spark.read().jdbc(url, "t_shop", connectionProperties);
		jdbcDF5.createOrReplaceTempView("t_shop");

		Dataset<Row> jdbcDF6 = spark.read().jdbc(url, "t_stor", connectionProperties);
		jdbcDF6.createOrReplaceTempView("t_stor");

		Dataset<Row> jdbcDF7 = spark.read().jdbc(url, "t_goods", connectionProperties);
		jdbcDF7.createOrReplaceTempView("t_goods");

		Dataset<Row> jdbcDF8 = spark.read().jdbc(url, "t_goods_sku", connectionProperties);
		jdbcDF8.createOrReplaceTempView("t_goods_sku");

		Dataset<Row> jdbcDF9 = spark.read().jdbc(url, "t_stockout_dtls", connectionProperties);
		jdbcDF9.createOrReplaceTempView("t_stockout_dtls");

		Dataset<Row> jdbcDF10 = spark.read().jdbc(url, "t_stockout", connectionProperties);
		jdbcDF10.createOrReplaceTempView("t_stockout");

		Dataset<Row> jdbcDF11 = spark.read().jdbc(url, "t_rtrnexch_fact_rtrn", connectionProperties);
		jdbcDF11.createOrReplaceTempView("t_rtrnexch_fact_rtrn");

		System.out.println("end maping table:" + (new Date()).toLocaleString());
		System.out.println("start excute sql:" + (new Date()).toLocaleString());
		List<Row> list = spark.sqlContext()
				.sql("SELECT " + "max(tos.ORDER_SOUR_NAME)," + "max(tonlineshop.ONLINE_SHOP_NAME),"
						+ "max(ts.SHOP_NAME)," + "max(tstor.STOR_NAME)," + "max(tgs.GOODS_SKU_CODE),"
						+ "max(tg.GOODS_NAME)," + "SUM(tsd.QTY)," + "SUM(tsd.QTY * tsd.GOODS_SKU_COST) / sum(tsd.QTY) ,"
						+ "SUM(tsd.QTY * tsd.GOODS_SKU_COST)," + "SUM(tsd.FACT_UNIT_PRIC * tsd.QTY),"
						+ "sum(trfr.QTY) ," + "sum(trfr.QTY*trfr.FACT_UNIT_PRIC)," + "tg.GOODS_GUID " +

						"from " + " t_order_goods tog" + " LEFT JOIN "
						+ "t_order torder on tog.ORDER_GUID=torder.ORDER_GUID" + " LEFT JOIN "
						+ "t_ordre_sour tos on torder.ORDER_SOUR_GUID=tos.ORDER_SOUR_GUID" + " LEFT JOIN "
						+ "t_online_shop tonlineshop on tog.ONLINE_SHOP_GUID=tonlineshop.ONLINE_SHOP_GUID"
						+ " LEFT JOIN " + "t_shop ts on tog.SHOP_GUID=ts.SHOP_GUID" + " LEFT JOIN "
						+ "t_stor tstor on torder.STOR_GUID = tstor.STOR_GUID" + " LEFT JOIN "
						+ "t_goods tg on tog.GOODS_GUID = tg.GOODS_GUID" + " LEFT JOIN "
						+ "t_goods_sku tgs on tgs.GOODS_GUID = tg.GOODS_GUID" + " LEFT JOIN "
						+ "	t_stockout_dtls tsd on tsd.GOODS_GUID = tg.GOODS_GUID" + " LEFT JOIN "
						+ "	t_stockout tout on tout.STOCKOUT_GUID = tsd.STOCKOUT_GUID" + " LEFT JOIN "
						+ "	t_rtrnexch_fact_rtrn trfr on tsd.GOODS_SKU_GUID = trfr.GOODS_SKU_GUID"
						+ " where tog.GOODS_GUID is not null" +

						" group by tg.GOODS_GUID")
				.collectAsList();
		System.out.println("end excute sql:" + (new Date()).toLocaleString());
		System.out.println("start system out:" + (new Date()).toLocaleString());
		for (Row r : list) {
			System.out
					.println("商品名称:" + r.getString(5) + "  本期销售数量:" + r.getDecimal(6) + "  本期退货数量:" + r.getDecimal(10));
		}
		System.out.println("end system out:" + (new Date()).toLocaleString());
		// System.out.println("************************"+list.size());
	}

	// /**
	// * 连表查询
	// */
	// public void unionTable() {
	// System.out.println("start running:"+(new Date()).toLocaleString());
	// SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic
	// example").getOrCreate();
	// Properties connectionProperties = new Properties();
	// connectionProperties.put("user", "rkylin");
	// connectionProperties.put("password", "RkyLin0^2&");
	// String url = "jdbc:mysql://10.45.136.102:3306/wms_storage";
	// System.out.println("start maping table:"+(new Date()).toLocaleString());
	// Dataset<Row> jdbcDF = spark.read().jdbc(url, "t_order_goods",
	// connectionProperties);
	// jdbcDF.createOrReplaceTempView("t_order_goods");
	//
	// System.out.println("start collect table1:"+(new
	// Date()).toLocaleString());
	// jdbcDF.collectAsList();
	// jdbcDF.cache();
	// System.out.println("end collect table1:"+(new Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF2 = spark.read().jdbc(url, "t_order",
	// connectionProperties);
	// jdbcDF2.createOrReplaceTempView("t_order");
	//
	// System.out.println("start collect t_order:"+(new
	// Date()).toLocaleString());
	// jdbcDF2.collectAsList();
	// jdbcDF2.cache();
	// System.out.println("end collect t_order:"+(new Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF3 = spark.read().jdbc(url, "t_ordre_sour",
	// connectionProperties);
	// jdbcDF3.createOrReplaceTempView("t_ordre_sour");
	//
	// System.out.println("start collect t_ordre_sour:"+(new
	// Date()).toLocaleString());
	// jdbcDF3.collectAsList();jdbcDF3.cache();
	// System.out.println("end collect t_ordre_sour:"+(new
	// Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF4 = spark.read().jdbc(url, "t_online_shop",
	// connectionProperties);
	// jdbcDF4.createOrReplaceTempView("t_online_shop");
	//
	// System.out.println("start collect t_online_shop:"+(new
	// Date()).toLocaleString());
	// jdbcDF4.collectAsList();jdbcDF4.cache();
	// System.out.println("end collect t_online_shop:"+(new
	// Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF5 = spark.read().jdbc(url, "t_shop",
	// connectionProperties);
	// jdbcDF5.createOrReplaceTempView("t_shop");
	//
	// System.out.println("start collect t_shop:"+(new
	// Date()).toLocaleString());
	// jdbcDF5.collectAsList();jdbcDF5.cache();
	// System.out.println("end collect t_shop:"+(new Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF6 = spark.read().jdbc(url, "t_stor",
	// connectionProperties);
	// jdbcDF6.createOrReplaceTempView("t_stor");
	//
	// System.out.println("start collect t_stor:"+(new
	// Date()).toLocaleString());
	// jdbcDF6.collectAsList();jdbcDF6.cache();
	// System.out.println("end collect t_stor:"+(new Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF7 = spark.read().jdbc(url, "t_goods",
	// connectionProperties);
	// jdbcDF7.createOrReplaceTempView("t_goods");
	//
	// System.out.println("start collect t_goods:"+(new
	// Date()).toLocaleString());
	// jdbcDF7.collectAsList();jdbcDF7.cache();
	// System.out.println("end collect t_goods:"+(new Date()).toLocaleString());
	//
	//
	// Dataset<Row> jdbcDF8 = spark.read().jdbc(url, "t_goods_sku",
	// connectionProperties);
	// jdbcDF8.createOrReplaceTempView("t_goods_sku");
	//
	// System.out.println("start collect t_goods_sku:"+(new
	// Date()).toLocaleString());
	// jdbcDF8.collectAsList();jdbcDF8.cache();
	// System.out.println("end collect t_goods_sku:"+(new
	// Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF9 = spark.read().jdbc(url, "t_stockout_dtls",
	// connectionProperties);
	// jdbcDF9.createOrReplaceTempView("t_stockout_dtls");
	//
	// System.out.println("start collect t_stockout_dtls:"+(new
	// Date()).toLocaleString());
	// jdbcDF9.collectAsList();jdbcDF9.cache();
	// System.out.println("end collect t_stockout_dtls:"+(new
	// Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF10 = spark.read().jdbc(url, "t_stockout",
	// connectionProperties);
	// jdbcDF10.createOrReplaceTempView("t_stockout");
	//
	// System.out.println("start collect t_stockout:"+(new
	// Date()).toLocaleString());
	// jdbcDF10.collectAsList();jdbcDF10.cache();
	// System.out.println("end collect t_stockout:"+(new
	// Date()).toLocaleString());
	//
	// Dataset<Row> jdbcDF11 = spark.read().jdbc(url, "t_rtrnexch_fact_rtrn",
	// connectionProperties);
	// jdbcDF11.createOrReplaceTempView("t_rtrnexch_fact_rtrn");
	//
	// System.out.println("start collect t_rtrnexch_fact_rtrn:"+(new
	// Date()).toLocaleString());
	// jdbcDF11.collectAsList();jdbcDF11.cache();
	// System.out.println("end collect t_rtrnexch_fact_rtrn:"+(new
	// Date()).toLocaleString());
	//
	// System.out.println("end maping table:"+(new Date()).toLocaleString());
	// System.out.println("start excute sql:"+(new Date()).toLocaleString());
	// List<Row> list = spark.sqlContext()
	// .sql("SELECT " + "max(tos.ORDER_SOUR_NAME)," +
	// "max(tonlineshop.ONLINE_SHOP_NAME),"
	// + "max(ts.SHOP_NAME)," + "max(tstor.STOR_NAME)," +
	// "max(tgs.GOODS_SKU_CODE),"
	// + "max(tg.GOODS_NAME)," + "SUM(tsd.QTY),"
	// + "SUM(tsd.QTY * tsd.GOODS_SKU_COST) / sum(tsd.QTY) ,"
	// + "SUM(tsd.QTY * tsd.GOODS_SKU_COST)," + "SUM(tsd.FACT_UNIT_PRIC *
	// tsd.QTY),"
	// + "sum(trfr.QTY) ," + "sum(trfr.QTY*trfr.FACT_UNIT_PRIC)," +
	// "tg.GOODS_GUID " +
	//
	// "from " + " t_order_goods tog" + " LEFT JOIN "
	// + "t_order torder on tog.ORDER_GUID=torder.ORDER_GUID" + " LEFT JOIN "
	// + "t_ordre_sour tos on torder.ORDER_SOUR_GUID=tos.ORDER_SOUR_GUID" + "
	// LEFT JOIN "
	// + "t_online_shop tonlineshop on
	// tog.ONLINE_SHOP_GUID=tonlineshop.ONLINE_SHOP_GUID" + " LEFT JOIN "
	// + "t_shop ts on tog.SHOP_GUID=ts.SHOP_GUID" + " LEFT JOIN "
	// + "t_stor tstor on torder.STOR_GUID = tstor.STOR_GUID" + " LEFT JOIN "
	// + "t_goods tg on tog.GOODS_GUID = tg.GOODS_GUID" + " LEFT JOIN "
	// + "t_goods_sku tgs on tgs.GOODS_GUID = tg.GOODS_GUID" + " LEFT JOIN "
	// + " t_stockout_dtls tsd on tsd.GOODS_GUID = tg.GOODS_GUID" + " LEFT JOIN
	// "
	// + " t_stockout tout on tout.STOCKOUT_GUID = tsd.STOCKOUT_GUID" + " LEFT
	// JOIN "
	// + " t_rtrnexch_fact_rtrn trfr on tog.GOODS_GUID = trfr.GOODS_SKU_GUID"
	// + " where tog.GOODS_GUID is not null" +
	//
	// " group by tg.GOODS_GUID")
	// .collectAsList();
	// System.out.println("end excute sql:"+(new Date()).toLocaleString());
	// System.out.println("start system out:"+(new Date()).toLocaleString());
	// for (Row r : list) {
	// System.out.println("********************交易平台:" + r.getString(0)+"
	// 店铺:"+r.getString(1));
	// }
	// System.out.println("end system out:"+(new Date()).toLocaleString());
	//// System.out.println("************************"+list.size());
	// }

	/**
	 * 连表查询
	 */
	public void insertTest() {
		System.out.println("start running:" + (new Date()).toLocaleString());
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").getOrCreate();
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "rkylin");
		connectionProperties.put("password", "RkyLin0^2&");
		String url = "jdbc:mysql://10.45.136.102:3306/wms_storage";
		System.out.println("start maping table:" + (new Date()).toLocaleString());
		Dataset<Row> jdbcDF = spark.read().jdbc(url, "wms_category", connectionProperties);
		jdbcDF.createOrReplaceTempView("wms_category");
		Dataset<Row> jdbcDF2 = spark.read().jdbc(url, "wms_category_copy", connectionProperties);
		jdbcDF2.createOrReplaceTempView("wms_category_copy");

		System.out.println("end maping table:" + (new Date()).toLocaleString());
		System.out.println("start excute sql:" + (new Date()).toLocaleString());
		List<Row> list = spark.sqlContext().sql(readSql("/home/spark/test/sql/inserTest2.sql"))
				.collectAsList();
		System.out.println("end excute sql:" + (new Date()).toLocaleString());
		System.out.println("start system out:" + (new Date()).toLocaleString());
		for (Row r : list) {
			System.out.println("店铺名称:" + r.getString(3) + "  店铺账户:" + r.getDecimal(6));
		}
		System.out.println("end system out:" + (new Date()).toLocaleString());
		
		// System.out.println("************************"+list.size());
	}
	
	
	/**
	 * 连表查询
	 */
	public void insertPurchaseSaleStockTotal() {
		System.out.println("start running:" + (new Date()).toLocaleString());
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").getOrCreate();
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "rkylin");
		connectionProperties.put("password", "RkyLin0^2&");
		String url = "jdbc:mysql://10.45.136.102:3306/wms_storage";
		System.out.println("start maping table:" + (new Date()).toLocaleString());
		Dataset<Row> jdbcDF = spark.read().jdbc(url, "t_sku_balance_day", connectionProperties);
		jdbcDF.createOrReplaceTempView("t_sku_balance_day");
		Dataset<Row> jdbcDF2 = spark.read().jdbc(url, "t_goods_sku_brcd", connectionProperties);
		jdbcDF2.createOrReplaceTempView("t_goods_sku_brcd");
		Dataset<Row> jdbcDF3 = spark.read().jdbc(url, "t_goods_sku", connectionProperties);
		jdbcDF3.createOrReplaceTempView("t_goods_sku");
		Dataset<Row> jdbcDF4 = spark.read().jdbc(url, "t_goods", connectionProperties);
		jdbcDF4.createOrReplaceTempView("t_goods");
		Dataset<Row> jdbcDF5 = spark.read().jdbc(url, "t_stor", connectionProperties);
		jdbcDF5.createOrReplaceTempView("t_stor");
		Dataset<Row> jdbcDF6 = spark.read().jdbc(url, "t_shop", connectionProperties);
		jdbcDF6.createOrReplaceTempView("t_shop");
		Dataset<Row> jdbcDF7 = spark.read().jdbc(url, "t_stockin_dtls", connectionProperties);
		jdbcDF7.createOrReplaceTempView("t_stockin_dtls");
		Dataset<Row> jdbcDF8 = spark.read().jdbc(url, "t_stockin", connectionProperties);
		jdbcDF8.createOrReplaceTempView("t_stockin");
		Dataset<Row> jdbcDF9 = spark.read().jdbc(url, "t_external_stockin_dtls", connectionProperties);
		jdbcDF9.createOrReplaceTempView("t_external_stockin_dtls");
		Dataset<Row> jdbcDF10 = spark.read().jdbc(url, "t_external_stockin", connectionProperties);
		jdbcDF10.createOrReplaceTempView("t_external_stockin");
		Dataset<Row> jdbcDF11 = spark.read().jdbc(url, "t_stockout", connectionProperties);
		jdbcDF11.createOrReplaceTempView("t_stockout");
		Dataset<Row> jdbcDF12 = spark.read().jdbc(url, "t_stockout_dtls", connectionProperties);
		jdbcDF12.createOrReplaceTempView("t_stockout_dtls");
		Dataset<Row> jdbcDF13 = spark.read().jdbc(url, "t_external_otherstockout", connectionProperties);
		jdbcDF13.createOrReplaceTempView("t_external_otherstockout");
		Dataset<Row> jdbcDF14 = spark.read().jdbc(url, "t_external_otherstockout_dtls", connectionProperties);
		jdbcDF14.createOrReplaceTempView("t_external_otherstockout_dtls");
		Dataset<Row> jdbcDF15 = spark.read().jdbc(url, "wms_purchase_sale_stock_total_show_copy", connectionProperties);
		jdbcDF15.createOrReplaceTempView("wms_purchase_sale_stock_total_show_copy");

		System.out.println("end maping table:" + (new Date()).toLocaleString());
		System.out.println("start excute sql:" + (new Date()).toLocaleString());
		List<Row> list = spark.sqlContext().sql(readSql("/home/spark/test/sql/inserTest.sql"))
				.collectAsList();
		System.out.println("end excute sql:" + (new Date()).toLocaleString());
		System.out.println("start system out:" + (new Date()).toLocaleString());
		for (Row r : list) {
			System.out.println("店铺名称:" + r.getString(3) + "  店铺账户:" + r.getDecimal(4));
		}
		System.out.println("end system out:" + (new Date()).toLocaleString());
		
		// System.out.println("************************"+list.size());
	}
	
	
	/**
	 * 连表查询
	 */
	public void insertSalesReport() {
		System.out.println("start running:" + (new Date()).toLocaleString());
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").getOrCreate();
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "rkylin");
		connectionProperties.put("password", "RkyLin0^2&");
		String url = "jdbc:mysql://10.45.136.102:3306/wms_storage";
		System.out.println("start maping table:" + (new Date()).toLocaleString());
		Dataset<Row> jdbcDF = spark.read().jdbc(url, "t_order_goods", connectionProperties);
		jdbcDF.createOrReplaceTempView("t_order_goods");
		Dataset<Row> jdbcDF2 = spark.read().jdbc(url, "t_order", connectionProperties);
		jdbcDF2.createOrReplaceTempView("t_order");
		Dataset<Row> jdbcDF3 = spark.read().jdbc(url, "t_ordre_sour", connectionProperties);
		jdbcDF3.createOrReplaceTempView("t_ordre_sour");
		Dataset<Row> jdbcDF4 = spark.read().jdbc(url, "t_online_shop", connectionProperties);
		jdbcDF4.createOrReplaceTempView("t_online_shop");
		Dataset<Row> jdbcDF5 = spark.read().jdbc(url, "t_stor", connectionProperties);
		jdbcDF5.createOrReplaceTempView("t_stor");
		Dataset<Row> jdbcDF6 = spark.read().jdbc(url, "t_shop", connectionProperties);
		jdbcDF6.createOrReplaceTempView("t_shop");
		Dataset<Row> jdbcDF7 = spark.read().jdbc(url, "wms_warehouse_datasource_relation", connectionProperties);
		jdbcDF7.createOrReplaceTempView("wms_warehouse_datasource_relation");
		Dataset<Row> jdbcDF8 = spark.read().jdbc(url, "wms_warehouse", connectionProperties);
		jdbcDF8.createOrReplaceTempView("wms_warehouse");
		Dataset<Row> jdbcDF9 = spark.read().jdbc(url, "t_goods", connectionProperties);
		jdbcDF9.createOrReplaceTempView("t_goods");
		Dataset<Row> jdbcDF10 = spark.read().jdbc(url, "t_goods_sku", connectionProperties);
		jdbcDF10.createOrReplaceTempView("t_goods_sku");
		Dataset<Row> jdbcDF11 = spark.read().jdbc(url, "t_goods_sku_brcd", connectionProperties);
		jdbcDF11.createOrReplaceTempView("t_goods_sku_brcd");
		Dataset<Row> jdbcDF12 = spark.read().jdbc(url, "t_stockout_dtls", connectionProperties);
		jdbcDF12.createOrReplaceTempView("t_stockout_dtls");
		Dataset<Row> jdbcDF13 = spark.read().jdbc(url, "t_stockout", connectionProperties);
		jdbcDF13.createOrReplaceTempView("t_stockout");
		Dataset<Row> jdbcDF14 = spark.read().jdbc(url, "wms_product_sale_report_show", connectionProperties);
		jdbcDF14.createOrReplaceTempView("wms_product_sale_report_show");
		System.out.println("end maping table:" + (new Date()).toLocaleString());
		System.out.println("start excute sql:" + (new Date()).toLocaleString());
		List<Row> list = spark.sqlContext().sql(readSql("/data/server/lxy_spark/sql/inserTest.sql"))
				.collectAsList();
		System.out.println("end excute sql:" + (new Date()).toLocaleString());
		System.out.println("start system out:" + (new Date()).toLocaleString());
		for (Row r : list) {
			System.out.println("店铺名称:" + r.getString(3) + "  店铺账户:" + r.getString(4));
		}
		System.out.println("end system out:" + (new Date()).toLocaleString());
		
		// System.out.println("************************"+list.size());
	}

	public String readSql(String path) {
		StringBuffer sb = new StringBuffer("");
		try {
			FileReader reader = new FileReader(path);
			BufferedReader br = new BufferedReader(reader);
			String str = null;
			while ((str = br.readLine()) != null) {
				sb.append(str+" ");
				System.out.println(str);
			}
			br.close();
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkSqlTest sst = new SparkSqlTest();
		sst.insertSalesReport();
	}
}
