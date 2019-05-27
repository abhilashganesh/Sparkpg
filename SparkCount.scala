import org.apache.spark.sql.functions.lit
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
//Command to run scala file: 
//:load C:/Users/abhilashg/Desktop/SparkCount.scala
//SparkCount.main(null)
object SparkCount {
   def main(args: Array[String]) {

		//val conf = new SparkConf().setAppName("SparkJoins").setMaster("local");
		//val sc = new SparkContext(conf);

		val sqlContext = new org.apache.spark.sql.SQLContext(sc);

		val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("C://Users//abhilashg//Desktop//data.csv");

		df.write.partitionBy("Country").parquet("C://Users//abhilashg//Desktop//xyz");

		val candf=sqlContext.read.parquet("C://Users//abhilashg//Desktop//xyz//Country=Canada");
		val chidf=sqlContext.read.parquet("C://Users//abhilashg//Desktop//xyz//Country=China");
		val engdf=sqlContext.read.parquet("C://Users//abhilashg//Desktop//xyz//Country=England");
		val gerdf=sqlContext.read.parquet("C://Users//abhilashg//Desktop//xyz//Country=Germany");
		val inddf=sqlContext.read.parquet("C://Users//abhilashg//Desktop//xyz//Country=India");

		val splitcandf=candf.withColumn("c1",split(col("Values"),";").getItem(0)).withColumn("c2",split(col("Values"),";").getItem(1)).withColumn("c3",split(col("Values"),";").getItem(2)).withColumn("c4",split(col("Values"),";").getItem(3)).withColumn("c5",split(col("Values"),";").getItem(4));

		val cn = splitcandf.drop(col("Values")).toDF();

		val CN= cn.selectExpr("cast(c1 as int) c1","cast(c2 as int) c2","cast(c3 as int) c3","cast(c4 as int) c4","cast(c5 as int) c5");

		val sum_cols = CN.columns.map(x=>sum(col(x)));

		val fcan=CN.agg(sum_cols.head,sum_cols.tail:_*);

		val FCAN=fcan.withColumn("Values",concat_ws(";",col("sum(c1)"),col("sum(c2)"),col("sum(c3)"),col("sum(c4)"),col("sum(c5)"))).drop(col("sum(c1)")).drop(col("sum(c2)")).drop(col("sum(c3)")).drop(col("sum(c4)")).drop(col("sum(c5)"));

		val FCANADA=FCAN.withColumn("Country",lit("Canada"));  
		
		
		val splitchidf=chidf.withColumn("c1",split(col("Values"),";").getItem(0)).withColumn("c2",split(col("Values"),";").getItem(1)).withColumn("c3",split(col("Values"),";").getItem(2)).withColumn("c4",split(col("Values"),";").getItem(3)).withColumn("c5",split(col("Values"),";").getItem(4));

		val ch = splitchidf.drop(col("Values")).toDF();

		val CH= ch.selectExpr("cast(c1 as int) c1","cast(c2 as int) c2","cast(c3 as int) c3","cast(c4 as int) c4","cast(c5 as int) c5");

		val sum_cols1 = CH.columns.map(x=>sum(col(x)));

		val fchi=CH.agg(sum_cols1.head,sum_cols1.tail:_*);

		val FCHI=fchi.withColumn("Values",concat_ws(";",col("sum(c1)"),col("sum(c2)"),col("sum(c3)"),col("sum(c4)"),col("sum(c5)"))).drop(col("sum(c1)")).drop(col("sum(c2)")).drop(col("sum(c3)")).drop(col("sum(c4)")).drop(col("sum(c5)"));

		val FCHINA=FCHI.withColumn("Country",lit("China"));  
		
		
		val splitengdf=engdf.withColumn("c1",split(col("Values"),";").getItem(0)).withColumn("c2",split(col("Values"),";").getItem(1)).withColumn("c3",split(col("Values"),";").getItem(2)).withColumn("c4",split(col("Values"),";").getItem(3)).withColumn("c5",split(col("Values"),";").getItem(4));

		val en = splitengdf.drop(col("Values")).toDF();

		val EN= en.selectExpr("cast(c1 as int) c1","cast(c2 as int) c2","cast(c3 as int) c3","cast(c4 as int) c4","cast(c5 as int) c5");

		val sum_cols2 = EN.columns.map(x=>sum(col(x)));

		val feng=EN.agg(sum_cols2.head,sum_cols2.tail:_*);

		val FENG=feng.withColumn("Values",concat_ws(";",col("sum(c1)"),col("sum(c2)"),col("sum(c3)"),col("sum(c4)"),col("sum(c5)"))).drop(col("sum(c1)")).drop(col("sum(c2)")).drop(col("sum(c3)")).drop(col("sum(c4)")).drop(col("sum(c5)"));

		val FENGLAND=FENG.withColumn("Country",lit("England"));  
		
		
		
		val splitgerdf=gerdf.withColumn("c1",split(col("Values"),";").getItem(0)).withColumn("c2",split(col("Values"),";").getItem(1)).withColumn("c3",split(col("Values"),";").getItem(2)).withColumn("c4",split(col("Values"),";").getItem(3)).withColumn("c5",split(col("Values"),";").getItem(4));

		val ge = splitgerdf.drop(col("Values")).toDF();

		val GE= ge.selectExpr("cast(c1 as int) c1","cast(c2 as int) c2","cast(c3 as int) c3","cast(c4 as int) c4","cast(c5 as int) c5");

		val sum_cols3 = GE.columns.map(x=>sum(col(x)));

		val fger=GE.agg(sum_cols3.head,sum_cols3.tail:_*);

		val FGER=fger.withColumn("Values",concat_ws(";",col("sum(c1)"),col("sum(c2)"),col("sum(c3)"),col("sum(c4)"),col("sum(c5)"))).drop(col("sum(c1)")).drop(col("sum(c2)")).drop(col("sum(c3)")).drop(col("sum(c4)")).drop(col("sum(c5)"));

		val FGERMANY=FGER.withColumn("Country",lit("Germany"));  
		
		
		
		val splitinddf=inddf.withColumn("c1",split(col("Values"),";").getItem(0)).withColumn("c2",split(col("Values"),";").getItem(1)).withColumn("c3",split(col("Values"),";").getItem(2)).withColumn("c4",split(col("Values"),";").getItem(3)).withColumn("c5",split(col("Values"),";").getItem(4));

		val in = splitinddf.drop(col("Values")).toDF();

		val IN= in.selectExpr("cast(c1 as int) c1","cast(c2 as int) c2","cast(c3 as int) c3","cast(c4 as int) c4","cast(c5 as int) c5");

		val sum_cols4 = IN.columns.map(x=>sum(col(x)));

		val find=IN.agg(sum_cols4.head,sum_cols4.tail:_*);

		val FIND=find.withColumn("Values",concat_ws(";",col("sum(c1)"),col("sum(c2)"),col("sum(c3)"),col("sum(c4)"),col("sum(c5)"))).drop(col("sum(c1)")).drop(col("sum(c2)")).drop(col("sum(c3)")).drop(col("sum(c4)")).drop(col("sum(c5)"));

		val FINDIA=FIND.withColumn("Country",lit("India")); 



		val k= FCANADA.unionAll(FCHINA).unionAll(FENGLAND).unionAll(FGERMANY).unionAll(FINDIA);
		
		val columns: Array[String] = k.columns;
		
		val reorderedColumnNames = Array(columns(1), columns(0));
		
		val result = k.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
		
		System.out.println(result.show);
		
		result.write.parquet("C://Users//abhilashg//Desktop//abc");

        System.out.println("Result stored in parquet file in C://Users//abhilashg//Desktop//abc " );
   }
}




