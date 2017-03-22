package dataframe;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 
 */

/**
 * @author nalin
 *
 */
public class DataFrameEG {

	public static void main(String [] args){
		
		SparkConf conf = new SparkConf().setAppName("Person").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		DataFrame df = sqlContext.read().json("src/main/resources/person.json");
		
		dataFrameOperation(df);
		dataFrameSqlOperation(df, sqlContext);
	}
	
	public static void dataFrameSqlOperation(DataFrame df, SQLContext sqlContext){
		
		df.registerTempTable("person");
		df = sqlContext.sql("SELECT * FROM person WHERE lastName = 'Adhikari'");
		df.show();
	}
	
	public static void dataFrameOperation(DataFrame df){

		// Show the content of the DataFrame
		df.show();
		
		// Print the schema in a tree format
		df.printSchema();
		
		//Remove duplicates
		df.dropDuplicates().show();
	}
}
