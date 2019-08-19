package ro.fortech.sparkembedded;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit test for simple App.
 */
@RunWith(JUnit4.class)
public class AppTestIT 
{
		
	private SparkSession sparkSession;
	
	@Before
	public void init() {

		sparkSession = SparkSession.builder()
				.appName("App_name")
				.master("local")
				.config("spark.cassandra.connection.host", "127.0.0.1")
				.config("spark.cassandra.connection.port", "9042")
				.config("spark.cassandra.auth.username", "cassandra")
				.config("spark.cassandra.auth.password", "cassandra")
				.getOrCreate();
	}
	
    @Test
    public void shouldAnswerWithTrue()
    {
    	Dataset<Row> parentTable = sparkSession
			.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "demo_keyspace")
			.option("table" , "parent_entity")
			.load();
			
		Dataset<Row> childTable = sparkSession
			.read()
			.format("org.apache.spark.sql.cassandra")
			.option("keyspace", "demo_keyspace")
			.option("table" , "child_entity")
			.load();
			
		Dataset<Row> joinResult = parentTable
				.join(childTable, parentTable.col("partitionkey").equalTo(childTable.col("partitionkey")), "inner")
				.where(parentTable.col("partitionkey").equalTo("partition2"))
				.cache();
			
		assertEquals(joinResult.count(), 16);
    
    }
}
