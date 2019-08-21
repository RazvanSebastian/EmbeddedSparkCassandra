package ro.fortech.sparkembedded;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Service {
	
	private ApplicationProperties applicationProperties;
	
	public Service(ApplicationProperties applicationProperties) {
		super();
		this.applicationProperties = applicationProperties;
	}

	/**
	 * 
	 * @param partitionKey - value for parent partition key on where clause
	 * @return
	 */
	public Dataset<Row> joinOnParentAndChildTables(String partitionKey){
		Dataset<Row> parentTable = applicationProperties.getSparkSession()
				.read()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", applicationProperties.getKeyspace())
				.option("table" , "parent_entity")
				.load();
				
			Dataset<Row> childTable = applicationProperties.getSparkSession()
				.read()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", applicationProperties.getKeyspace())
				.option("table" , "child_entity")
				.load();
				
			Dataset<Row> joinResult = parentTable
					.join(childTable, parentTable.col("partitionkey").equalTo(childTable.col("partitionkey")), "inner")
					.where(parentTable.col("partitionkey").equalTo(partitionKey))
					.cache();
			
			return joinResult;
	}

}
