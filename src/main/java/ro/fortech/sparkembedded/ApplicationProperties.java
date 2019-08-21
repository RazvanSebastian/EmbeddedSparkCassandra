package ro.fortech.sparkembedded;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.sql.SparkSession;

public class ApplicationProperties {
	
	private static volatile ApplicationProperties instance; 
	
	private ClassLoader classLoader;
	private String keyspace;
	private SparkSession sparkSession;
	private String profile;
	
	private ApplicationProperties() {
		this.classLoader = this.getClass().getClassLoader();
	}
	
	public static ApplicationProperties getInstance() {
		if(instance == null) {
			instance = new ApplicationProperties();
			instance.setProfile();
		}
		return instance;
	}
	
	public SparkSession getSparkSession() {
		if(sparkSession == null) {
			try(InputStream input = this.classLoader.getResourceAsStream(getProfile().concat("-config.properties"))) {
				Properties profileProperties = new Properties();
				profileProperties.load(input);
				
				this.setKeyspace(profileProperties.getProperty("cassandra.keyspace"));
			
				sparkSession = SparkSession.builder()
					.appName("App_name")
					.master("local")
					.config("spark.cassandra.connection.ssl.enabled", profileProperties.getProperty("cassandra.ssl"))
					.config("spark.cassandra.connection.host", profileProperties.getProperty("cassandra.host"))
					.config("spark.cassandra.connection.port", profileProperties.getProperty("cassandra.port"))
					.config("spark.cassandra.auth.username", profileProperties.getProperty("cassandra.username"))
					.config("spark.cassandra.auth.password", profileProperties.getProperty("cassandra.password"))
					.getOrCreate();
			
				return sparkSession;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sparkSession;
	}
	
	public String getKeyspace() {
		return keyspace;
	}
	
	private void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
	
	public String getProfile() {
		return this.profile;
	}
	
	
	private ApplicationProperties setProfile() {
		try (InputStream input = this.classLoader.getResourceAsStream("config.properties")) {
			Properties applicationPoperties= new Properties();
			applicationPoperties.load(input);
		
			this.profile = applicationPoperties.getProperty("application.profile");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return instance;
	}
	
	/**
	 * Override the profile set from config.properties with 
	 * 
	 * @return 
	 */
	public ApplicationProperties setProfile(String profile) {
		this.profile = profile;
		return instance;
	}

}
