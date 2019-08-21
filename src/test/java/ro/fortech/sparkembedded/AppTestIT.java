package ro.fortech.sparkembedded;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
		
	private Service service;
	
	@Before
	public void init() {
		ApplicationProperties applicationProperties = ApplicationProperties.getInstance().setProfile("local");;
		service = new Service(applicationProperties);
	}
	
    @Test
    public void shouldAnswerWithTrue()
    {
		Dataset<Row> joinResult = service.joinOnParentAndChildTables("partition2");
		assertEquals(joinResult.count(), 16);
    }
}
