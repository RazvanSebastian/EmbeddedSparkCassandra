package ro.fortech.sparkembedded;

public class SparkEmbedded {

	public static void main(String[] args) {
		ApplicationProperties applicationProperties = ApplicationProperties.getInstance();
		
		args = new String[1];
		args[0] = "partition2";
		
		if (args.length == 1) {
			System.out.println("WHERE argument = " + args[0]);
			final Service service = new Service(applicationProperties);
			service.joinOnParentAndChildTables(args[0]).show();
			System.out.println("End of Program!");
		} else {
			System.out.println("Only one argument is required for WHERE clause!");
			System.exit(0);
		}
	}
}
