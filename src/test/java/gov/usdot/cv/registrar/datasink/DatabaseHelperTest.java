package gov.usdot.cv.registrar.datasink;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabaseHelperTest {

	public static DatabaseHelper dbHelper;
	
	@BeforeClass
	public static void setup() throws Exception {
		String mongoServerHost = "54.83.140.41";
		int mongoServerPort = 27017;
		String databaseName = "cvdb";
		boolean autoConnectRetry = true;
		int connectTimeoutMs = 0;
		String collectionName = "objectRegisterTest";
		String geospatialFieldName = "region";
		
		dbHelper = new DatabaseHelper(mongoServerHost, mongoServerPort, 
				databaseName, autoConnectRetry, connectTimeoutMs, collectionName, geospatialFieldName);
	}
	
	@Test
	public void testUpsert() throws IOException {
		String jsonFile = "src/test/resources/register_good.json";
		String json = FileUtils.readFileToString(new File(jsonFile));
		RegisterModel model = RegisterModel.fromJSON(json);
		model.validate();
		dbHelper.upsert(model);
	}
	
}
