package gov.usdot.cv.registrar.datasink;

import gov.usdot.cv.common.database.mongodb.MongoOptionsBuilder;
import gov.usdot.cv.common.database.mongodb.dao.InsertObjectRegistrationDataDao;
import gov.usdot.cv.common.util.PropertyLocator;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public class DatabaseHelper {

	private final Logger logger = Logger.getLogger(getClass());
	
	private String collectionName;
	private String geoSpatialFieldName;
	
	private InsertObjectRegistrationDataDao objectRegistrationDao;
	
	public DatabaseHelper(String mongoServerHost, int mongoServerPort, 
			String databaseName, boolean autoConnectRetry, int connectTimeoutMs, 
			String collectionName, String geospatialFieldName) throws UnknownHostException {
		
		logger.info("Constructing MongoDB data access object ...");
		MongoOptionsBuilder optionsBuilder = new MongoOptionsBuilder();
		optionsBuilder.setAutoConnectRetry(autoConnectRetry).setConnectTimeoutMs(connectTimeoutMs);
		
		if (mongoServerHost.endsWith("%s")) {
			String domain = PropertyLocator.getString("RTWS_DOMAIN", null);
			mongoServerHost = String.format(mongoServerHost, domain);
		}
		
		logger.info(String.format("Setting MongoDB host to '%s' and port to '%s' ...", mongoServerHost, mongoServerPort));
		int maxAttempts = 20;
		int attempt = 0;
		while (attempt < maxAttempts) {
			try {
				this.objectRegistrationDao = InsertObjectRegistrationDataDao.newInstance(
					mongoServerHost, 
					mongoServerPort, 
					optionsBuilder.build(),
					databaseName);
				break;
			} catch (Exception e) {
				attempt++;
				logger.warn("Failed to connect to Mongo, retrying in 30 seconds " + e.toString());
				try { Thread.sleep(1000 * 30); } catch (InterruptedException e1) {}
			}
		}
		
		this.collectionName = collectionName;
		this.geoSpatialFieldName = geospatialFieldName;
	}
	
	public WriteResult upsert(RegisterModel registerModel) {
		DBObject query = new BasicDBObject("serviceId", registerModel.serviceId);
		query.put("serviceProviderId", registerModel.serviceProviderId);
		return objectRegistrationDao.upsert(collectionName, query, registerModel.getDBObject());
	}
	
	public void createIndexes() {
		objectRegistrationDao.create2dSphereIndex(collectionName, geoSpatialFieldName);
		objectRegistrationDao.createExpirationIndex(collectionName, RegisterModel.EXPIRE_AT_KEY, 0);
	}
	
}
