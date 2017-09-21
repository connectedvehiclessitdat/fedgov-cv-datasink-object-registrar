package gov.usdot.cv.registrar.datasink;

import gov.usdot.asn1.generated.j2735.dsrc.Position3D;
import gov.usdot.asn1.generated.j2735.semi.GeoRegion;
import gov.usdot.asn1.generated.j2735.semi.ObjectRegistrationData;
import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.j2735.CVSampleMessageBuilder;

import java.util.ArrayList;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import net.sf.json.JSONObject;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.deleidos.rtws.commons.exception.InitializationException;
import com.deleidos.rtws.core.framework.Description;
import com.deleidos.rtws.core.framework.SystemConfigured;
import com.deleidos.rtws.core.framework.UserConfigured;
import com.deleidos.rtws.core.framework.processor.AbstractDataSink;
import com.oss.asn1.EncodeFailedException;
import com.oss.asn1.EncodeNotSupportedException;

@Description("Processes object registration requests by updating records in the object registery database")
public class ObjectRegistrar extends AbstractDataSink {
	
	private final Logger logger = Logger.getLogger(getClass());
	
	// Mongo
	private String 			mongoServerHost;
	private int    			mongoServerPort;
	private String 			databaseName;
	private boolean 		autoConnectRetry = true;
	private int 			connectTimeoutMs = 0;
	private String			collectionName;
	private String			geospatialFieldName;
	// ResponseSender
	private boolean			forwardAll = false;
	private String 			bundleForwarderHost;
	private int    			bundleForwarderPort = -1;
	// ReceiptSender
	private String			receiptJmsHost;
	private int				receiptJmsPort = -1;
	private String 			topicName;
	// Helpers
	private DatabaseHelper 	dbHelper;
	private ResponseSender 	responseSender;
	private ReceiptSender  	receiptSender;
	private SystemRegistrar systemRegistrar;
	// Auto register information
	private String			whSdcHostIpv4;
	private String			whSdcHostIpv6;
	private int				whSdcPortSecure;
	private int				whSdcPortClear;

	private String			whSdwHostIpv4;
	private String			whSdwHostIpv6;
	private int				whSdwPortSecure;
	private int				whSdwPortClear;
	
	private double			nwLat;
	private double			nwLon;
	private double			seLat;
	private double			seLon;
	
	@Override
	@SystemConfigured(value = "Object Registrar DataSink")
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	@SystemConfigured(value = "objectregistrar")
	public void setShortname(String shortname) {
		super.setShortname(shortname);
	}
	
	@UserConfigured(value="cvdb", description="The database name to store Object Registration records.")
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public String getDatabaseName() {
		return this.databaseName;
	}
	
	@UserConfigured(
		value= "127.0.0.1", 
		description="The MongoDB server hostname.", 
		flexValidator={"StringValidator minLength=2 maxLength=1024"})
	public void setMongoServerHost(String mongoServerHost) {
		if (mongoServerHost != null) {
			this.mongoServerHost = mongoServerHost.trim();
		}
	}
		
	@NotNull
	public String getMongoServerHost() {
		return this.mongoServerHost;
	}
		
	@UserConfigured(
		value = "27017", 
		description = "The MongoDB server port number.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setMongoServerPort(int mongoServerPort) {
		this.mongoServerPort = mongoServerPort;
	}
		
	@Min(0)
	@Max(65535)
	public int getMongoServerPort() {
		return this.mongoServerPort;
	}
	
	@UserConfigured(
		value = "true",
		description = "MongoDB client auto connect retry flag.",
		flexValidator = {"RegExpValidator expression=true|false"})
	public void setAutoConnectRetry(boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
	}
	
	@NotNull
	public boolean getAutoConnectRetry() {
		return this.autoConnectRetry;
	}
		
	@UserConfigured(
		value = "3000",
		description = "Time (in milliseconds) to wait for a successful connection.",
		flexValidator = {"NumberValidator minValue=0 maxValue=" + Integer.MAX_VALUE})
	public void setConnectTimeoutMs(int connectTimeoutMs) {
		this.connectTimeoutMs = connectTimeoutMs;
	}
	
	@NotNull
	public int getConnectTimeoutMs() {
		return this.connectTimeoutMs;
	}
	
	@UserConfigured(
		value = "true",
		description = "Flag indicating if all responses will be forwarded or not.",
		flexValidator = {"RegExpValidator expression=true|false"})
	public void setForwardAll(boolean forwardAll) {
		this.forwardAll = forwardAll;
	}
	
	@NotNull
	public boolean getForwardAll() {
		return this.forwardAll;
	}
	
	@UserConfigured(
		value= "127.0.0.1", 
		description="The bundle forwarder host.", 
		flexValidator={"StringValidator minLength=2 maxLength=1024"})
	public void setBundleForwarderHost(String bundleForwarderHost) {
		this.bundleForwarderHost = bundleForwarderHost;
	}
			
	@NotNull
	public String getBundleForwarderHost() {
		return this.bundleForwarderHost;
	}
			
	@UserConfigured(
		value = "46761", 
		description = "The bundle forwarder port number.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setBundleForwarderPort(int bundleForwarderPort) {
		this.bundleForwarderPort = bundleForwarderPort;
	}
			
	@Min(0)
	@Max(65535)
	public int getBundleForwarderPort() {
		return this.bundleForwarderPort;
	}
	
	@UserConfigured(
		value= "", 
		description="The receipt jms server hostname.", 
		flexValidator={"StringValidator minLength=0 maxLength=1024"})
	public void setReceiptJmsHost(String receiptJmsHost) {
		this.receiptJmsHost = receiptJmsHost;
	}
	
	@NotNull
	public String getReceiptJmsHost() {
		return this.receiptJmsHost;
	}
	
	@UserConfigured(
		value = "61617", 
		description = "The receipt jms server port.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setReceiptJmsPort(int receiptJmsPort) {
		this.receiptJmsPort = receiptJmsPort;
	}
	
	@Min(0)
	@Max(65535)
	public int getReceiptJmsPort() {
		return this.receiptJmsPort;
	}
	
	@UserConfigured(
		value = "cv.receipts",
		description = "The jms topic to place receipts.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setReceiptTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	@UserConfigured(value = "objectRegister", description = "Name of the Object Register data collection.", 
			flexValidator = { "StringValidator minLength=2 maxLength=1024" })
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	
	@NotNull
	public String getWhSdcHostIpv4() {
		return whSdcHostIpv4;
	}

	@UserConfigured(
		value = "104.130.163.235",
		description = "The IPv4 address of the SDC",
		flexValidator = {"StringValidator minLength=3 maxLength=1024"})
	public void setWhSdcHostIpv4(String whSdcHostIpv4) {
		this.whSdcHostIpv4 = whSdcHostIpv4;
	}

	@NotNull
	public String getWhSdcHostIpv6() {
		return whSdcHostIpv6;
	}

	@UserConfigured(
		value = "2001:4802:7801:104:be76:4eff:fe20:ff",
		description = "The IPv6 address of the SDC",
		flexValidator = {"StringValidator minLength=3 maxLength=1024"})
	public void setWhSdcHostIpv6(String whSdcHostIpv6) {
		this.whSdcHostIpv6 = whSdcHostIpv6;
	}

	public int getWhSdcPortSecure() {
		return whSdcPortSecure;
	}

	@UserConfigured(
		value = "46757", 
		description = "The secure port of the SDC", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setWhSdcPortSecure(int whSdcPortSecure) {
		this.whSdcPortSecure = whSdcPortSecure;
	}

	public int getWhSdcPortClear() {
		return whSdcPortClear;
	}

	@UserConfigured(
		value = "46753", 
		description = "The clear port of the SDC", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setWhSdcPortClear(int whSdcPortClear) {
		this.whSdcPortClear = whSdcPortClear;
	}

	@NotNull
	public String getWhSdwHostIpv4() {
		return whSdwHostIpv4;
	}

	@UserConfigured(
		value = "104.239.163.110",
		description = "The IPv4 address of the SDW",
		flexValidator = {"StringValidator minLength=3 maxLength=1024"})
	public void setWhSdwHostIpv4(String whSdwHostIpv4) {
		this.whSdwHostIpv4 = whSdwHostIpv4;
	}

	@NotNull
	public String getWhSdwHostIpv6() {
		return whSdwHostIpv6;
	}

	@UserConfigured(
		value = "2001:4802:7805:101:be76:4eff:fe20:806a",
		description = "The IPv6 address of the SDW",
		flexValidator = {"StringValidator minLength=3 maxLength=1024"})
	public void setWhSdwHostIpv6(String whSdwHostIpv6) {
		this.whSdwHostIpv6 = whSdwHostIpv6;
	}

	public int getWhSdwPortSecure() {
		return whSdwPortSecure;
	}

	@UserConfigured(
		value = "46759", 
		description = "The secure port of the SDW", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setWhSdwPortSecure(int whSdwPortSecure) {
		this.whSdwPortSecure = whSdwPortSecure;
	}

	public int getWhSdwPortClear() {
		return whSdwPortClear;
	}

	@UserConfigured(
		value = "46755", 
		description = "The clear port of the SDW", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setWhSdwPortClear(int whSdwPortClear) {
		this.whSdwPortClear = whSdwPortClear;
	}

	@UserConfigured(value = "43.0", description = "The northwest latitude of the service region for object registration.", 
            flexValidator = { "NumberValidator minValue=-90.0 maxValue=90.0" })
	public void setSvcNorthwestLatitude(double nwLat) {
	      this.nwLat = nwLat;
	}

	public double getSvcNorthwestLatitude() {
	      return this.nwLat;
	}

	@UserConfigured(value = "-85.0", description = "The northwest longitude of the service region for object registration.", 
	            flexValidator = { "NumberValidator minValue=-180.0 maxValue=180.0" })
	public void setSvcNorthwestLongitude(double nwLon) {
	      this.nwLon = nwLon;
	}

	public double getSvcNorthwestLongitude() {
	      return this.nwLon;
	}
	
	@UserConfigured(value = "41.0", description = "The southeast latitude of the service region for object registration.", 
	            flexValidator = { "NumberValidator minValue=-90.0 maxValue=90.0" })
	public void setSvcSoutheastLatitude(double seLat) {
	      this.seLat = seLat;
	}
	
	public double getSvcSoutheastLatitude() {
	      return this.seLat;
	}
	
	@UserConfigured(value = "-82.0", description = "The southeast longitude of the service region for object registration.", 
	            flexValidator = { "NumberValidator minValue=-180.0 maxValue=180.0" })
	public void setSvcSoutheastLongitude(double seLon) {
	      this.seLon = seLon;
	}
	
	@UserConfigured(value = "region", description = "Name of the field to perform geospatial query.", 
			flexValidator = { "StringValidator minLength=2 maxLength=1024" })
	public void setGeospatialFieldName(String geospatialFieldName) {
		this.geospatialFieldName = geospatialFieldName;
	}
	
	public double getSvcSoutheastLongitude() {
	      return this.seLon;
	}

	public void initialize() throws InitializationException {
		try {
			dbHelper = new DatabaseHelper(mongoServerHost, mongoServerPort, 
				databaseName, autoConnectRetry, connectTimeoutMs, collectionName, geospatialFieldName);
			dbHelper.createIndexes();
			receiptSender = new ReceiptSender(receiptJmsHost, receiptJmsPort, topicName);
			responseSender = new ResponseSender(bundleForwarderHost, bundleForwarderPort, forwardAll);
			
			systemRegistrar = new SystemRegistrar();
			Thread t = new Thread(systemRegistrar);
			t.start();
		} catch (Exception ex) {
			throw new InitializationException("Failed to initialize QueryProcessor.", ex);
		}
	}
	
	public void dispose() {
		if (systemRegistrar != null) {
			systemRegistrar.stop();
		}
		if (this.receiptSender != null) {
			this.receiptSender.close();
			this.receiptSender = null;
		}
	}

	@Override
	protected void processInternal(JSONObject record, FlushCounter counter) {
		try {
			logger.debug("Processing registration request: " + record.toString());
			
			long dialogId = record.getInt("dialogId");
			if (dialogId == SemiDialogID.objReg.longValue()) {
				RegisterModel registerModel = RegisterModel.fromJSON(record.toString());
				registerModel.validate();
				dbHelper.upsert(registerModel);
				responseSender.sendResponse(registerModel);
				receiptSender.sendReceipt(registerModel);
			} else {
				logger.error("Received unexpected dialogId: " + dialogId + " expected dialogId:" + SemiDialogID.objReg.longValue());
			}
			
		} catch (Exception ex) {
			logger.error(String.format("Failed to process registration request: %s", record.toString()), ex);
		} finally {
			counter.noop();
		}
	}
	
	public void flush() {
		logger.debug(String.format("The method flush() is not used by this class '%s'.", this.getClass().getName()));
	}
	
	private class SystemRegistrar implements Runnable {

		private boolean stop = false;
		private static final int TWENTY_THREE_HOURS = 1000 * 60 * 60 * 23;
		
		private int groupId = 1;
		private int requestId = 1;
		private int serviceId = 1;
		private int serviceProviderId = 1;
		private int psid = 1;
		
		private String sdcIpv4Clear = whSdcHostIpv4 + "," + whSdcPortClear;
		private String sdcIpv4Secure = whSdcHostIpv4 + "," + whSdcPortSecure;
		private String sdcIpv6Clear = whSdcHostIpv6 + "," + whSdcPortClear;
		private String sdcIpv6Secure = whSdcHostIpv6 + "," + whSdcPortSecure;
		
		private String sdwIpv4Clear = whSdwHostIpv4 + "," + whSdwPortClear;
		private String sdwIpv4Secure = whSdwHostIpv4 + "," + whSdwPortSecure;
		private String sdwIpv6Clear = whSdwHostIpv6 + "," + whSdwPortClear;
		private String sdwIpv6Secure = whSdwHostIpv6 + "," + whSdwPortSecure;
		
		private ArrayList<RegisterModel> registerSystems = new ArrayList<RegisterModel>();
		
		public void run() {
			try {
				// sdc serviceId=1
				serviceId = 1;
				serviceProviderId = 1;
				RegisterModel model = buildRegisterModel(groupId, requestId, serviceId, 
						serviceProviderId, psid, sdcIpv4Clear, sdcIpv4Secure);
				registerSystems.add(model);
				serviceProviderId = 2;
				model = buildRegisterModel(groupId, requestId, serviceId, 
						serviceProviderId, psid, sdcIpv6Clear, sdcIpv6Secure);
				registerSystems.add(model);
				
				// sdw serviceId=2
				serviceId = 2;
				serviceProviderId = 1;
				model = buildRegisterModel(groupId, requestId, serviceId, 
						serviceProviderId, psid, sdwIpv4Clear, sdwIpv4Secure);
				registerSystems.add(model);
				serviceProviderId = 2;
				model = buildRegisterModel(groupId, requestId, serviceId, 
						serviceProviderId, psid, sdwIpv6Clear, sdwIpv6Secure);
				registerSystems.add(model);
				
			} catch (Exception e) {
				logger.error("Failed to build RegisterModel, can't auto register the system", e);
			}
			while (!stop) {
				try {
					for (RegisterModel model: registerSystems) {
						logger.info("Auto registering sysetm: " + model);
						dbHelper.upsert(model);
					}
					Thread.sleep(TWENTY_THREE_HOURS);
				} catch (Exception e) {
					logger.error("Error updating system registration", e);
				}
			}
		}
		
		public RegisterModel buildRegisterModel(
				int groupId,
				int requestId,
				int serviceId,
				int serviceProviderId,
				int psid,
				String ... ipPorts) throws EncodeFailedException, EncodeNotSupportedException {
			
			Position3D nwCnr = CVSampleMessageBuilder.getPosition3D(nwLat, nwLon);
			Position3D seCnr = CVSampleMessageBuilder.getPosition3D(seLat, seLon);
			ObjectRegistrationData objReg = CVSampleMessageBuilder.buildObjectRegistrationData(groupId, requestId, 
					serviceId, serviceProviderId, psid, new GeoRegion(nwCnr, seCnr), ipPorts);
			String encodedMsg = Base64.encodeBase64String(CVSampleMessageBuilder.messageToEncodedBytes(objReg));
			
			return new RegisterModel(serviceId, serviceProviderId, groupId, encodedMsg, nwLat, nwLon, seLat, seLon);
		}
		
		public void stop() {
			stop = true;
		}
	}
}