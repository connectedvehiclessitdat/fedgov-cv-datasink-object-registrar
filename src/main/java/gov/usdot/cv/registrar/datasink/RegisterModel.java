package gov.usdot.cv.registrar.datasink;

import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

import net.sf.json.JSONObject;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RegisterModel {

	private static final ObjectMapper mapper;
	
	static {
		mapper = new ObjectMapper();
	}
	
	public static final String CREATED_AT_KEY		= "createdAt";
	public static final String EXPIRE_AT_KEY		= "expireAt";
	
	public static RegisterModel fromJSON(String json) throws JsonParseException, JsonMappingException, IOException {
		InputStream is = new ByteArrayInputStream(json.getBytes());
		RegisterModel model = mapper.readValue(is, RegisterModel.class);
		model.region = GeoJSONHelper.buildGeoJSON(model.nwPos.lat, model.nwPos.lon, model.sePos.lat, model.sePos.lon);
		return model;
	}
	
	public int dialogId;
	public String receiptId;
	public int sequenceId;
	public int groupId;
	public int requestId;
	public int serviceId;
	public int serviceProviderId;
	
	public String destHost;
	public int destPort;
	public String fromForwarder;
	public String certificate;
	
	public Position nwPos;
	public Position sePos;
	
	public String encodedMsg;
	public JSONObject region;
	
	public static class Position {
		public double lat;
		public double lon;
	}
	
	public RegisterModel() {
	}
	
	public RegisterModel(int serviceId, int serviceProviderId, int groupId, String encodedMsg, 
			double nwLat, double nwLon, double seLat, double seLon) {
		this.serviceId = serviceId;
		this.serviceProviderId = serviceProviderId;
		this.groupId = groupId;
		this.encodedMsg = encodedMsg;
		Position nwPos = new Position();
		nwPos.lat = nwLat;
		nwPos.lon = nwLon;
		this.nwPos = nwPos;
		Position sePos = new Position();
		sePos.lat = seLat;
		sePos.lon = seLon;
		this.sePos = sePos;
		this.region = GeoJSONHelper.buildGeoJSON(nwPos.lat, nwPos.lon, sePos.lat, sePos.lon);
	}
	
	public void validate() throws IllegalArgumentException {
		if (dialogId != SemiDialogID.objReg.longValue()) {
			throw new IllegalArgumentException("Invalid dialogId " + dialogId + " for DiscoveryDataRequest");
		}
		
		if (nwPos == null) {
			throw new IllegalArgumentException("Missing northwest position object.");
		} else {
			validateLat("nwPos.lat", nwPos.lat);
			validateLon("nwPos.lon", nwPos.lon);
		}
		
		if (sePos == null) {
			throw new IllegalArgumentException("Missing southeast position object.");
		} else {
			validateLat("sePos.lat", sePos.lat);
			validateLon("sePos.lon", sePos.lon);
		}
		
		if (encodedMsg == null || encodedMsg.isEmpty()) {
			throw new IllegalArgumentException("encodedMsg is required");
		}
	}
	
	public DBObject getDBObject() {
		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("serviceId", this.serviceId);
		dbObject.put("serviceProviderId", this.serviceProviderId);
		dbObject.put("groupId", this.groupId);
		dbObject.put("encodedMsg", this.encodedMsg);
		dbObject.put("region", this.region);
		
		Calendar cal = Calendar.getInstance();
		dbObject.put(CREATED_AT_KEY, cal.getTime());
		
		// Expiring all Object Registration records 1 day (24 hours) after insert
		cal.add(Calendar.DAY_OF_MONTH, 1);
		dbObject.put(EXPIRE_AT_KEY, cal.getTime());
		
		return dbObject;
	}
	
	private void validateLat(String name, double lat) {
		// 0.0 is invalid for our purposes, catches uninitialized values
		if (lat == 0.0)
			throw new IllegalArgumentException(name + " is required");
		if (lat < -90.0 | lat > 90.0)
			throw new IllegalArgumentException(name + " " + lat + " is not a valid Latitude value");
	}
	
	private void validateLon(String name, double lon) {
		// 0.0 is invalid for our purposes, catches uninitialized values
		if (lon == 0.0)
			throw new IllegalArgumentException(name + " is required");
		if (lon < -180.0 | lon > 180.0)
			throw new IllegalArgumentException(name + " " + lon + " is not a valid Longitude value");
	}

	@Override
	public String toString() {
		return "RegisterModel [dialogId=" + dialogId + ", receiptId="
				+ receiptId + ", sequenceId=" + sequenceId + ", groupId="
				+ groupId + ", requestId=" + requestId + ", serviceId="
				+ serviceId + ", serviceProviderId=" + serviceProviderId
				+ ", destHost=" + destHost + ", destPort=" + destPort
				+ ", fromForwarder=" + fromForwarder + ", certificate="
				+ certificate + ", nwPos=" + nwPos + ", sePos=" + sePos
				+ ", encodedMsg=" + encodedMsg + ", region=" + region + "]";
	}
}
