package gov.usdot.cv.registrar.datasink;

import gov.usdot.cv.common.database.mongodb.geospatial.Coordinates;
import gov.usdot.cv.common.database.mongodb.geospatial.Geometry;
import gov.usdot.cv.common.database.mongodb.geospatial.Point;
import net.sf.json.JSONObject;

public class GeoJSONHelper {

	private static final String POLYGON_GEOMETRY_TYPE = "Polygon";
	//private static final String POINT_GEOMETRY_TYPE = "Point";
	
	public static JSONObject buildGeoJSON(double nwLat, double nwLon, double seLat, double seLon) {
		Point nwCorner = buildPoint(nwLat, nwLon);
		Point neCorner = buildPoint(nwLat, seLon);
		Point seCorner = buildPoint(seLat, seLon);
		Point swCorner = buildPoint(seLat, nwLon);
	
		Geometry geometry = buildGeometry(POLYGON_GEOMETRY_TYPE, 
				buildCoordinates(nwCorner, neCorner, seCorner, swCorner));
	
		return geometry.toJSONObject();
	}
	
	private static Point buildPoint(Double lat, Double lon) {
		Point.Builder builder = new Point.Builder();
		builder.setLat(lat).setLon(lon);
		return builder.build();
	}
	
	private static Geometry buildGeometry(String type, Coordinates coordinates) {
		Geometry.Builder builder = new Geometry.Builder();
		builder.setType(type).setCoordinates(coordinates);
		return builder.build();
	}
	
	private static Coordinates buildCoordinates(
			Point nwCorner, 
			Point neCorner, 
			Point seCorner, 
			Point swCorner) {
		Coordinates.Builder builder = new Coordinates.Builder();
		// Note: geojson requires that all geometry shape start and end at the same point
		builder.addPoint(nwCorner).addPoint(neCorner).addPoint(seCorner).addPoint(swCorner).addPoint(nwCorner);
		return builder.build();
	}
}
