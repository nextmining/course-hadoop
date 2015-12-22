package com.nextmining.common.util;

public class GeoUtil {

	/**
	 * This routine calculates the distance between two points (given the latitude/longitude of those points). 
	 * 
	 * @param lat1
	 *          Latitude of point 1 (in decimal degrees)
	 * @param lon1
	 *          Longitude of point 1 (in decimal degrees)
	 * @param lat2
	 *          Latitude of point 2 (in decimal degrees)
	 * @param lon2
	 *          Longitude of point 2 (in decimal degrees)
	 * @param unit
	 *          the unit you desire for results 'M' is statute miles 'K' is kilometers (default) 'N' is nautical miles
	 * @return
	 */
	public static double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
				* Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		if (unit == 'K') {
			dist = dist * 1.609344;
		} else if (unit == 'N') {
			dist = dist * 0.8684;
		}
		return (dist);
	}

	/**
	 * This function converts decimal degrees to radians.
	 * 
	 * @param deg
	 * @return
	 */
	private static double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	/**
	 * This function converts radians to decimal degrees.
	 * 
	 * @param rad
	 * @return
	 */
	private static double rad2deg(double rad) {
		return (rad * 180.0 / Math.PI);
	}
	
	public static void main(String[] args) {
		double lat1 = 37.608;
		double lon1 = 127.094;
		double lat2 = 37.565;
		double lon2 = 127.078;
		
		System.out.println("distance == " + distance(lat1, lon1, lat2, lon2, 'K'));
	}

}
