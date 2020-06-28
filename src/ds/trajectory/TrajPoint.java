/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ds.trajectory;

import com.vividsolutions.jts.geom.Coordinate;

/**
 *
 * @author Saqib
 */
public class TrajPoint {
    private Coordinate pointLocation;
    private long timeInSec;

    public TrajPoint() {
        pointLocation = new Coordinate();
        timeInSec = 0;
    }

    public TrajPoint(Coordinate pointLocation, long timeInSec) {
        this.pointLocation = pointLocation;
        this.timeInSec = timeInSec;
    }
    
    // constructing from the object received from database
    public TrajPoint(Object trajPoint){
        // expectation = Object contains comma separated lat, lon, timestamp
        String[] objValues = trajPoint.toString().split(",");
        double lat = Double.parseDouble(objValues[0].substring(1));
        double lon = Double.parseDouble(objValues[1]);
        pointLocation = new Coordinate(lat, lon);
        timeInSec = Long.parseLong(objValues[2].substring(0, objValues[2].length()-1));
    }

    public void setPointLocation(Coordinate pointLocation) {
        this.pointLocation = pointLocation;
    }

    public void setTimeInSec(long timeInSec) {
        this.timeInSec = timeInSec;
    }

    public Coordinate getPointLocation() {
        return pointLocation;
    }

    public long getTimeInSec() {
        return timeInSec;
    }

    @Override
    public String toString() {
        return "TrajPoint{" + "Coordinate = " + pointLocation + " , timestamp = " + timeInSec + '}';
    }
    
    
}
