/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.real;

import com.vividsolutions.jts.geom.Coordinate;
import ds.trajectory.TrajPoint;
import ds.trajectory.Trajectory;
import java.util.HashMap;
import java.util.TreeSet;

/**
 *
 * @author Saqib
 */
public class TrajNormalizer {
    
    public HashMap<String, Trajectory> normalize (HashMap<String, Trajectory> allTrajectories, double minLon, double minLat, double maxLon, double maxLat){
        // iterate over all the entries in the trajectories, maintained as hashmap of key = anonymized id, value = trajectory object
        for (HashMap.Entry<String, Trajectory> entry : allTrajectories.entrySet()) {
            Trajectory trajectory = entry.getValue();
            TreeSet <TrajPoint> trajPointList = trajectory.getPointList();
            // normalize latitude, longitude values of each point of each trajectory 
            for (TrajPoint trajPoint: trajPointList) {
                Coordinate trajPointLocation = trajPoint.getPointLocation();
                trajPointLocation.setOrdinate(0, (trajPointLocation.x - minLat)*100/(maxLat-minLat));
                trajPointLocation.setOrdinate(1, (trajPointLocation.y - minLon)*100/(maxLon-minLon));
                // update the location values in the trajPoint object
                trajPoint.setPointLocation(trajPointLocation);
            }
            // update the trajPoint list in the trajectory object
            trajectory.setPointList(trajPointList);
        }
        return allTrajectories;
    }
}
