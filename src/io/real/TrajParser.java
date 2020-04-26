/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.real;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.util.Assert;
import ds.trajectory.TrajPoint;
import ds.trajectory.Trajectory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 *
 * @author Saqib
 */
public class TrajParser {

    private double minLon, maxLon, minLat, maxLat, latCoeff, latConst, lonCoeff, lonConst;
    private final double  latLowerLimit, latUpperLimit, lonLowerLimit, lonUpperLimit;
    
    public TrajParser(){
        // the following variables are used in spatial normalization
        minLon = minLat = 1000;
        maxLon = maxLat = -1000;
        // the following variables can be used in spatial denormalization if needed
        latCoeff = 0;
        latConst = 0;
        lonCoeff = 0;
        lonConst = 0;
        // the following limits are used to remove spatially noisy data (if any)
        latLowerLimit = 20; // 20
        latUpperLimit = 27; // 27
        lonLowerLimit = 88; // 88
        lonUpperLimit = 93; // 93
        // there may be some temporal noise which we do not know about, if there is, it should be cleaned as well
    }
    
    public HashMap<String, Trajectory> parseUserTrajectories(String path) throws FileNotFoundException, IOException, ParseException{
        File userTrajectoryFile = new File(path);
        Assert.isTrue(userTrajectoryFile.exists(), "user trajectory file not found");

        BufferedReader br = null;
        String line = new String();

        br = new BufferedReader(new FileReader(path));
        
        // maintains a map of anonymized id to the whole trajectory data (may use an integer user id instead of anonymized id later
        HashMap<String, Trajectory> allTrajectories = new HashMap<String, Trajectory>();
        // used to assign auto incrementing user ids to trajectories
        long userCount = 0;
        
        // parse each line and add its point (location, time) to the appropriate trajectory
        while ((line = br.readLine()) != null) {
            // different fields in each line are extracted and quotes are trimmed
            String[] data = line.split(",");
            String anonymizedId = data[0].substring(1, data[0].length()-1);
            String date = data[1].substring(1, data[1].length()-1);
            String time = data[2].substring(1, data[2].length()-1);
            String unknown = data[3].substring(1, data[3].length()-1);
            String latitude = data[4].substring(1, data[4].length()-1);
            String longitude = data[5].substring(1, data[5].length()-1);
            
            // when an anonymized id is encountered the first time, a new entry in the map is generated
            if (!allTrajectories.containsKey(anonymizedId)){
                allTrajectories.put(anonymizedId, new Trajectory(anonymizedId, userCount));
                userCount++;
            }
            
            Coordinate trajPointCoord = new Coordinate(Double.parseDouble(latitude), Double.parseDouble(longitude));
            // calculate timestamp (timeInSec) with simple date format, its parse and getTime methods and converting obtained ms value to seconds
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
            long timeInSec = dateTimeFormat.parse(date + " " + time).getTime()/1000;
            
            // remove spatial noise (outside our desired geographical zone)
            if (trajPointCoord.x < latLowerLimit || trajPointCoord.x > latUpperLimit) continue;
            if (trajPointCoord.y < lonLowerLimit || trajPointCoord.y > lonUpperLimit) continue;
            
            // update the boundary values for normalization
            if (trajPointCoord.x < minLat){
                minLat = trajPointCoord.x;
            }
            else if (trajPointCoord.x > maxLat){
                maxLat = trajPointCoord.x;
            }
            if (trajPointCoord.y < minLon){
                minLon = trajPointCoord.y;
            }
            else if (trajPointCoord.y > maxLon){
                maxLon = trajPointCoord.y;
            }
            
            // a point containing location and time is constructed from the recently processed values and inserted into the trajectory
            TrajPoint trajPoint = new TrajPoint(trajPointCoord, timeInSec);
            allTrajectories.get(anonymizedId).addTrajPoint(trajPoint);
        }
        br.close();
        
        // location values are normalized in [0, 100] range
        TrajNormalizer trajNormalizer = new TrajNormalizer();
        allTrajectories = trajNormalizer.normalize(allTrajectories, minLon, minLat, maxLon, maxLat);
        
        // the denormalizing variables are updated
        latCoeff = (maxLat/100.0-minLat/100.0);
        latConst = minLat;
        lonCoeff = (maxLon/100.0-minLon/100.0);
        lonConst = minLon;
        
        return allTrajectories; 
    }
    
    public double getLatCoeff() {
        return latCoeff;
    }

    public double getLatConst() {
        return latConst;
    }

    public double getLonCoeff() {
        return lonCoeff;
    }

    public double getLonConst() {
        return lonConst;
    }

    public double getMinLon() {
        return minLon;
    }

    public double getMaxLon() {
        return maxLon;
    }

    public double getMinLat() {
        return minLat;
    }

    public double getMaxLat() {
        return maxLat;
    }
    
}
