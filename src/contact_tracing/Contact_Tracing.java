/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contact_tracing;

import com.vividsolutions.jts.util.Assert;
import db.TrajStorage;
import java.io.IOException;
import ds.qtrajtree.TQIndex;
import ds.trajectory.Trajectory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import query.service.DistanceConverter;
import query.service.ServiceQueryProcessor;
import query.service.TestServiceQuery;
/**
 *
 * @author Saqib
 */
public class Contact_Tracing {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.io.FileNotFoundException
     * @throws java.text.ParseException
     */
    public static void main(String[] args) throws IOException, FileNotFoundException, ParseException, SQLException {
        
        double spatialProximity = 2; // should be around 13 feet for example
        String proximityUnit = "m"; // it can be "m", "km", "mile" and "ft"
        long temporalProximity = 30; // in minutes, may be anything around 5 to 240 for example
        temporalProximity *= 60;    // in seconds
        int maxRecursionDepth = 1;
        
        // move the following values (index params and db params) to a text file so that jar does not have to be rebuilt for making some changes
        // index params
        int timeWindowInSec = 30*60;
        int qNodePointCapacity = 128;
        int rTreeBlockTrajCapacity = 4;
        // using default temporal proximity as the time window used in index
        
        // db params
        final String trajTableName = "normalized_trajectory_day_one";
        final int fetchSize = 10000;    // small during dev, larger during run
        final int numOfRows = 20;    // small during dev, removed during run
        final int commitAfterTrajs = 10000;    // small during dev, larger during run
        
        long fromTime = System.nanoTime();
        // create an object of TrajStorage to imitate database functionalities
        TrajStorage trajStorage = new TrajStorage(trajTableName, fetchSize);
        
        TQIndex quadTrajTree = new TQIndex(trajStorage, trajTableName, fetchSize, numOfRows, commitAfterTrajs, timeWindowInSec);
        long toTime = System.nanoTime();
        System.out.println("Index built for " + trajStorage.getTrajCount() + " Trajs, time: " + (toTime-fromTime)/1.0e9);
        System.out.println("Now the program will wait for input in an infinite loop.\n"
                + "Give spatial proximity in meter, followed by temporal proximity in minutes, followed by recursion depth,"
                + "followed by ranking method, followed by comma separated trajectory ids");
        Scanner sc = new Scanner(System.in);
        int rankingMethod;  // 0 denotes earliest exposure, 1 denotes number of exposure
        String [] trajIds;
        
        while(true){
            // expect spatial proximity in meters
            spatialProximity = sc.nextDouble();
            // followed by temporal proximity in minutes
            temporalProximity = sc.nextLong();
            temporalProximity *= 60;
            // followed by max recursion depth
            maxRecursionDepth = sc.nextInt();
            // followed by rankingMethod (0: earliest exposure, 1: number of exposure)
            rankingMethod = sc.nextInt();
            // followed by covid-19 positive user id (anonymized id)
            trajIds = sc.next().split(",");
            
            DistanceConverter distanceConverter = new DistanceConverter(trajStorage.getMaxLon(), trajStorage.getMaxLat(),
                                                                        trajStorage.getMinLon(), trajStorage.getMinLat());
            
            double latProximity = distanceConverter.getLatProximity(spatialProximity, proximityUnit);
            double lonProximity = distanceConverter.getLonProximity(spatialProximity, proximityUnit);
            
            ArrayList <Trajectory> facilityGraph = trajStorage.getTrajectoriesByMultipleIds(trajIds);
            
            TestServiceQuery.run(trajStorage, quadTrajTree, facilityGraph, latProximity, lonProximity, temporalProximity, maxRecursionDepth, rankingMethod);
        }
    }
}
