/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db;

import ds.qtree.Node;
import ds.qtree.Point;
import ds.trajectory.Trajectory;
import ds.transformed_trajectory.TransformedTrajPoint;
import ds.transformed_trajectory.TransformedTrajectory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
/**
 *
 * @author Saqib
 */
public class TrajStorage {
    private HashMap<String, Trajectory> trajData;   // db
    private HashMap<String, TransformedTrajectory> transformedTrajData; // db
    private final int chunkSize;
    private int cursor;
    private HashMap<Node, ArrayList<Point>> tempQNodeToPointListMap;    // db
    private HashMap<String, Integer> trajIdToDiskBlockIdMap;    // in memory
    private HashMap<Integer, ArrayList<String>> diskBlockIdToTrajIdListMap; // in memory
    private ArrayList<String> []pointWiseTrajIdList;    // for query traj selection
    
    // data related stats required at different places
    private final double latCoeff, latConst, lonCoeff, lonConst, maxLat, maxLon, minLat, minLon;
    private final long minTimeInSec;
    private final int trajCount;
    
    public TrajStorage() throws SQLException {
        this.trajData = new HashMap<String, Trajectory>();
        this.transformedTrajData = new HashMap<String, TransformedTrajectory>();
        chunkSize = 10000;
        cursor = 0;
        this.tempQNodeToPointListMap = new HashMap<Node, ArrayList<Point>>();
        this.diskBlockIdToTrajIdListMap = new HashMap<Integer, ArrayList<String>>();
        
        // initialize stats related variables properly from database, change 0 values
        this.latCoeff = 0;
        this.latConst = 0;
        this.lonCoeff = 0;
        this.lonConst = 0;
        
        this.maxLat = 0;
        this.maxLon = 0;
        this.minLat = 0;
        this.minLon = 0;
        
        this.minTimeInSec = 0;
        
        this.trajCount = 0;
        
        // testing with db
        // try to connect to database
        String url = "jdbc:postgresql://ec2-3-132-194-145.us-east-2.compute.amazonaws.com:5432/contact_tracing";
        Properties props = new Properties();
        props.setProperty("user","contact_tracing");
        props.setProperty("password","datalabctq");
        // props.setProperty("ssl","true");
        props.setProperty("sslmode","require");
        Connection conn = DriverManager.getConnection(url, props);
        
        /*
        String url = "jdbc:postgresql://ec2-3-132-194-145.us-east-2.compute.amazonaws.com:5432/contact_tracing?user=contact_tracing&password=datalabctq&ssl=true";
        Connection conn = DriverManager.getConnection(url);
        */
        System.out.println("Connection is successful");
        Statement stmt = conn.createStatement();
        
        String SQL = "SELECT * FROM raw_data LIMIT 10";
        ResultSet rs = stmt.executeQuery(SQL);
        while (rs.next()){
            int pointId = rs.getInt(1);
            String anonymizedId = rs.getString(2);
            double lat = rs.getDouble(3);
            double lon = rs.getDouble(4);
            Timestamp timeStamp = rs.getTimestamp(5);
            
            System.out.println(pointId + " : " + anonymizedId + " (" + lat + "," + lon + "), " + timeStamp + " = " + timeStamp.getTime()/1000);
        }
        
        System.out.println("Using prepared statement");
        SQL = "SELECT * FROM raw_data where anonymous_id = ? LIMIT ?";
        PreparedStatement pstmt = conn.prepareStatement(SQL);
        pstmt.setString(1, "AAH03JAAQAAAO9VAA/");
        pstmt.setInt(2, 7);
        rs = pstmt.executeQuery();
        while (rs.next()){
            int pointId = rs.getInt(1);
            String anonymizedId = rs.getString(2);
            double lat = rs.getDouble(3);
            double lon = rs.getDouble(4);
            Timestamp timeStamp = rs.getTimestamp(5);
            
            System.out.println(pointId + " : " + anonymizedId + " (" + lat + "," + lon + "), " + timeStamp + " = " + timeStamp.getTime()/1000);
        }
        
        conn.close();
    }

    public HashMap<String, Trajectory> getTrajData() {
        return trajData;
    }

    public void setTrajData(HashMap<String, Trajectory> trajData) {
        this.trajData = trajData;
    }
    
    public ArrayList<Trajectory> getTrajDataAsList() {
        return new ArrayList<Trajectory>(trajData.values());
    }
    
    private boolean hasNext(){
        return cursor < trajData.size();
    }
    
    private void resetCursor(){
        cursor = 0;
    }
    
    // gives next x trajectories where x = chunkSize
    public ArrayList<Trajectory> getNextChunkAsList(){
        if (hasNext()){
            int from = cursor;
            int to = cursor + chunkSize;
            if (to > trajData.size()){
                to = trajData.size();
            }
            cursor = to;
            //System.out.println("Returning chunk from " + from + " to " + to);
            return new ArrayList<Trajectory>(getTrajDataAsList().subList(from, to));
        }
        else{
            resetCursor();
            return null;
        }
    }
    
    public Trajectory getTrajectoryById(String Id){
        Trajectory trajectory = trajData.get(Id);
        return trajectory;
    }
    
    // all points of a leaf should be propagated to its child during splitting
    public ArrayList<Point> getPointsFromQNode(Node qNode){
        if (tempQNodeToPointListMap.containsKey(qNode)){
            return tempQNodeToPointListMap.get(qNode);
        }
        return null;
    }
    
    public void addPointToQNode(Node qNode, Point point){
        if (!tempQNodeToPointListMap.containsKey(qNode)){
            tempQNodeToPointListMap.put(qNode, new ArrayList<Point>());
        }
        tempQNodeToPointListMap.get(qNode).add(point);
    }
    
    // a node should be removed after it is no longer leaf (becomes pointer)
    public void removePointListFromQNode(Node qNode){
        if (tempQNodeToPointListMap.containsKey(qNode)){
            tempQNodeToPointListMap.remove(qNode);
        }
    }
    
    public void clearQNodeToPointListMap(){
        tempQNodeToPointListMap.clear();
    }
    
    // should we pass transformed trajectory data chunk by chunk?
    public HashMap<String, TransformedTrajectory> getTransformedTrajData() {
        return transformedTrajData;
    }

    public void setTransformedTrajData(HashMap<String, TransformedTrajectory> transformedTrajData) {
        this.transformedTrajData = transformedTrajData;
    }
    
    public ArrayList<TransformedTrajectory> getTransformedTrajDataAsList() {
        return new ArrayList<TransformedTrajectory>(transformedTrajData.values());
    }
    
    public TransformedTrajectory getTransformedTrajectoryById(String Id){
        TransformedTrajectory transformedTrajectory = transformedTrajData.get(Id);
        return transformedTrajectory;
    }
    
    public void addKeyToTransformedTrajData(String id){
        if (!transformedTrajData.containsKey(id)){
            // using the same method ignoring the second parameter
            transformedTrajData.put(id, new TransformedTrajectory(id, -1));
        }
    }
    
    public void addValueToTransformedTrajData(String id, TransformedTrajPoint transformedTrajPoint){
        addKeyToTransformedTrajData(id);
        transformedTrajData.get(id).addTransformedTrajPoint(transformedTrajPoint);
    }
    
    public HashMap<String, Integer> getTrajIdToDiskBlockIdMap() {
        return trajIdToDiskBlockIdMap;
    }

    public void setTrajIdToDiskBlockIdMap(HashMap<String, Integer> trajToDiskBlockIdMap) {
        this.trajIdToDiskBlockIdMap = trajToDiskBlockIdMap;
    }
    
    public void setDiskBlockIdToTrajIdListMap(){
        for (Map.Entry<String, Integer> entry : trajIdToDiskBlockIdMap.entrySet()) {
            String trajId = entry.getKey();
            Integer blockId = entry.getValue();
            if (!diskBlockIdToTrajIdListMap.containsKey(blockId)){
                diskBlockIdToTrajIdListMap.put(blockId, new ArrayList<String>());
            }
            diskBlockIdToTrajIdListMap.get(blockId).add(trajId);
        }
    }
    
    public Object getDiskBlockIdByTrajId(String id){
        if (!trajIdToDiskBlockIdMap.containsKey(id)) return null;
        return trajIdToDiskBlockIdMap.get(id);
    }
    
    public ArrayList<String> getTrajIdListByBlockId(Integer blockId){
        if (!diskBlockIdToTrajIdListMap.containsKey(blockId)) return null;
        return diskBlockIdToTrajIdListMap.get(blockId);
    }
    
    public void printTrajectories(){
        for (Map.Entry<String, Trajectory> entry : trajData.entrySet()) {
            String trajId = entry.getKey();
            Trajectory traj = entry.getValue();
            System.out.println(traj);
            System.out.println(transformedTrajData.get(trajId));
        }
    }
    
    // the following functions should not be in this file, kept here for shortage of time
    public void prepareQueryDataset(){
        System.out.println("No. of Trajs = " + trajData.size());
        int pointCountInBucket = 50;
        pointWiseTrajIdList = new ArrayList [4];
        for (int i=0; i<pointWiseTrajIdList.length; i++) pointWiseTrajIdList[i] = new ArrayList<String>();
        for (Map.Entry<String, Trajectory> entry : trajData.entrySet()) {
            Trajectory traj = entry.getValue();
            int numberOfPoints = traj.getPointList().size();
            if (numberOfPoints > 500) continue;
            else if (numberOfPoints <= 50) pointWiseTrajIdList[0].add(entry.getKey());
            else if (numberOfPoints <= 100) pointWiseTrajIdList[1].add(entry.getKey());
            else if (numberOfPoints <= 200) pointWiseTrajIdList[2].add(entry.getKey());
            else pointWiseTrajIdList[3].add(entry.getKey());
            /*
            if (trajBucket > 4) trajBucket = 4;
            if (pointWiseTrajIdList[trajBucket] == null) pointWiseTrajIdList[trajBucket] = new ArrayList<String>();
            pointWiseTrajIdList[trajBucket].add(entry.getKey());
            */
        }
        /*
        for (int i=0; i<pointWiseTrajIdList.length; i++){
            System.out.println(i*pointCountInBucket + "-" + ((i+1)*pointCountInBucket-1) + " : " + pointWiseTrajIdList[i].size());
        }
        */
    }
    
    public Trajectory getQueryTrajectory(int pointBucketId){
        if (pointBucketId < 0 || pointBucketId >= pointWiseTrajIdList.length) return null;
        int bucketSize = pointWiseTrajIdList[pointBucketId].size();
        int randomTrajId = (int)(Math.random()*bucketSize);
        if (randomTrajId == bucketSize) randomTrajId--;
        return trajData.get(pointWiseTrajIdList[pointBucketId].get(randomTrajId));
    }
    
    public void generateRandomTrajIds(int numberOfRuns){
        HashSet<String> randomTrajIdList = new HashSet<String>();
        while(numberOfRuns > 0){
            int pointBucketId = (int)(Math.random()*pointWiseTrajIdList.length);
            if (pointBucketId == pointWiseTrajIdList.length) pointBucketId--;
            // if (pointBucketId < 0 || pointBucketId >= pointWiseTrajIdList.length) continue;
            // since 0-th bucket has no entry
            if (pointBucketId < 1 || pointBucketId >= pointWiseTrajIdList.length) continue;
            int bucketSize = pointWiseTrajIdList[pointBucketId].size();
            int randomTrajId = (int)(Math.random()*bucketSize);
            if (randomTrajId == bucketSize) randomTrajId--;
            
            String trajId = (trajData.get(pointWiseTrajIdList[pointBucketId].get(randomTrajId))).getAnonymizedId();
            if (!randomTrajIdList.contains(trajId)){
                numberOfRuns--;
                randomTrajIdList.add(trajId);
            }
        }
        System.out.println("Randomly generated traj ids:");
        for (String trajId : randomTrajIdList){
            System.out.println(trajId);
        }
    }
    
    public double getLatCoeff(){
        return this.latCoeff;
    }
    
    public double getLatConst(){
        return this.latConst;
    }
    
    public double getLonCoeff(){
        return this.lonCoeff;
    }
    
    public double getLonConst(){
        return this.lonConst;
    }

    public double getMaxLat(){
        return this.maxLat;
    }
    
    public double getMaxLon(){
        return this.maxLon;
    }
    
    public double getMinLat(){
        return this.minLat;
    }
    
    public double getMinLon(){
        return this.minLon;
    }
    
    public long getMinTimeInSec(){
        return this.minTimeInSec;
    }
    
    public int getTrajCount(){
        return this.trajCount;
    }
}
