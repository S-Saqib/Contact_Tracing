/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db;

import ds.qtree.Node;
import ds.qtree.Point;
import ds.trajectory.TrajPoint;
import ds.trajectory.Trajectory;
import ds.transformed_trajectory.TransformedTrajPoint;
import ds.transformed_trajectory.TransformedTrajectory;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
/**
 *
 * @author Saqib
 */
public class TrajStorage {
    private HashMap<String, Trajectory> trajData;   // db
    private HashMap<String, TransformedTrajectory> transformedTrajData; // db

    private HashMap<String, Integer> trajIdToDiskBlockIdMap;    // in memory
    private HashMap<Integer, ArrayList<String>> diskBlockIdToTrajIdListMap; // in memory
    private ArrayList<String> []pointWiseTrajIdList;    // for query traj selection
    
    // data related stats required at different places
    private final double latCoeff, latConst, lonCoeff, lonConst, maxLat, maxLon, minLat, minLon;
    private final long minTimeInSec;
    
    private final DbInterface dbInterface;
    private final String trajTableName;
    private final int fetchSize;
    
    public TrajStorage(String trajTableName, int fetchSize) throws SQLException {
        this.trajData = new HashMap<String, Trajectory>();
        this.transformedTrajData = new HashMap<String, TransformedTrajectory>();
        this.diskBlockIdToTrajIdListMap = new HashMap<Integer, ArrayList<String>>();
        
        // initialize stats related variables properly from database
        long fromTime = System.currentTimeMillis();
        this.dbInterface = new DbInterface();
        
        this.trajTableName = trajTableName;
        this.fetchSize = fetchSize;
        
        String statsQuery = "SELECT max((point).lat), max((point).lng), min((point).lat), min((point).lng), min((point).ts) FROM raw_data";
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(statsQuery);
        
        ResultSet rs = pstmt.executeQuery();
        if(rs.next()){
            this.maxLat = rs.getDouble(1);
            this.maxLon = rs.getDouble(2);
            this.minLat = rs.getDouble(3);
            this.minLon = rs.getDouble(4);
            this.minTimeInSec = rs.getLong(5);
            
            this.latConst = this.minLat;
            this.lonConst = this.minLon;
            this.latCoeff = (this.maxLat - this.minLat)/100.0;
            this.lonCoeff = (this.maxLon - this.minLon)/100.0;
        }
        else{
            this.maxLat = 0;
            this.maxLon = 0;
            this.minLat = 0;
            this.minLon = 0;
            this.minTimeInSec = 00;
            
            this.latConst = 0;
            this.lonConst = 0;
            this.latCoeff = 0;
            this.lonCoeff = 0;
        }
        
        long toTime = System.currentTimeMillis();
        System.out.println("Stats retrieval : " + (toTime-fromTime)/1000 + " seconds");
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
        
    public Trajectory getTrajectoryById(String Id){
        Trajectory trajectory = trajData.get(Id);
        return trajectory;
    }
    
    public ArrayList<Trajectory> getTrajectoriesByMultipleIds(String[] trajIdList) throws SQLException{
        ArrayList<Trajectory> trajList = new ArrayList<Trajectory>();
        
        String normalizedTrajQuery = "select normalized_points from " + trajTableName + " where anonymous_id in + (?";
        for (int i=1; i<trajIdList.length; i++){
            normalizedTrajQuery += ",?";
        }
        normalizedTrajQuery += ")";
        
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(normalizedTrajQuery);
        
        for (int i=0; i<trajIdList.length; i++){
            String trajId = trajIdList[i];
            pstmt.setString(i+1, trajId);
        }
        
        pstmt.setFetchSize(fetchSize);
        
        int trajsProcessed = 0;
        
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()){
            String anonymizedId = trajIdList[trajsProcessed++];
            Array trajPointArray = rs.getArray(1);
            Object[] trajPointList = (Object[])trajPointArray.getArray();
            int pointCount = 0;
            Trajectory traj = new Trajectory(anonymizedId, -1);
            for(Object trjPtObj: trajPointList){
                TrajPoint trajPoint = new TrajPoint(trjPtObj);
                double normalizedLat = trajPoint.getPointLocation().x;
                double normalizedLon = trajPoint.getPointLocation().y;
                long trajPointTimeInSec = trajPoint.getTimeInSec();
                if (normalizedLat < 0 || normalizedLat > 100 || normalizedLon < 0 || normalizedLon > 100){
                    if (normalizedLat < 0){
                        if (normalizedLat > -1.0e-6){
                            normalizedLat = 0;
                            trajPoint.getPointLocation().setOrdinate(0, normalizedLat);
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lat");
                            continue;
                        }
                    }
                    else if (normalizedLat > 100){
                        if (normalizedLat-100 < 1.0e-6){
                            normalizedLat = 100;
                            trajPoint.getPointLocation().setOrdinate(0, normalizedLat);
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lat");
                            continue;
                        }
                    }
                    else{
                        // ok, no issues with lat
                    }
                    if (normalizedLon < 0){
                        if (normalizedLon > -1.0e-6){
                            normalizedLon = 0;
                            trajPoint.getPointLocation().setOrdinate(1, normalizedLon);
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lon");
                            continue;
                        }
                    }
                    else if (normalizedLon > 100){
                        if (normalizedLon-100 < 1.0e-6){
                            normalizedLon = 100;
                            trajPoint.getPointLocation().setOrdinate(1, normalizedLon);
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lon");
                            continue;
                        }
                    }
                    else{
                        // ok, no issues with lon
                    }
                }
                traj.addTrajPoint(trajPoint);
            }
            trajList.add(traj);
        }
        return trajList;
    }
        
    public ArrayList<Point> getPointsFromQNode(Node qNode) throws SQLException{
        // SQL: select points from qnode_to_point_map where node = row(0, 0, 50, 50)::qnode;
        String qNodePointFetchQuery = "select points, traj_ids from qnode_to_point_map where node = row(?, ?, ?, ?)::qnode";
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(qNodePointFetchQuery);

        pstmt.setDouble(1, qNode.getX());
        pstmt.setDouble(2, qNode.getY());
        pstmt.setDouble(3, qNode.getW());
        pstmt.setDouble(4, qNode.getH());

        ArrayList<Point> qNodePointList = new ArrayList<Point>();

        ResultSet rs = pstmt.executeQuery();
        while (rs.next()){
            Array pointArray = rs.getArray(1);
            Object[] pointList = (Object[])pointArray.getArray();
            Array trajIdArray = rs.getArray(2);
            Object[] trajIdList = (Object[])trajIdArray.getArray();
            int curTrajIndex = 0;
            for(Object ptObj : pointList){
                Point point = new Point(ptObj, trajIdList[curTrajIndex++]);
                qNodePointList.add(point);
            }
        }

        if (qNodePointList.size() == 0){
            System.out.println("Something went wrong while retrieving point list of qnode " + qNode);
        }
        return qNodePointList;
    }
    
    public void addPointToQNode(Node qNode, Point point) throws SQLException{
        String qNodePointMapNodeInsQuery = "insert into qnode_to_point_map (node) values (row(?, ?, ?, ?)) on conflict do nothing;";
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(qNodePointMapNodeInsQuery);

        pstmt.setDouble(1, qNode.getX());
        pstmt.setDouble(2, qNode.getY());
        pstmt.setDouble(3, qNode.getW());
        pstmt.setDouble(4, qNode.getH());

        int affectedRows = pstmt.executeUpdate();

        if (affectedRows > 1){
            System.out.println("Something went wrong while inserting qnode " + qNode);
            System.out.println(affectedRows + " Rows affected");
        }
        
        // tempQNodeToPointListMap.get(qNode).add(point);
        
        String qNodePointMapPointInsQuery = "update qnode_to_point_map set points = array_append(points,row(?, ?, ?)::trajectory_point),"
                                            + " traj_ids = array_append(traj_ids,?) where node = row(?, ?, ?, ?)::qnode;";
        pstmt = dbInterface.getConnection().prepareStatement(qNodePointMapPointInsQuery);
        
        pstmt.setDouble(1, point.getX());
        pstmt.setDouble(2, point.getY());
        pstmt.setLong(3, point.getTimeInSec());
        pstmt.setString(4, (String)point.getTraj_id());
        
        pstmt.setDouble(5, qNode.getX());
        pstmt.setDouble(6, qNode.getY());
        pstmt.setDouble(7, qNode.getW());
        pstmt.setDouble(8, qNode.getH());
        
        affectedRows = pstmt.executeUpdate();
        
        if (affectedRows != 1){
            System.out.println("Something went wrong while inserting point " + point + " at " + qNode);
        }
    }
    
    // a node should be removed after it is no longer leaf (becomes pointer)
    public void removePointListFromQNode(Node qNode) throws SQLException{            
        // SQL: delete from qnode_to_point_map where node = row(50, 75, 25, 25)::qnode;
        String qNodePointDeleteQuery = "delete from qnode_to_point_map where node = row(?, ?, ?, ?)::qnode";
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(qNodePointDeleteQuery);

        pstmt.setDouble(1, qNode.getX());
        pstmt.setDouble(2, qNode.getY());
        pstmt.setDouble(3, qNode.getW());
        pstmt.setDouble(4, qNode.getH());

        int affectedRows = pstmt.executeUpdate();

        if (affectedRows != 1){
            System.out.println("Something went wrong while deleting row of qnode " + qNode);
        }
    }
    
    public void clearQNodeToPointListMap() throws SQLException{
        // tempQNodeToPointListMap.clear();
        String qNodePointClearQuery = "delete from qnode_to_point_map";
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(qNodePointClearQuery);
        int affectedRows = pstmt.executeUpdate();

        if (affectedRows == 0){
            System.out.println("Something went wrong while deleting all rows of qnode point map table");
        }
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
        // transformed trajectory: remove point addition, min max value update for envelope
        addKeyToTransformedTrajData(id);
        //transformedTrajData.get(id).addTransformedTrajPoint(transformedTrajPoint);
        transformedTrajData.get(id).updateMinMaxBounds(transformedTrajPoint);
    }
    
    public HashMap<String, Integer> getTrajIdToDiskBlockIdMap() {
        return trajIdToDiskBlockIdMap;
    }

    public void setTrajIdToDiskBlockIdMap(HashMap<String, Integer> trajToDiskBlockIdMap) {
        this.trajIdToDiskBlockIdMap = trajToDiskBlockIdMap;
    }
    
    public void setDiskBlockIdToTrajIdListMap() throws SQLException{
        for (Map.Entry<String, Integer> entry : trajIdToDiskBlockIdMap.entrySet()) {
            String trajId = entry.getKey();
            Integer blockId = entry.getValue();
            if (!diskBlockIdToTrajIdListMap.containsKey(blockId)){
                diskBlockIdToTrajIdListMap.put(blockId, new ArrayList<String>());
            }
            diskBlockIdToTrajIdListMap.get(blockId).add(trajId);
            
            // update normalized trajectory table column in database
            //SQL: update normalized_trajectory_day_one set rtree_block_id = 0 where anonymous_id = 'AAH03JAAQAAAO9VAA4'
            String updateDiskBlockQuery = "update " + trajTableName + " set rtree_block_id = ? where anonymous_id = ?";
            PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(updateDiskBlockQuery);

            pstmt.setInt(1, blockId);
            pstmt.setString(2, trajId);
            
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows != 1){
                System.out.println("Something went wrong while assigning disk block id to trajectory " + trajId);
            }
            
        }
    }
    
    public void clusterTrajData() throws SQLException{
        final String indexName = "rtree_block_id_" + trajTableName + "_idx";
                
        String indexQuery = "create index if not exists " + indexName + " on " + trajTableName + " (rtree_block_id)";
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(indexQuery);
        pstmt.execute();
        
        String clusterQuery = "cluster " + trajTableName + " using " + indexName;
        pstmt = dbInterface.getConnection().prepareStatement(clusterQuery);
        pstmt.execute();
    }
    
    public Object getDiskBlockIdByTrajId(String id){
        if (!trajIdToDiskBlockIdMap.containsKey(id)) return null;
        return trajIdToDiskBlockIdMap.get(id);
    }
    
    public ArrayList<String> getTrajIdListByBlockId(Integer blockId){
        if (!diskBlockIdToTrajIdListMap.containsKey(blockId)) return null;
        return diskBlockIdToTrajIdListMap.get(blockId);
    }
    
    // may need to complete this function later if filtering by traj id is bad
    public ArrayList<Trajectory> getTrajsByMultipleBlockId(int[] blockIds){
        return null;
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
        return this.trajIdToDiskBlockIdMap.size();
    }
    
    public DbInterface getDbInterface(){
        return dbInterface;
    }
}
