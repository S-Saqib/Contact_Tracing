/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package query.service;

import java.util.TreeSet;
import org.javatuples.Pair;

/**
 *
 * @author Saqib
 */
public class CTQResultTrajData {
    private TreeSet<Pair<Long, Pair<Double, Double>>> allPoints;
    private TreeSet<Pair<Long, Pair<Double, Double>>> exposedPoints;
    private int exposureLevel;
    private String exposedTimestamp;
    private int rankByEarliestExposureTimestamp;
    private int noOfExposures;
    private int rankByNoOfExposures;

    public CTQResultTrajData() {
        this.allPoints =  new TreeSet<>();
        this.exposedPoints = new TreeSet<>();
        this.exposureLevel = -1;
    }
    
    public CTQResultTrajData(int exposureLevel) {
        this.allPoints =  new TreeSet<>();
        this.exposedPoints = new TreeSet<>();
        this.exposureLevel = exposureLevel;
    }
        
    public TreeSet<Pair<Long, Pair<Double, Double>>> getAllPoints() {
        return allPoints;
    }

    public void setAllPoints(TreeSet<Pair<Long, Pair<Double, Double>>> allPoints){
        this.allPoints = allPoints;
    }
    
    public void addPointToAllPoints(Pair<Long, Pair<Double, Double>> pointLocation){
        allPoints.add(pointLocation);
    }

    public TreeSet<Pair<Long, Pair<Double, Double>>> getExposedPoints() {
        return exposedPoints;
    }

    public void setExposedPoints(TreeSet<Pair<Long, Pair<Double, Double>>> exposedPoints) {
        this.exposedPoints = exposedPoints;
    }
    
    public void addPointToExposedPoints(Pair<Long, Pair<Double, Double>> pointLocation){
        exposedPoints.add(pointLocation);
    }

    public int getExposureLevel() {
        return exposureLevel;
    }

    public void setExposureLevel(int exposureLevel) {
        this.exposureLevel = exposureLevel;
    }

    public String getExposedTimestamp() {
        return exposedTimestamp;
    }

    public void setExposedTimestamp(String exposedTimestamp) {
        this.exposedTimestamp = exposedTimestamp;
    }

    public int getRankByEarliestExposureTimestamp() {
        return rankByEarliestExposureTimestamp;
    }

    public void setRankByEarliestExposureTimestamp(int rankByEarliestExposureTimestamp) {
        this.rankByEarliestExposureTimestamp = rankByEarliestExposureTimestamp;
    }

    public int getNoOfExposures() {
        return noOfExposures;
    }

    public void setNoOfExposures(int noOfExposures) {
        this.noOfExposures = noOfExposures;
    }

    public int getRankByNoOfExposures() {
        return rankByNoOfExposures;
    }

    public void setRankByNoOfExposures(int rankByNoOfExposures) {
        this.rankByNoOfExposures = rankByNoOfExposures;
    }
    
}
