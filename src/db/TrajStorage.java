/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db;

import ds.trajectory.Trajectory;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author Saqib
 */
public class TrajStorage {
    HashMap<String, Trajectory> trajData;
    private final int chunkSize;
    private int cursor;
    
    public TrajStorage(HashMap<String, Trajectory> trajData) {
        this.trajData = trajData;
        chunkSize = 10000;
        cursor = 0;
    }

    public TrajStorage() {
        this.trajData = null;
        chunkSize = 100;
        cursor = 0;
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
    
}
