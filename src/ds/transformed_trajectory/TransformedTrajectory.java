package ds.transformed_trajectory;

import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import java.util.ArrayList;

import java.util.TreeSet;

public class TransformedTrajectory {
    
    private TreeSet<TransformedTrajPoint> transformedPointList;
    private Rectangle envelope;
    private long userId;
    private String anonymizedId;
    private String contactNo;
    private double minQnodeIndex;
    private double minTimeIndex;
    private double maxQnodeIndex; 
    private double maxTimeIndex;

    public TransformedTrajectory() {
        transformedPointList = new TreeSet<TransformedTrajPoint>(new TransformedTrajPointComparator());
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
        minQnodeIndex = minTimeIndex = Double.MAX_VALUE;
        maxQnodeIndex = maxTimeIndex = Double.MIN_VALUE;
    }
    
    public TransformedTrajectory(String anonymizedId, long userId){
        transformedPointList = new TreeSet<TransformedTrajPoint>(new TransformedTrajPointComparator());
        this.anonymizedId = anonymizedId;
        this.userId = userId;
        contactNo = null;
        minQnodeIndex = minTimeIndex = Double.MAX_VALUE;
        maxQnodeIndex = maxTimeIndex = Double.MIN_VALUE;
    }
    
    public TransformedTrajectory(TreeSet<TransformedTrajPoint> pointList) {
        this.transformedPointList = pointList;
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
        minQnodeIndex = minTimeIndex = Double.MAX_VALUE;
        maxQnodeIndex = maxTimeIndex = Double.MIN_VALUE;
    }
    
    public void setTransformedPointList(TreeSet<TransformedTrajPoint> transformedPointList) {
        this.transformedPointList = transformedPointList;
    }

    public TreeSet<TransformedTrajPoint> getTransformedPointList() {
        return this.transformedPointList;
    }
    
    public String getAnonymizedId() {
        return anonymizedId;
    }

    public void setAnonymizedId(String anonymizedId) {
        this.anonymizedId = anonymizedId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getContactNo() {
        return contactNo;
    }

    public void setContactNo(String contactNo) {
        this.contactNo = contactNo;
    }
    
    public void addTransformedTrajPoint(TransformedTrajPoint p){
        if (transformedPointList == null){
            // should not come here
            transformedPointList = new TreeSet<TransformedTrajPoint>(new TransformedTrajPointComparator());
        }
        transformedPointList.add(p);
    }
      
    public boolean contains(TransformedTrajPoint p){
        return transformedPointList.contains(p);
    }

    public void updateMinMaxBounds(TransformedTrajPoint p){
        minQnodeIndex = Math.min(minQnodeIndex, p.getqNodeIndex());
        maxQnodeIndex = Math.max(maxQnodeIndex, p.getqNodeIndex());
        minTimeIndex = Math.min(minTimeIndex, p.getTimeIndex());
        maxTimeIndex = Math.max(maxTimeIndex, p.getTimeIndex());
    }
    
    public void setEnvelope() {
        double minQnodeIndex, maxQnodeIndex, minTimeIndex, maxTimeIndex;
        minQnodeIndex = minTimeIndex = Double.MAX_VALUE;
        maxQnodeIndex = maxTimeIndex = Double.MIN_VALUE;
        for (TransformedTrajPoint p: this.transformedPointList){
            minQnodeIndex = Math.min(minQnodeIndex, p.getqNodeIndex());
            maxQnodeIndex = Math.max(maxQnodeIndex, p.getqNodeIndex());
            minTimeIndex = Math.min(minTimeIndex, p.getTimeIndex());
            maxTimeIndex = Math.max(maxTimeIndex, p.getTimeIndex());
        } 
        double[] mins = new double[]{minQnodeIndex, minTimeIndex};
        double[] maxes = new double[]{maxQnodeIndex, maxTimeIndex};
        this.envelope = Rectangle.create(mins, maxes);
    }
    
    public void setEnvelope(boolean minMaxBoundsUpdated){
        if (minMaxBoundsUpdated){
            double[] mins = new double[]{minQnodeIndex, minTimeIndex};
            double[] maxes = new double[]{maxQnodeIndex, maxTimeIndex};
            this.envelope = Rectangle.create(mins, maxes);
        }
        else setEnvelope();
    }

    public Rectangle getEnvelope() {
        return envelope;
    }
    
    

    @Override
    public String toString() {
        String trajString = "(Transformed) Trajectory ID = " + userId + " , Anonymized ID = " + anonymizedId + " , Contact No. = " + contactNo + "\n";
        trajString += transformedPointList.toString() + "\n";
        return trajString;
    }
    
    
        
}
