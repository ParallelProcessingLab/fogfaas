package org.fog.serverless.faas;


import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.fog.serverless.scheduler.FogFaaSRequestScheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * Container Class for FoGFaaS extension.
 *
 * @author Ghaseminya
 * Created on 12/16/2024
 */

public class FogFaaSContainer extends Container {
    private  ArrayList<FogFaaSRequest> pendingTasks = new ArrayList<>();
    /**
     * The running task list for the container
     */
    private  ArrayList<FogFaaSRequest> runningTasks = new ArrayList<>();

    /**
     * The running task list for the container
     */
    private  ArrayList<FogFaaSRequest> finishedTasks = new ArrayList<>();
    /**
     * Container type
     */
    private  String functionType = null;
    boolean newContainer = false;
    private boolean reschedule = false;
    private boolean idling = false;

    private double startTime = 0;
    private double finishTime = 0;
    private double idleStartTime = 0;
    public FogFaaSContainer(int id, int userId, String type, double mips, int numberOfPes, int ram, long bw, long size, String containerManager, ContainerCloudletScheduler containerRequestScheduler, double schedulingInterval, boolean newCont, boolean idling, boolean reschedule, double idleStartTime, double startTime, double finishTime) {
        super(id, userId, mips, numberOfPes, ram, bw, size, containerManager, containerRequestScheduler, schedulingInterval);
        this.newContainer = newCont;
        setReschedule(reschedule);
        setIdling(idling);
        setType(type);
        setIdleStartTime(idleStartTime);
    }
    public void setReschedule(boolean reschedule){this.reschedule = reschedule;}
    public void setIdling(boolean idling){this.idling = idling;}

    public void setIdleStartTime(double time){this.idleStartTime = time;}
    public void setStartTime(double time){this.startTime = time;}
    public void setFinishTime(double time){this.finishTime = time;}
    public void setType(String type){this.functionType = type;}
    public void setPendingTask(FogFaaSRequest task){
    	pendingTasks.add(task);
    }
    public void setRunningTask(FogFaaSRequest task){runningTasks.add(task); }

    public void setfinishedTask(FogFaaSRequest task){finishedTasks.add(task); }
    public boolean getReschedule() {return reschedule;}
    public boolean getIdling() {return idling;}
    public double getIdleStartTime(){return idleStartTime;}
    public double getStartTime(){return startTime;}
    public double getFinishTime(){return finishTime;}

    public FogFaaSRequest getPendingTask(int index){
        return pendingTasks.get(index);
    }
    public FogFaaSRequest getRunningTask(int index){
        return runningTasks.get(index);
    }
    public ArrayList<FogFaaSRequest> getfinishedTasks(){
    	return finishedTasks;
    }
    public ArrayList<FogFaaSRequest> getRunningTasks(){
    	return runningTasks;
    }
    public String getType() {return functionType;}


    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, FogFaaSInvoker vm) {
        if (mipsShare != null) {
        	return ((FogFaaSRequestScheduler) getContainerCloudletScheduler()).updateContainerProcessing(currentTime, mipsShare,vm);
        }
        return 0.0;
    }

    
}