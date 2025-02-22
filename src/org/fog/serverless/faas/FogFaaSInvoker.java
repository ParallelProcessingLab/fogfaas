package org.fog.serverless.faas;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.serverless.utils.Constants;

import java.util.*;

/**
 * AutoScaler Class for FoGFaaS extension.
 *
 * @author Ghaseminya
 * Created on 12/16/2024
 */

public class FogFaaSInvoker extends FogDevice {
    /**
     * The container type map of FoG - contains the function type and the container list
     */
    private  Map<String, ArrayList<Container>> functionContainerMap = new HashMap<String, ArrayList<Container>>();
    /**
     * The pending container type map of FoG - contains the function type and the pending container list
     */
    private  Map<String, ArrayList<Container>> functionContainerMapPending = new HashMap<String, ArrayList<Container>>();

    /**
     * The task type map of FoG - contains the function type and the task count
     */

    private Map<String, Integer> fogTaskMap = new HashMap<>();
    /**
     * Records the requests in execution as per the execution order
     */

    private Stack<FogFaaSRequest> runningrequestStack = new Stack<FogFaaSRequest>();
    private  ArrayList<FogFaaSRequest> runningrequestList = new ArrayList<>();

    /**
     * The task  map of FoG - contains the function type and the task
     */
    private Map<String, ArrayList<FogFaaSRequest>> fogTaskExecutionMap = new HashMap<>();
    private Map<String, ArrayList<FogFaaSRequest>> fogTaskExecutionMapFull = new HashMap<>();

    /**
     * On Off status of FoG
     */

    private  String status = null;

    /**
     * On Off status record time of FoG
     */

    private  double recordTime = 0;

    public double onTime = 0;
    public double offTime  = 0;
    public boolean used = false;



    public FogFaaSInvoker(int id, int userId, double mips, float ram, long bw, long size, String FoGm, 
    		ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner,
    		ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval,
    		FogDeviceCharacteristics characteristics,LinkedList<Storage> storageList,List<Host> hostList, double ratePerMips
    		) throws Exception {
//        super(id, userId, mips, ram, bw, size, FoGm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
    	
    	super(""+id, characteristics, 
				new AppModuleAllocationPolicy(hostList), storageList, 10, bw, bw, 0, ratePerMips);
    	
    	
    }

    public Map<String, Integer> getfogTaskMap(){return fogTaskMap;}

    public Map<String, ArrayList<FogFaaSRequest>> getVmTaskExecutionMap(){return fogTaskExecutionMap;}
    public Map<String, ArrayList<FogFaaSRequest>> getVmTaskExecutionMapFull(){return fogTaskExecutionMapFull;}
    public String getStatus(){
        return status;
    }
    public ArrayList<FogFaaSRequest> getRunningRequestList() {
        return runningrequestList;
    }

    public void setStatus(String FoGStatus){
        status = FoGStatus;
    }

    public void setRecordTime(double time){
        recordTime = time;
    }

    public double getRecordTime(){
        return recordTime;
    }

    public void setFunctionContainerMap(Container container, String functionId){
        //System.out.println("Debug: Before: Set map "+ this.getId()+" "+functionContainerMap);

        if(!functionContainerMap.containsKey(functionId)){
            ArrayList<Container> containerListMap = new ArrayList<>();
            containerListMap.add(container);
            functionContainerMap.put(functionId,containerListMap );
        }
        else {
            if(!functionContainerMap.get(functionId).contains(container)){
                functionContainerMap.get(functionId).add(container);
            }


        }
    }
    public void setFunctionContainerMapPending(Container container, String functionId){
        //System.out.println("Debug: Before: Set map "+ this.getId()+" "+functionContainerMap);

        if(!functionContainerMapPending.containsKey(functionId)){
            ArrayList<Container> containerListMap = new ArrayList<>();
            containerListMap.add(container);
            functionContainerMapPending.put(functionId,containerListMap );
        }
        else {
            if(!functionContainerMapPending.get(functionId).contains(container)){
                functionContainerMapPending.get(functionId).add(container);
            }


        }
    }
    public Map<String, ArrayList<Container>> getFunctionContainerMap(){
        return functionContainerMap;
    }
    public Map<String, ArrayList<Container>> getFunctionContainerMapPending(){
        return functionContainerMapPending;
    }
    public void addToFogTaskExecutionMap(FogFaaSRequest task, FogFaaSInvoker fog){

        /** Adding to full task map **/
        int countFull = fogTaskExecutionMapFull.containsKey(task.getRequestFunctionId()) ? (fogTaskExecutionMapFull.get(task.getRequestFunctionId())).size(): 0;

        if(countFull == 0){
            ArrayList<FogFaaSRequest> taskListFull = new ArrayList<>();
            taskListFull.add(task);
            fogTaskExecutionMapFull.put(task.getRequestFunctionId(),taskListFull);
        }
        else{
            fogTaskExecutionMapFull.get(task.getRequestFunctionId()).add(task);
        }

        /** Adding to moving average task map **/

        int count = fogTaskExecutionMap.containsKey(task.getRequestFunctionId()) ? (fogTaskExecutionMap.get(task.getRequestFunctionId())).size(): 0;
        //vm.getVmTaskExecutionMap().put(task.getrequestFunctionId(), count+1);

        if(count == 0){
            ArrayList<FogFaaSRequest> taskList = new ArrayList<>();
            taskList.add(task);
            fogTaskExecutionMap.put(task.getRequestFunctionId(),taskList);
        }
        else{
            fogTaskExecutionMap.get(task.getRequestFunctionId()).add(task);
        }

        if(count ==Constants.WINDOW_SIZE){
            fogTaskExecutionMap.get(task.getRequestFunctionId()).remove(0);
        }



    }

}
