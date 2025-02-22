package org.fog.serverless.faas;

import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 * Request Class for FoGFaaS extension.
 *
 * @author Ghaseminya
 * Created on 12/16/2024
 */
public class FogFaaSRequest extends ContainerCloudlet {

    private String requestType = null;
    private String requestFunctionId = null;
    private int containerMemory = 0;
    private long containerMIPS = 0;
    private double cpuShareReq = 0;
    private double memShareReq = 0;
    private double maxExecTime = 0;
    private double arrivalTime = 0;
    public boolean reschedule = false;
    public boolean success = false;
    public int retry = 0;
    private int priority = 0;
    private FogFaaSUtilizationModelPartial fogfaaSUtilizationModelPartialCpu;
    private FogFaaSUtilizationModelPartial fogFaaSUtilizationModelPartialRam;

    public FogFaaSRequest(int requestId, double arrivalTime, String requestFunctionId, long requestLength, int pesNumber,  int containerMemory, long containerMIPS,  double cpuShareReq, double memShareReq, long requestFileSize, long requestOutputSize, FogFaaSUtilizationModelPartial utilizationModelCpu, FogFaaSUtilizationModelPartial utilizationModelRam, UtilizationModel utilizationModelBw, int retry, boolean success) {
        super(requestId, requestLength, pesNumber, requestFileSize, requestOutputSize, utilizationModelCpu, utilizationModelRam, utilizationModelBw);

//        setRequestType(requestType);
        setRequestFunctionId(requestFunctionId);
        setContainerMemory(containerMemory);
        setContainerMIPS(containerMIPS);
//        setMaxExecTime(maxExecTime);
        setArrivalTime(arrivalTime);
        setPriority(priority);
//        setReschedule(reschedule);
        setSuccess(success);
        setRetry(retry);
        setUtilizationModelCpu(utilizationModelCpu);
        setUtilizationModelRam(utilizationModelCpu);
        setCpuShareRequest(cpuShareReq);
        setMemShareRequest(memShareReq);
    }

    public void setRequestType(String requestType){this.requestType = requestType;}
    public void setPriority(int priority){this.priority = priority;}
    public void setRequestFunctionId(String requestFunctionId){this.requestFunctionId = requestFunctionId;}
    public void setContainerMemory(int containerMemory){this.containerMemory = containerMemory;}
    public void setContainerMIPS(long containerMIPS){this.containerMIPS = containerMIPS;}
    public void setCpuShareRequest(double cpuShareReq){this.cpuShareReq = cpuShareReq;}
    public void setMemShareRequest(double memShareReq){this.memShareReq = memShareReq;}
    public void setMaxExecTime(double maxExecTime){this.maxExecTime = maxExecTime;}
    public void setArrivalTime(double arrivalTime){this.arrivalTime = arrivalTime;}
//    public void setReschedule(boolean reschedule){this.reschedule = reschedule;}
    public void setSuccess(boolean success){this.success = success;}
    public void setRetry(int retry){this.retry = retry;}

    public String getRequestType() {return requestType;}
    public String getRequestFunctionId() {return requestFunctionId;}
    public int getContainerMemory() {return containerMemory;}
    public long getContainerMIPS() {return containerMIPS;}
    public double getCpuShareRequest() {return cpuShareReq;}
    public double getMemShareRequest() {return memShareReq;}
    public double getMaxExecTime() {return maxExecTime;}
    public int getPriority() {return priority;}
    public double getArrivalTime() {return arrivalTime;}
//    public boolean getReschedule() {return reschedule;}
    public boolean getSuccess() {return success;}
    public int getRetry() {return retry;}


    public void setResourceParameter(final int resourceID, final double cost, int vmId) {
        final Resource res = new Resource();
        res.vmId = vmId;
        res.resourceId = resourceID;
        res.costPerSec = cost;
        res.resourceName = CloudSim.getEntityName(resourceID);

        // add into a list if moving to a new grid resource
        resList.add(res);

        if (index == -1 && record) {
            write("Allocates this request to " + res.resourceName + " (ID #" + resourceID
                    + ") with cost = $" + cost + "/sec");
        } else if (record) {
            final int id = resList.get(index).resourceId;
            final String name = resList.get(index).resourceName;
            write("Moves request from " + name + " (ID #" + id + ") to " + res.resourceName + " (ID #"
                    + resourceID + ") with cost = $" + cost + "/sec");
        }

        index++;  // initially, index = -1
    }


    public void setResourceParameter(final int resourceID, final double costPerCPU, final double costPerBw, int vmId) {
        setResourceParameter(resourceID, costPerCPU, vmId);
        this.costPerBw = costPerBw;
        accumulatedBwCost = costPerBw * getCloudletFileSize();
    }

    public String getResList() {
        String resString = "";
        for(int x=0; x<resList.size(); x++){
            if(x==resList.size()-1){
                resString = resString.concat(Integer.toString(resList.get(x).vmId)) ;
            }
            else
                resString = resString.concat(Integer.toString(resList.get(x).vmId)+" ,") ;

        }
        return resString;
    }

    public double getUtilizationOfCpu() {
        return getUtilizationModelCpu().getCpuUtilization(this);
    }

    public double getUtilizationOfRam() {
        return getUtilizationModelRam().getMemUtilization(this);
    }
    public void setUtilizationModelCpu(final FogFaaSUtilizationModelPartial utilizationModelCpu) {
        this.fogfaaSUtilizationModelPartialCpu = utilizationModelCpu;
    }

    public void setUtilizationModelRam(final FogFaaSUtilizationModelPartial utilizationModelRam) {
        this.fogFaaSUtilizationModelPartialRam = utilizationModelRam;
    }

    public FogFaaSUtilizationModelPartial getUtilizationModelCpu() {
        return fogfaaSUtilizationModelPartialCpu;
    }
    public FogFaaSUtilizationModelPartial getUtilizationModelRam() {
        return fogFaaSUtilizationModelPartialRam;
    }

}


