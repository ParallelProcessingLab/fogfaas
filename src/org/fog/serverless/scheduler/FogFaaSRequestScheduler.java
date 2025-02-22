package org.fog.serverless.scheduler;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletSchedulerDynamicWorkload;
import org.cloudbus.cloudsim.core.CloudSim;
import org.fog.serverless.faas.FogFaaSContainer;
import org.fog.serverless.faas.FogFaaSInvoker;
import org.fog.serverless.faas.FogFaaSRequest;
import org.fog.serverless.utils.Constants;

/**
 * Request Scheduler Class for FoGFaaS extension.
 *
 * @author Ghaseminya
 * Created on 12/16/2024
 */

public class FogFaaSRequestScheduler extends ContainerCloudletSchedulerDynamicWorkload {
    int x=0;
    private double longestRunTimeContainer = 0;
    private double containerQueueTime = 0;
    protected int currentCpus=0;
    protected int usedPes=0;
    /** The total current mips requested from each pe by all requests allocated to this container. */
    private double totalCurrentRequestedMipsShareForRequests ;
    /** The total current mips allocated to all requests running in this container from each pe. */
    private double totalCurrentAllocatedMipsShareForRequests ;

    /** The total current ram requested by all requests allocated to this container. */
    private double totalCurrentRequestedRamForRequests ;
    /** The total current ram allocated to all requests running in this container. */
    private double totalCurrentAllocatedRamForRequests;

    public FogFaaSRequestScheduler(double mips, int numberOfPes) {
        super(mips,numberOfPes);

    }
    public boolean setTotalCurrentRequestedMipsShareForRequests(FogFaaSRequest cl) {
        if ((cl.getNumberOfPes() <= getNumberOfPes()) && (cl.getUtilizationOfCpu() <= 1 - totalCurrentRequestedMipsShareForRequests)){
            totalCurrentRequestedMipsShareForRequests += cl.getUtilizationOfCpu();
            return true;
        }
        else {
            return false;
        }
    }
    public void setTotalCurrentAllocatedMipsShareForRequests(FogFaaSRequest cl) {
     totalCurrentAllocatedMipsShareForRequests += cl.getUtilizationOfCpu();
    }
    public boolean setTotalCurrentRequestedRamForRequests(FogFaaSRequest cl, FogFaaSContainer cont) {
        double currentRequested = totalCurrentRequestedRamForRequests;
        if (cl.getContainerMemory()*cl.getUtilizationOfRam() <= cont.getRam() - currentRequested){
            totalCurrentRequestedRamForRequests += cl.getContainerMemory()*cl.getUtilizationOfRam();
            return true;
        }
        else{
            return false;
        }

    }

    public void setTotalCurrentAllocatedRamForRequests(FogFaaSRequest cl) {
        totalCurrentAllocatedRamForRequests += cl.getContainerMemory()*cl.getUtilizationOfRam();
    }
    public boolean isSuitableForRequest(FogFaaSRequest cl, FogFaaSContainer cont) {
        Log.printLine(String.format("Current allocated ram of cont #%s is #%s and requested ram of cl #%s is %s", cont.getId(), totalCurrentAllocatedRamForRequests, cl.getCloudletId(), cl.getContainerMemory()*cl.getUtilizationOfRam()));
        Log.printLine(String.format("Current allocated mips of cont #%s is #%s and requested mips of cl #%s is %s", cont.getId(), totalCurrentAllocatedMipsShareForRequests, cl.getCloudletId(), cl.getUtilizationOfCpu()));
        return (cl.getContainerMemory() * cl.getUtilizationOfRam() <= (cont.getRam() - totalCurrentAllocatedRamForRequests)) && (cl.getNumberOfPes() <= getNumberOfPes()) && (cl.getUtilizationOfCpu() <= 1 - totalCurrentAllocatedMipsShareForRequests);
    }

    public void deAllocateResources(FogFaaSRequest cl){
        totalCurrentAllocatedRamForRequests -= cl.getContainerMemory()*cl.getUtilizationOfRam();
        totalCurrentAllocatedMipsShareForRequests -= cl.getUtilizationOfCpu();

    }
    public double getTotalCurrentRequestedMipsShareForRequests() {
        return totalCurrentRequestedMipsShareForRequests;
    }
    public double getTotalCurrentAllocatedMipsShareForRequests() {
        return totalCurrentAllocatedMipsShareForRequests;
    }
    public double getTotalCurrentAllocatedRamForRequests() {
        return totalCurrentAllocatedRamForRequests;
    }
    @Override
//    allocated mips to be calculated using no of pes allocated for the request (not to the entire container) * utilization % of request
    public double getEstimatedFinishTime(ResCloudlet rcl, double time) {
        FogFaaSRequest cl =(FogFaaSRequest)(rcl.getCloudlet());
        return time
                + ((rcl.getRemainingCloudletLength()) / (cl.getNumberOfPes()*this.getMips()*cl.getUtilizationOfCpu()));
    }

    public double getLongestRunTime() {
        return longestRunTimeContainer;
    }

    public double getContainerQueueTime() {
        return containerQueueTime;
    }

//    Is called each time a request is finally submitted to DC
    public double requestSubmit(FogFaaSRequest cl, FogFaaSInvoker vm, FogFaaSContainer cont) {
        if (!Constants.CONTAINER_CONCURRENCY || Constants.SCALE_PER_REQUEST){
        setTotalCurrentAllocatedMipsShareForRequests(cl);
        setTotalCurrentAllocatedRamForRequests(cl);
        }
        ResCloudlet rcl = new ResCloudlet(cl);
        rcl.setCloudletStatus(Cloudlet.INEXEC);
        vm.getRunningRequestList(). add((FogFaaSRequest) cl);
        rcl.setCloudletStatus(Cloudlet.INEXEC);
        vm.getRunningRequestList(). add((FogFaaSRequest) cl);
        for (int i = 0; i < cl.getNumberOfPes(); i++) {
            rcl.setMachineAndPeId(0, i);
        }
        getCloudletExecList().add(rcl);
        //System.out.println("request "+cl.getrequestId()+" is added to exec list of container "+ ((FogFaaSRequest) cl).getContainerId());;
        usedPes += cl.getNumberOfPes();
        vm.addToFogTaskExecutionMap((FogFaaSRequest)cl,vm);
        return getEstimatedFinishTime(rcl, getPreviousTime());
    }

    public double updateContainerProcessing(double currentTime, List<Double> mipsShare, FogFaaSInvoker vm) {
        setCurrentMipsShare(mipsShare);
        int cpus=0;
        longestRunTimeContainer=0;
        containerQueueTime = 0;

        for (Double mips : mipsShare) { // count the CPUs available to the VMM
            if (mips > 0) {
                cpus++;
            }
        }

        currentCpus = cpus;
        double timeSpan = currentTime - getPreviousTime();
        double nextEvent = Double.MAX_VALUE;
        List<ResCloudlet> requestsToFinish = new ArrayList<>();

        for (ResCloudlet rcl : getCloudletExecList()) {

            rcl.updateCloudletFinishedSoFar((long) (timeSpan
                    * rcl.getCloudlet().getNumberOfPes()*((FogFaaSRequest)(rcl.getCloudlet())).getUtilizationOfCpu()*((FogFaaSRequest)(rcl.getCloudlet())).getContainerMIPS()*Consts.MILLION));

        }
        if (getCloudletExecList().size() == 0 && getCloudletWaitingList().size() == 0) {

            setPreviousTime(currentTime);
            return 0.0;
        }

        int finished = 0;
        int pesFreed = 0;
        for (ResCloudlet rcl : getCloudletExecList()) {
            // finished anyway, rounding issue...
            if (rcl.getRemainingCloudletLength() == 0) { // finished: remove from the list
                requestsToFinish.add(rcl);
                finished++;
                pesFreed+=rcl.getNumberOfPes();
            }
        }
        usedPes -=pesFreed;

        for (ResCloudlet rgl : requestsToFinish) {
            getCloudletExecList().remove(rgl);
            cloudletFinish(rgl);
        }

        List<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
        if (!getCloudletWaitingList().isEmpty()) {
            for (int i = 0; i < finished; i++) {
                toRemove.clear();
                for (ResCloudlet rcl : getCloudletWaitingList()) {
                    if ((currentCpus - usedPes) >= rcl.getNumberOfPes()) {
//                        if(rcl.getCloudlet().getCloudletId()==815){
//                            System.out.println(CloudSim.clock()+" request #815 running: Debug");
//                        }
                        rcl.setCloudletStatus(Cloudlet.INEXEC);
                        //vm.getRunningrequestStack().push((FogFaaSRequest) rcl.getrequest());
                        boolean added = false;
                        for(int x=0; x< vm.getRunningRequestList().size(); x++){
                            if((((FogFaaSRequest) rcl.getCloudlet()).getArrivalTime()+((FogFaaSRequest) rcl.getCloudlet()).getMaxExecTime()<=vm.getRunningRequestList().get(x).getArrivalTime()+vm.getRunningRequestList().get(x).getMaxExecTime())){
                                vm.getRunningRequestList().add(x,((FogFaaSRequest) rcl.getCloudlet()));
                                added = true;
                                break;
                            }
                        }
                        if(added == false){
                            vm.getRunningRequestList(). add((FogFaaSRequest) rcl.getCloudlet());
                        }
                        for (int k = 0; k < rcl.getNumberOfPes(); k++) {
                            rcl.setMachineAndPeId(0, i);
                        }
                        getCloudletExecList().add(rcl);

                        /** To enable average latency of application */
                        vm.addToFogTaskExecutionMap((FogFaaSRequest)rcl.getCloudlet(),vm);
                        usedPes += rcl.getNumberOfPes();
                        toRemove.add(rcl);
                        break;
                    }
                }
                getCloudletWaitingList().removeAll(toRemove);
            }
        }


        for (ResCloudlet rcl : getCloudletExecList()) {
            double estimatedFinishTime = getEstimatedFinishTime(rcl, currentTime);
            if (estimatedFinishTime < nextEvent) {
                nextEvent = estimatedFinishTime;
            }

            FogFaaSRequest task = (FogFaaSRequest)(rcl.getCloudlet());
            /** Record the longest remaining execution time of the container*/
            containerQueueTime += task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock();
            if (task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock()> longestRunTimeContainer) {
                longestRunTimeContainer = task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock();
            }
        }

        for (ResCloudlet rcl : getCloudletWaitingList()) {
            FogFaaSRequest task = (FogFaaSRequest)(rcl.getCloudlet());
            containerQueueTime += task.getMaxExecTime();
            /** Record the longest remaining execution time of the container*/
            if (task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock()> longestRunTimeContainer) {
                longestRunTimeContainer = task.getMaxExecTime()+ task.getArrivalTime()-CloudSim.clock();
            }

        }




        setPreviousTime(currentTime);


        requestsToFinish.clear();

        return nextEvent;
    }

    @Override
    public Cloudlet cloudletCancel(int requestId) {
        boolean found = false;
        int position = 0;

        // First, looks in the finished queue
        for (ResCloudlet rcl : getCloudletFinishedList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            return getCloudletFinishedList().remove(position).getCloudlet();
        }

        // Then searches in the exec list
        position = 0;
        found = false;
        for (ResCloudlet rcl : getCloudletExecList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            ResCloudlet rcl = getCloudletExecList().remove(position);
            if (rcl.getRemainingCloudletLength() == 0) {
                cloudletFinish(rcl);
            } else {
                rcl.setCloudletStatus(Cloudlet.CANCELED);
            }
            return rcl.getCloudlet();
        }


        // Then searches in the waiting list
        position = 0;
        found = false;
        for (ResCloudlet rcl : getCloudletWaitingList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                break;
            }
            position++;
        }

        if (found) {
            ResCloudlet rcl = getCloudletWaitingList().remove(position);
            if (rcl.getRemainingCloudletLength() == 0) {
                cloudletFinish(rcl);
            } else {
                rcl.setCloudletStatus(Cloudlet.CANCELED);
            }
            return rcl.getCloudlet();
        }

        // Now, looks in the paused queue
        found = false;
        position = 0;
        for (ResCloudlet rcl : getCloudletPausedList()) {
            if (rcl.getCloudletId() == requestId) {
                found = true;
                rcl.setCloudletStatus(Cloudlet.CANCELED);
                break;
            }
            position++;
        }

        if (found) {
            return getCloudletPausedList().remove(position).getCloudlet();
        }

        return null;
    }
}



