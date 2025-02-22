package org.fog.serverless.example;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

import com.opencsv.CSVWriter;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPeProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.CotainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisionerSimple;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicy;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicyFirstFit;
import org.cloudbus.cloudsim.container.resourceAllocatorMigrationEnabled.PCVmAllocationPolicyMigrationAbstractHostSelection;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicy;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicyMaximumUsage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.application.selectivity.FractionalSelectivity;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.serverless.application.FogFaaSAppEdge;
import org.fog.serverless.application.FogFaaSAppLoop;
import org.fog.serverless.application.FogFaaSApplication;
import org.fog.serverless.entities.FogFaaSActuator;
import org.fog.serverless.entities.FogFaaSBroker;
import org.fog.serverless.entities.FogFaaSDevice;
import org.fog.serverless.entities.FogFaaSSensor;
import org.fog.serverless.entities.FogFaaSTuple;
import org.fog.serverless.faas.FogFaaSContainer;
import org.fog.serverless.faas.FogFaaSInvoker;
import org.fog.serverless.faas.FogFaaSRequest;
import org.fog.serverless.faas.FogFaaSUtilizationModelPartial;
import org.fog.serverless.placement.FogFaaSContainerRamProvisioner;
import org.fog.serverless.placement.FogFaaSController;
import org.fog.serverless.placement.FogFaaSModuleMapping;
import org.fog.serverless.placement.FogFaaSModulePlacementMapping;
import org.fog.serverless.placement.FogFaaSModulePlacementMobileEdgewards;
import org.fog.serverless.scheduler.FogFaaSContainerScheduler;
import org.fog.serverless.utils.Constants;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.distribution.DeterministicDistribution;

/**
 * Simulation setup for FogFaaS Function execution
 * Case Study 1:
 * This test covers featured such as,
 * 1. Request load balancing
 * 2. Function scheduling
 *
 * @author Anupama Mampage
 */

/**
 * Config properties
 * containerConcurrency -> false
 * functionAutoScaling -> false
 * functionHorizontalAutoscaling -> false
 * funtionVerticalAutoscaling -> false
 * scalePerRequest -> true/false
 * containerIdlingEnabled -> false/true
 *
 *
 *  Case Study 2:
 *  This test covers featured such as,
 *  1. Function vertical auto-scaling
 *  2. Function horizontal auto-scaling
 *  3. Container concurrency
 *
 *  Config properties
 *  containerConcurrency -> true
 *  functionAutoScaling -> true
 *  functionHorizontalAutoscaling -> false/true
 *  funtionVerticalAutoscaling -> false/true
 *  scalePerRequest -> false
 *  containerIdlingEnabled -> false
 *
 */

public class DCNS_FogFaaS {


//    private static List<FogFaaSInvoker> fogList;
    private static int controllerId;
    private static FogFaaSBroker broker;

	static int numOfAreas = 1;
	static int numOfCamerasPerArea = 4;
	static List<FogFaaSDevice> fogDevices = new ArrayList<FogFaaSDevice>();
	static List<FogFaaSSensor> sensors = new ArrayList<FogFaaSSensor>();
	static List<FogFaaSActuator> actuators = new ArrayList<FogFaaSActuator>();
	static FogFaaSController controller = null;
    public static void main(String[] args) {
        Log.printLine("Starting FogFaaS Example1...");
        try {
            int num_user = 1;   // number of cloud users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false,CLOUD=false;  // mean trace events
            CloudSim.init(num_user, calendar, trace_flag);
            String appId = "fogfaasapp1";
            broker = createBroker();
            controllerId = broker.getId();
            FogFaaSModuleMapping moduleMapping = FogFaaSModuleMapping.createModuleMapping();
            for(FogFaaSDevice device : fogDevices){
				if(device.getName().startsWith("m")){ // names of all Smart Cameras start with 'm' 
					moduleMapping.addModuleToDevice("motion_detector", device.getName());  // fixing 1 instance of the Motion Detector module to each Smart Camera
				}
			}
			moduleMapping.addModuleToDevice("user_interface", "cloud"); // fixing instances of User Interface module in the Cloud
			if(CLOUD){
				// if the mode of deployment is cloud-based
				moduleMapping.addModuleToDevice("object_detector", "cloud"); // placing all instances of Object Detector module in the Cloud
				moduleMapping.addModuleToDevice("object_tracker", "cloud"); // placing all instances of Object Tracker module in the Cloud
			}
			FogFaaSApplication application = createApplication(appId, broker.getId());
            System.out.println("asdasdasd");
//            fogList = createFogList(controllerId);
            createFogDevices(broker.getId(), appId);
            
            controller = new FogFaaSController("master-controller", fogDevices, sensors, 
					actuators);
//            controller.submitFogList(fogList);
			controller.submitApplication(application, 
					(CLOUD)?(new FogFaaSModulePlacementMapping(fogDevices, application, moduleMapping))
							:(new FogFaaSModulePlacementMobileEdgewards(fogDevices, sensors, actuators, application, moduleMapping)));
			createRequests();
//            DC = createDatacenter("datacenter");
            
            
//            controller.setFogFaaSDatacenter(DC);
            
            CloudSim.terminateSimulation(2500.00);
            CloudSim.startSimulation();
            CloudSim.stopSimulation();
//            List<ContainerCloudlet> finishedRequests = controller.getCloudletReceivedList();
//            List<FogFaaSContainer> destroyedContainers = controller.getContainersDestroyedList();
//            printRequestList(finishedRequests);
//            printContainerList(destroyedContainers);
//            if (Constants.MONITORING){
//                printVmUpDownTime();
//                printVmUtilization();
//            }
//            writeDataLineByLine(finishedRequests);
            Log.printLine("ContainerCloudSimExample1 finished!");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    private static void createRequests() throws IOException {
        long fileSize = 300L;
        long outputSize = 300L;
        int createdRequests = 0;
        BufferedReader br = new BufferedReader(new FileReader(Constants.FUNCTION_REQUESTS_FILENAME));
        String line = null;
        String cvsSplitBy = ",";
        controller.noOfTasks++;

        FogFaaSUtilizationModelPartial utilizationModelPar = new FogFaaSUtilizationModelPartial();
        UtilizationModelFull utilizationModel = new UtilizationModelFull();

        while ((line = br.readLine()) != null) {
            String[] data = line.split(cvsSplitBy);
            FogFaaSRequest request = null;

            try {
                request = new FogFaaSRequest(IDs.pollId(FogFaaSRequest.class), Double.parseDouble(String.valueOf(data[0])),
                		String.valueOf(data[1]), Long.parseLong(data[2]), Integer.parseInt(data[3]), 
                		Integer.parseInt(data[4]), Long.parseLong(data[5]), Double.parseDouble(data[6]),
                		Double.parseDouble(data[7]),
                        fileSize, outputSize, utilizationModelPar, utilizationModelPar, utilizationModel,  0, true);
                System.out.println("request No " + request.getCloudletId());
            } catch (Exception e) {
            	e.printStackTrace();
            }
            request.setUserId(controller.getId());
            System.out.println(CloudSim.clock() + " request created. This request arrival time is :" + Double.parseDouble(data[0]));
            controller.requestArrivalTime.add(Double.parseDouble(data[0]) + Constants.FUNCTION_SCHEDULING_DELAY);
            controller.requestQueue.add(request);
            createdRequests += 1;

        }
    }
private static FogFaaSApplication createApplication(String appId, int userId){
		
	FogFaaSApplication application =FogFaaSApplication.createApplication(appId, userId);
		/*
		 * Adding modules (vertices) to the application model (directed graph)
		 */
		application.addAppModule("object_detector", 10);
		application.addAppModule("motion_detector", 10);
		application.addAppModule("object_tracker", 10);
		application.addAppModule("user_interface", 10);
		
		/*
		 * Connecting the application modules (vertices) in the application model (directed graph) with edges
		 */
		application.addAppEdge("CAMERA", "motion_detector", 1000, 20000, "CAMERA", FogFaaSTuple.UP, FogFaaSAppEdge.SENSOR); // adding edge from CAMERA (sensor) to Motion Detector module carrying tuples of type CAMERA
		application.addAppEdge("motion_detector", "object_detector", 2000, 2000, "MOTION_VIDEO_STREAM", FogFaaSTuple.UP, FogFaaSAppEdge.MODULE); // adding edge from Motion Detector to Object Detector module carrying tuples of type MOTION_VIDEO_STREAM
		application.addAppEdge("object_detector", "user_interface", 500, 2000, "DETECTED_OBJECT", FogFaaSTuple.UP, FogFaaSAppEdge.MODULE); // adding edge from Object Detector to User Interface module carrying tuples of type DETECTED_OBJECT
		application.addAppEdge("object_detector", "object_tracker", 1000, 100, "OBJECT_LOCATION", FogFaaSTuple.UP, FogFaaSAppEdge.MODULE); // adding edge from Object Detector to Object Tracker module carrying tuples of type OBJECT_LOCATION
		application.addAppEdge("object_tracker", "PTZ_CONTROL", 100, 28, 100, "PTZ_PARAMS", FogFaaSTuple.DOWN, FogFaaSAppEdge.ACTUATOR); // adding edge from Object Tracker to PTZ CONTROL (actuator) carrying tuples of type PTZ_PARAMS
		
		/*
		 * Defining the input-output relationships (represented by selectivity) of the application modules. 
		 */
		application.addTupleMapping("motion_detector", "CAMERA", "MOTION_VIDEO_STREAM", new FractionalSelectivity(1.0)); // 1.0 tuples of type MOTION_VIDEO_STREAM are emitted by Motion Detector module per incoming tuple of type CAMERA
		application.addTupleMapping("object_detector", "MOTION_VIDEO_STREAM", "OBJECT_LOCATION", new FractionalSelectivity(1.0)); // 1.0 tuples of type OBJECT_LOCATION are emitted by Object Detector module per incoming tuple of type MOTION_VIDEO_STREAM
		application.addTupleMapping("object_detector", "MOTION_VIDEO_STREAM", "DETECTED_OBJECT", new FractionalSelectivity(0.05)); // 0.05 tuples of type MOTION_VIDEO_STREAM are emitted by Object Detector module per incoming tuple of type MOTION_VIDEO_STREAM
	
		/*
		 * Defining application loops (maybe incomplete loops) to monitor the latency of. 
		 * Here, we add two loops for monitoring : Motion Detector -> Object Detector -> Object Tracker and Object Tracker -> PTZ Control
		 */
		final FogFaaSAppLoop loop1 = new FogFaaSAppLoop(new ArrayList<String>(){{add("motion_detector");add("object_detector");add("object_tracker");}});
		final FogFaaSAppLoop loop2 = new FogFaaSAppLoop(new ArrayList<String>(){{add("object_tracker");add("PTZ_CONTROL");}});
		List<FogFaaSAppLoop> loops = new ArrayList<FogFaaSAppLoop>(){{add(loop1);add(loop2);}};
		
		application.setLoops(loops);
		System.out.println(application+"Application");
		return application;
	}


private static void createFogDevices(int userId, String appId) {
	FogFaaSDevice cloud = createFogDevice("cloud", 44800, 40000, 100, 10000, 0, 0.01, 16*103, 16*83.25);
	cloud.setParentId(-1);
	fogDevices.add(cloud);
	FogFaaSDevice proxy = createFogDevice("proxy-server", 2800, 4000, 10000, 10000, 1, 0.0, 107.339, 83.4333);
	proxy.setParentId(cloud.getId());
	proxy.setUplinkLatency(100); // latency of connection between proxy server and cloud is 100 ms
	fogDevices.add(proxy);
	for(int i=0;i<numOfAreas;i++){
		addArea(i+"", userId, appId, proxy.getId());
	}
}

private static FogFaaSDevice addArea(String id, int userId, String appId, int parentId){
	FogFaaSDevice router = createFogDevice("d-"+id, 2800, 4000, 10000, 10000, 1, 0.0, 107.339, 83.4333);
	fogDevices.add(router);
	router.setUplinkLatency(2); // latency of connection between router and proxy server is 2 ms
	for(int i=0;i<numOfCamerasPerArea;i++){
		String mobileId = id+"-"+i;
		FogFaaSDevice camera = addCamera(mobileId, userId, appId, router.getId()); // adding a smart camera to the physical topology. Smart cameras have been modeled as fog devices as well.
		camera.setUplinkLatency(2); // latency of connection between camera and router is 2 ms
		fogDevices.add(camera);
	}
	router.setParentId(parentId);
	return router;
}
	
private static FogFaaSDevice addCamera(String id, int userId, String appId, int parentId){
	FogFaaSDevice camera = createFogDevice("m-"+id, 500, 1000, 10000, 10000, 3, 0, 87.53, 82.44);
	camera.setParentId(parentId);
	FogFaaSSensor sensor = new FogFaaSSensor("s-"+id, "CAMERA", userId, appId, new DeterministicDistribution(5)); // inter-transmission time of camera (sensor) follows a deterministic distribution
	sensors.add(sensor);
	FogFaaSActuator ptz = new FogFaaSActuator("ptz-"+id, userId, appId, "PTZ_CONTROL");//Pan-Tilt-Zoom
	actuators.add(ptz);
	sensor.setGatewayDeviceId(camera.getId());
	sensor.setLatency(1.0);  // latency of connection between camera (sensor) and the parent Smart Camera is 1 ms
	ptz.setGatewayDeviceId(camera.getId());
	ptz.setLatency(1.0);  // latency of connection between PTZ Control and the parent Smart Camera is 1 ms
	return camera;
}

/**
 * Creates a vanilla fog device
 * @param nodeName name of the device to be used in simulation
 * @param mips MIPS
 * @param ram RAM
 * @param upBw uplink bandwidth
 * @param downBw downlink bandwidth
 * @param level hierarchy level of the device
 * @param ratePerMips cost rate per MIPS used
 * @param busyPower
 * @param idlePower
 * @return
 */
private static FogFaaSDevice createFogDevice(String nodeName, long mips,
		int ram, long upBw, long downBw, int level, double ratePerMips, double busyPower, double idlePower) {
	
	List<Pe> peList = new ArrayList<Pe>();

	// 3. Create PEs and add these into a list.
	peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating

	int hostId = FogUtils.generateEntityId();
	long storage = 1000000; // host storage
	int bw = 10000;

	PowerHost host = new PowerHost(
			hostId,
			new RamProvisionerSimple(ram),
			new BwProvisionerOverbooking(bw),
			storage,
			peList,
			new StreamOperatorScheduler(peList),
			new FogLinearPowerModel(busyPower, idlePower)
		);

	List<Host> hostList = new ArrayList<Host>();
	hostList.add(host);

	String arch = "x86"; // system architecture
	String os = "Linux"; // operating system
	String vmm = "Xen";
	double time_zone = 10.0; // time zone this resource located
	double cost = 3.0; // the cost of using processing in this resource
	double costPerMem = 0.05; // the cost of using memory in this resource
	double costPerStorage = 0.001; // the cost of using storage in this
									// resource
	double costPerBw = 0.0; // the cost of using bw in this resource
	LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
												// devices by now

	FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(
			arch, os, vmm, host, time_zone, cost, costPerMem,
			costPerStorage, costPerBw);

	FogFaaSDevice fogdevice = null;
	try {
		fogdevice = new FogFaaSDevice(nodeName, characteristics, 
				new AppModuleAllocationPolicy(hostList), storageList, 10, upBw, downBw, 0, ratePerMips);
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	fogdevice.setLevel(level);
	return fogdevice;
}	



    private static ArrayList<FogFaaSInvoker> createFogList(int brokerId) throws Exception {
        ArrayList<FogFaaSInvoker> containerFoGs = new ArrayList<FogFaaSInvoker>();
        for (int i = 0; i < Constants.NUMBER_FOG; ++i) {
        	Random rand = new Random();
        	int vmType = rand.nextInt(4);
        	List<Pe> pes = new ArrayList<Pe>();
        	pes.add(new Pe(0, new PeProvisionerOverbooking((double) Constants.FOG_MIPS[vmType])));
        	List<ContainerPe> peList = new ArrayList<ContainerPe>();
            
            
            for (int j = 0; j < Constants.FOG_PES[vmType]; ++j) {
//            	ContainerVmBwProvisionerSimple provisioner = new ContainerVmBwProvisionerSimple(11);
//                peList.add(new ContainerPe(j,provisioner));
            }
            
            
            
            int hostId = FogUtils.generateEntityId();
    		long storage = 1000000; // host storage
    		int bw = 10000;

    		PowerHost host = new PowerHost(
    				hostId,
    				new RamProvisionerSimple((int)Constants.FOG_RAM[vmType]),
    				new BwProvisionerOverbooking(bw),
    				storage,
    				pes,
    				new StreamOperatorScheduler(pes),
    				new FogLinearPowerModel(87.53, 82.44)
    			);

    		List<Host> hostList = new ArrayList<Host>();
    		hostList.add(host);

    		String arch = "x86"; // system architecture
    		String os = "Linux"; // operating system
    		String vmm = "Xen";
    		double time_zone = 10.0; // time zone this resource located
    		double cost = 3.0; // the cost of using processing in this resource
    		double costPerMem = 0.05; // the cost of using memory in this resource
    		double costPerStorage = 0.001; // the cost of using storage in this
    										// resource
    		double costPerBw = 0.0; // the cost of using bw in this resource
    		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
    													// devices by now

    		FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(
    				arch, os, vmm, host, time_zone, cost, costPerMem,
    				costPerStorage, costPerBw);

    		
            containerFoGs.add(
            		new FogFaaSInvoker(IDs.pollId(ContainerVm.class), brokerId,
                    (double) Constants.FOG_MIPS[vmType], (float) Constants.FOG_RAM[vmType],
                    Constants.FOG_BW, Constants.FOG_SIZE, "Xen",
                    new FogFaaSContainerScheduler(peList),
                    new FogFaaSContainerRamProvisioner(Constants.FOG_RAM[vmType]),
                    new ContainerBwProvisionerSimple(Constants.FOG_BW),
                    peList, Constants.SCHEDULING_INTERVAL,
            		characteristics, storageList, hostList,  0.0)
            		);
        }
        System.out.println("Fognumber"+containerFoGs.size());
        return containerFoGs;
    }
    private static FogFaaSBroker createBroker() {
    	FogFaaSBroker controller = null;
        int overBookingFactor = 80;
        try {
            controller = new FogFaaSBroker("Broker");
        } catch (Exception var2) {
            var2.printStackTrace();
            System.exit(0);
        }
        return controller;
    }
    
    private static void printContainerList(List<FogFaaSContainer> list) {
        int size = list.size();
        FogFaaSContainer container;
        int deadlineMetStat = 0;
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Container ID" + indent + "VM ID" + indent + "Start Time" + indent
                + "Finish Time" + indent + "Finished Requests List");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            container = list.get(i);
            Log.print(indent + container.getId() + indent + indent);
            Log.printLine(indent + (container.getVm()).getId() + indent + indent + (dft.format(container.getStartTime()))
                    + indent + indent + indent +indent +  dft.format(container.getFinishTime())
                    + indent + indent+ indent+ indent
                    +  container.getfinishedTasks());
        }
        Log.printLine("Deadline met no: "+deadlineMetStat);
    }
    private static void printRequestList(List<ContainerCloudlet> list) {
        int size = list.size();
        Cloudlet request;
        int deadlineMetStat = 0;
        int totalResponseTime = 0;
        int failedRequestRatio = 0;
        float averageResponseTime = 0;
        int totalRequests = 0;
        int failedRequests = 0;
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("request ID" + indent +"Function ID" + indent+"Container ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "Final VM ID" + indent + "Execution Time" + indent
                + "Start Time" + indent + "Finish Time"+ indent + "Response Time"+ indent + "Vm List");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            request = list.get(i);
            Log.print(indent + request.getCloudletId() + indent + indent);
            Log.print(indent + ((FogFaaSRequest)request).getRequestFunctionId() + indent + indent);
            Log.print(indent + ((FogFaaSRequest)request).getContainerId() + indent + indent);
            totalRequests += 1;
            if (request.getCloudletStatusString() == "Success") {
                totalResponseTime += request.getFinishTime()-((FogFaaSRequest)request).getArrivalTime();
                Log.print("SUCCESS");
                if (Math.ceil((request.getFinishTime() - ((FogFaaSRequest) request).getArrivalTime())) <= (Math.ceil(((FogFaaSRequest) request).getMaxExecTime())) || Math.floor((request.getFinishTime() - ((FogFaaSRequest) request).getArrivalTime())) <= (Math.ceil(((FogFaaSRequest) request).getMaxExecTime()))) {
                    deadlineMetStat++;
                }
            }
            else{
                Log.print("DROPPED");
                failedRequests += 1;
            }
            Log.printLine(indent + indent + request.getResourceId()
                    + indent + indent + indent +indent + request.getVmId()
                    + indent + indent+ indent+ indent
                    + dft.format(request.getActualCPUTime()) + indent+ indent
                    + indent + dft.format(request.getExecStartTime())
                    + indent + indent+ indent
                    + dft.format(request.getFinishTime())+ indent + indent + indent
                    + dft.format(request.getFinishTime()-((FogFaaSRequest)request).getArrivalTime())+ indent + indent + indent
                    + ((FogFaaSRequest)request).getResList());
        }
        Log.printLine("Deadline met no: "+deadlineMetStat);
    }
}
