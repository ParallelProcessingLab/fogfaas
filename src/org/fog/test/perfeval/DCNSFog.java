package org.fog.test.perfeval;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.Application;
import org.fog.application.selectivity.FractionalSelectivity;
import org.fog.entities.Actuator;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.Sensor;
import org.fog.entities.Tuple;
import org.fog.placement.Controller;
import org.fog.placement.ModuleMapping;
import org.fog.placement.ModulePlacementEdgewards;
import org.fog.placement.ModulePlacementMapping;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.TimeKeeper;
import org.fog.utils.distribution.DeterministicDistribution;

/**
 * Simulation setup for case study 2 - Intelligent Surveillance
 * @author Harshit Gupta
 *
 */

public class DCNSFog {
	static List<FogDevice> fogDevices = new ArrayList<FogDevice>();
	static List<Sensor> sensors = new ArrayList<Sensor>();
	static List<Actuator> actuators = new ArrayList<Actuator>();
	static int numOfAreas = 1;
	static int numOfCLIENTsPerArea = 4;
	
	private static boolean CLOUD = false;
	
	public static void main(String[] args) {

		Log.printLine("Starting DCNS...");//Data Center Networks

		try {
			Log.disable();
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			CloudSim.init(num_user, calendar, trace_flag);

			String appId = "dcns"; // identifier of the application
			
			FogBroker broker = new FogBroker("broker");
			
			Application application = createApplication(appId, broker.getId());
			application.setUserId(broker.getId());
			
			createFogDevices(broker.getId(), appId);
			
			Controller controller = null;
			
			ModuleMapping moduleMapping = ModuleMapping.createModuleMapping(); // initializing a module mapping
			for(FogDevice device : fogDevices){
				if(device.getName().startsWith("m")){ // names of all Smart CLIENTs start with 'm' 
					moduleMapping.addModuleToDevice("compute_unit", device.getName());  // fixing 1 instance of the Motion Detector module to each Smart CLIENT
				}
			}
			moduleMapping.addModuleToDevice("request_dispatcher", "cloud"); // fixing instances of User Interface module in the Cloud
			if(CLOUD){
				// if the mode of deployment is cloud-based
				moduleMapping.addModuleToDevice("request_handler", "cloud"); // placing all instances of Object Detector module in the Cloud
				moduleMapping.addModuleToDevice("load_balancer", "cloud"); // placing all instances of Object Tracker module in the Cloud
			}
			
			controller = new Controller("master-controller", fogDevices, sensors, 
					actuators);
			
			controller.submitApplication(application, 
					(CLOUD)?(new ModulePlacementMapping(fogDevices, application, moduleMapping))
							:(new ModulePlacementEdgewards(fogDevices, sensors, actuators, application, moduleMapping)));
			
			TimeKeeper.getInstance().setSimulationStartTime(Calendar.getInstance().getTimeInMillis());
			
			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			Log.printLine("VRGame finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}
	
	/**
	 * Creates the fog devices in the physical topology of the simulation.
	 * @param userId
	 * @param appId
	 */
	private static void createFogDevices(int userId, String appId) {
		FogDevice cloud = createFogDevice("cloud", 44800, 40000, 100, 10000, 0, 0.01, 16*103, 16*83.25);
		cloud.setParentId(-1);
		fogDevices.add(cloud);
		FogDevice proxy = createFogDevice("proxy-server", 2800, 4000, 10000, 10000, 1, 0.0, 107.339, 83.4333);
		proxy.setParentId(cloud.getId());
		proxy.setUplinkLatency(100); // latency of connection between proxy server and cloud is 100 ms
		fogDevices.add(proxy);
		for(int i=0;i<numOfAreas;i++){
			addArea(i+"", userId, appId, proxy.getId());
		}
	}

	private static FogDevice addArea(String id, int userId, String appId, int parentId){
		FogDevice router = createFogDevice("d-"+id, 2800, 4000, 10000, 10000, 1, 0.0, 107.339, 83.4333);
		fogDevices.add(router);
		router.setUplinkLatency(2); // latency of connection between router and proxy server is 2 ms
		for(int i=0;i<numOfCLIENTsPerArea;i++){
			String mobileId = id+"-"+i;
			FogDevice CLIENT = addCLIENT(mobileId, userId, appId, router.getId()); // adding a smart CLIENT to the physical topology. Smart CLIENTs have been modeled as fog devices as well.
			CLIENT.setUplinkLatency(2); // latency of connection between CLIENT and router is 2 ms
			fogDevices.add(CLIENT);
		}
		router.setParentId(parentId);
		return router;
	}
	
	private static FogDevice addCLIENT(String id, int userId, String appId, int parentId){
		FogDevice CLIENT = createFogDevice("m-"+id, 500, 1000, 10000, 10000, 3, 0, 87.53, 82.44);
		CLIENT.setParentId(parentId);
		Sensor sensor = new Sensor("s-"+id, "CLIENT", userId, appId, new DeterministicDistribution(5)); // inter-transmission time of CLIENT (sensor) follows a deterministic distribution
		sensors.add(sensor);
		Actuator ptz = new Actuator("ptz-"+id, userId, appId, "MYCONTROL");//Pan-Tilt-Zoom
		actuators.add(ptz);
		sensor.setGatewayDeviceId(CLIENT.getId());
		sensor.setLatency(1.0);  // latency of connection between CLIENT (sensor) and the parent Smart CLIENT is 1 ms
		ptz.setGatewayDeviceId(CLIENT.getId());
		ptz.setLatency(1.0);  // latency of connection between PTZ Control and the parent Smart CLIENT is 1 ms
		return CLIENT;
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
	private static FogDevice createFogDevice(String nodeName, long mips,
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

		FogDevice fogdevice = null;
		try {
			fogdevice = new FogDevice(nodeName, characteristics, 
					new AppModuleAllocationPolicy(hostList), storageList, 10, upBw, downBw, 0, ratePerMips);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		fogdevice.setLevel(level);
		return fogdevice;
	}

	/**
	 * Function to create the Intelligent Surveillance application in the DDF model. 
	 * @param appId unique identifier of the application
	 * @param userId identifier of the user of the application
	 * @return
	 */

	private static Application createApplication(String appId, int userId){
		
		Application application = Application.createApplication(appId, userId);
		/*
		 * Adding modules (vertices) to the application model (directed graph)
		 */
		application.addAppModule("request_handler", 10);
		application.addAppModule("compute_unit", 10);
		application.addAppModule("load_balancer", 10);
		application.addAppModule("request_dispatcher", 10);
		
		/*
		 * Connecting the application modules (vertices) in the application model (directed graph) with edges
		 */
		application.addAppEdge("CLIENT", "compute_unit", 1000, 20000, "CLIENT", Tuple.UP, AppEdge.SENSOR); // adding edge from CLIENT (sensor) to Motion Detector module carrying tuples of type CLIENT
		application.addAppEdge("compute_unit", "request_handler", 2000, 2000, "NEW_REQUEST", Tuple.UP, AppEdge.MODULE); // adding edge from Motion Detector to Object Detector module carrying tuples of type NEW_REQUEST
		application.addAppEdge("request_handler", "request_dispatcher", 500, 2000, "REQUEST_ASSIGN", Tuple.UP, AppEdge.MODULE); // adding edge from Object Detector to User Interface module carrying tuples of type REQUEST_ASSIGN
		application.addAppEdge("request_handler", "load_balancer", 1000, 100, "REQUEST_PROCESSED", Tuple.UP, AppEdge.MODULE); // adding edge from Object Detector to Object Tracker module carrying tuples of type REQUEST_PROCESSED
		application.addAppEdge("load_balancer", "MYCONTROL", 100, 28, 100, "CLIENT2", Tuple.DOWN, AppEdge.ACTUATOR); // adding edge from Object Tracker to PTZ CONTROL (actuator) carrying tuples of type CLIENT2
		
		/*
		 * Defining the input-output relationships (represented by selectivity) of the application modules. 
		 */
		application.addTupleMapping("compute_unit", "CLIENT", "NEW_REQUEST", new FractionalSelectivity(1.0)); // 1.0 tuples of type NEW_REQUEST are emitted by Motion Detector module per incoming tuple of type CLIENT
		application.addTupleMapping("request_handler", "NEW_REQUEST", "REQUEST_PROCESSED", new FractionalSelectivity(1.0)); // 1.0 tuples of type REQUEST_PROCESSED are emitted by Object Detector module per incoming tuple of type NEW_REQUEST
		application.addTupleMapping("request_handler", "NEW_REQUEST", "REQUEST_ASSIGN", new FractionalSelectivity(0.05)); // 0.05 tuples of type NEW_REQUEST are emitted by Object Detector module per incoming tuple of type NEW_REQUEST
	
		/*
		 * Defining application loops (maybe incomplete loops) to monitor the latency of. 
		 * Here, we add two loops for monitoring : Motion Detector -> Object Detector -> Object Tracker and Object Tracker -> PTZ Control
		 */
		final AppLoop loop1 = new AppLoop(new ArrayList<String>(){{
			add("compute_unit");
			add("request_handler");
			add("load_balancer");}});
		final AppLoop loop2 = new AppLoop(new ArrayList<String>(){{
			add("load_balancer");
			add("MYCONTROL");}});
		List<AppLoop> loops = new ArrayList<AppLoop>(){{
			add(loop1);
			add(loop2);}};
		
		application.setLoops(loops);
		return application;
	}
}