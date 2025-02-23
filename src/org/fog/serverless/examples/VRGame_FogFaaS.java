package org.fog.serverless.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;

import org.fog.application.selectivity.FractionalSelectivity;
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
import org.fog.serverless.faas.FogFaaSRequest;
import org.fog.serverless.faas.FogFaaSUtilizationModelPartial;
import org.fog.serverless.placement.FogFaaSController;
import org.fog.serverless.placement.FogFaaSModuleMapping;
import org.fog.serverless.placement.FogFaaSModulePlacementEdgewards;
import org.fog.serverless.placement.FogFaaSModulePlacementMapping;
import org.fog.serverless.utils.Constants;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.TimeKeeper;
import org.fog.utils.distribution.DeterministicDistribution;

/**
 * Implementation class for simulating the EEG Beam Tractor Game within the
 * FogFaaS environment.
 * 
 * This class serves as a practical example of how to use the FogFaaS simulator
 * for optimization problems based on EEG feedback. In this implementation, the
 * goal is to guide a tractor (Beam) through a farmland using the user's EEG
 * signals. **EEG Beam Tractor Game:** Complete implementation of the game logic
 * and related rules. **Integration with FogFaaS:** Utilizes FogFaaS functions
 * and tools to simulate the environment, collect data, and evaluate the
 * performance of EEG-based control algorithms. **Educational Example:**
 * Provides a clear and understandable structure for implementing similar
 * problems in the FogFaaS environment. **Customizability:** Allows for
 * modification of game parameters, control algorithms, and evaluation metrics
 * to experiment with and compare different methods.
 * 
 * @author Ghaseminya
 */
public class VRGame_FogFaaS {
	private static FogFaaSController controller;

	static List<FogFaaSDevice> fogDevices = new ArrayList<FogFaaSDevice>();
	static List<FogFaaSSensor> sensors = new ArrayList<FogFaaSSensor>();
	static List<FogFaaSActuator> actuators = new ArrayList<FogFaaSActuator>();

	static boolean CLOUD = false;

	static int numOfDepts = 2;
	static int numOfMobilesPerDept = 5;
	static double EEG_TRANSMISSION_TIME = 5;

	public static void main(String[] args) {

		Log.printLine("Starting VRGame FaaS version...");

		try {
			Log.disable();
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			CloudSim.init(num_user, calendar, trace_flag);

			String appId = "vr_game"; // identifier of the application

			FogFaaSBroker broker = new FogFaaSBroker("broker");

			FogFaaSApplication application = createApplication(appId, broker.getId());
			application.setUserId(broker.getId());

			createFogDevices(broker.getId(), appId);

			FogFaaSModuleMapping moduleMapping = FogFaaSModuleMapping.createModuleMapping(); // initializing a module
																								// mapping

			if (CLOUD) {
				moduleMapping.addModuleToDevice("connector", "cloud"); // fixing all instances of the Connector
				moduleMapping.addModuleToDevice("concentration_calculator", "cloud"); // fixing all instances of the
																						// Concentration Calculator
																						// module to the Cloud
				for (FogFaaSDevice device : fogDevices) {
					if (device.getName().startsWith("m")) {
						// moduleMapping.addModuleToDevice("client", device.getName(), 1); // fixing all
						// instances of the Client module to the Smartphones
						moduleMapping.addModuleToDevice("client", device.getName()); // fixing all instances of the
																						// Client module to the
																						// Smartphones
					}
				}
			} else {
				moduleMapping.addModuleToDevice("connector", "cloud"); // fixing all instances of the Connector module
																		// to the Cloud
				// rest of the modules will be placed by the Edge-ward placement policy
			}

			controller = new FogFaaSController("master-controller", fogDevices, sensors, actuators);

			controller.submitApplication(application, 0,
					(CLOUD) ? (new FogFaaSModulePlacementMapping(fogDevices, application, moduleMapping))
							: (new FogFaaSModulePlacementEdgewards(fogDevices, sensors, actuators, application,
									moduleMapping)));
			createRequests();
			TimeKeeper.getInstance().setSimulationStartTime(Calendar.getInstance().getTimeInMillis());

			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			Log.printLine("VRGame FaaS version finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Errors Occured");
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

		// Serverless requests could utilize part of a vCPU core in case container
		// concurrency is enabled
		FogFaaSUtilizationModelPartial utilizationModelPar = new FogFaaSUtilizationModelPartial();
		UtilizationModelFull utilizationModel = new UtilizationModelFull();

		while ((line = br.readLine()) != null) {
			String[] data = line.split(cvsSplitBy);
			FogFaaSRequest request = null;

			try {
				request = new FogFaaSRequest(IDs.pollId(FogFaaSRequest.class),
						Double.parseDouble(String.valueOf(data[0])), String.valueOf(data[1]), Long.parseLong(data[2]),
						Integer.parseInt(data[3]), Integer.parseInt(data[4]), Long.parseLong(data[5]),
						Double.parseDouble(data[6]), Double.parseDouble(data[7]), fileSize, outputSize,
						utilizationModelPar, utilizationModelPar, utilizationModel, 0, true);
				System.out.println("request No " + request.getCloudletId());
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}

			request.setUserId(controller.getId());
			System.out.println(CloudSim.clock() + " request created. This request arrival time is :"
					+ Double.parseDouble(data[0]));
			controller.requestArrivalTime.add(Double.parseDouble(data[0]) + Constants.FUNCTION_SCHEDULING_DELAY);
			// requestList.add(request);
			controller.requestQueue.add(request);
			createdRequests += 1;

		}
	}

	/**
	 * Creates the fog devices in the physical topology of the simulation.
	 * 
	 * @param userId
	 * @param appId
	 */
	private static void createFogDevices(int userId, String appId) {
		FogFaaSDevice cloud = createFogDevice("cloud", 44800, 40000, 100, 10000, 0, 0.01, 16 * 103, 16 * 83.25); // creates
																													// the
																													// fog
																													// device
																													// Cloud
																													// at
																													// the
																													// apex
																													// of
																													// the
																													// hierarchy
																													// with
																													// level=0
		cloud.setParentId(-1);
		FogFaaSDevice proxy = createFogDevice("proxy-server", 2800, 4000, 10000, 10000, 1, 0.0, 107.339, 83.4333); // creates
																													// the
																													// fog
																													// device
																													// Proxy
																													// Server
																													// (level=1)
		proxy.setParentId(cloud.getId()); // setting Cloud as parent of the Proxy Server
		proxy.setUplinkLatency(100); // latency of connection from Proxy Server to the Cloud is 100 ms

		fogDevices.add(cloud);
		fogDevices.add(proxy);

		for (int i = 0; i < numOfDepts; i++) {
			addGw(i + "", userId, appId, proxy.getId()); // adding a fog device for every Gateway in physical topology.
															// The parent of each gateway is the Proxy Server
		}

	}

	private static FogFaaSDevice addGw(String id, int userId, String appId, int parentId) {
		FogFaaSDevice dept = createFogDevice("d-" + id, 2800, 4000, 10000, 10000, 1, 0.0, 107.339, 83.4333);
		fogDevices.add(dept);
		dept.setParentId(parentId);
		dept.setUplinkLatency(4); // latency of connection between gateways and proxy server is 4 ms
		for (int i = 0; i < numOfMobilesPerDept; i++) {
			String mobileId = id + "-" + i;
			FogFaaSDevice mobile = addMobile(mobileId, userId, appId, dept.getId()); // adding mobiles to the physical
																						// topology. Smartphones have
																						// been modeled as fog devices
																						// as well.
			mobile.setUplinkLatency(2); // latency of connection between the smartphone and proxy server is 4 ms
			fogDevices.add(mobile);
		}
		return dept;
	}

	private static FogFaaSDevice addMobile(String id, int userId, String appId, int parentId) {
		FogFaaSDevice mobile = createFogDevice("m-" + id, 1000, 1000, 10000, 270, 3, 0, 87.53, 82.44);
		mobile.setParentId(parentId);
		FogFaaSSensor eegSensor = new FogFaaSSensor("s-" + id, "EEG", userId, appId,
				new DeterministicDistribution(EEG_TRANSMISSION_TIME)); // inter-transmission time of EEG sensor follows
																		// a deterministic distribution
		sensors.add(eegSensor);
		FogFaaSActuator display = new FogFaaSActuator("a-" + id, userId, appId, "DISPLAY");
		actuators.add(display);
		eegSensor.setGatewayDeviceId(mobile.getId());
		eegSensor.setLatency(6.0); // latency of connection between EEG sensors and the parent Smartphone is 6 ms
		display.setGatewayDeviceId(mobile.getId());
		display.setLatency(1.0); // latency of connection between Display actuator and the parent Smartphone is 1
									// ms
		return mobile;
	}

	/**
	 * Creates a vanilla fog device
	 * 
	 * @param nodeName
	 *            name of the device to be used in simulation
	 * @param mips
	 *            MIPS
	 * @param ram
	 *            RAM
	 * @param upBw
	 *            uplink bandwidth
	 * @param downBw
	 *            downlink bandwidth
	 * @param level
	 *            hierarchy level of the device
	 * @param ratePerMips
	 *            cost rate per MIPS used
	 * @param busyPower
	 * @param idlePower
	 * @return
	 */
	private static FogFaaSDevice createFogDevice(String nodeName, long mips, int ram, long upBw, long downBw, int level,
			double ratePerMips, double busyPower, double idlePower) {

		List<Pe> peList = new ArrayList<Pe>();

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating

		int hostId = FogUtils.generateEntityId();
		long storage = 1000000; // host storage
		int bw = 10000;

		PowerHost host = new PowerHost(hostId, new RamProvisionerSimple(ram), new BwProvisionerOverbooking(bw), storage,
				peList, new StreamOperatorScheduler(peList), new FogLinearPowerModel(busyPower, idlePower));

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

		FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(arch, os, vmm, host, time_zone, cost,
				costPerMem, costPerStorage, costPerBw);

		FogFaaSDevice fogdevice = null;
		try {
			fogdevice = new FogFaaSDevice(nodeName, characteristics, new AppModuleAllocationPolicy(hostList),
					storageList, 10, upBw, downBw, 0, ratePerMips);
		} catch (Exception e) {
			e.printStackTrace();
		}

		fogdevice.setLevel(level);
		return fogdevice;
	}

	/**
	 * Function to create the EEG Tractor Beam game application in the DDF model.
	 * 
	 * @param appId
	 *            unique identifier of the application
	 * @param userId
	 *            identifier of the user of the application
	 * @return
	 */
	@SuppressWarnings({ "serial" })
	private static FogFaaSApplication createApplication(String appId, int userId) {

		FogFaaSApplication application = FogFaaSApplication.createApplication(appId, userId); // creates an empty
																								// application model
																								// (empty directed
																								// graph)

		/*
		 * Adding modules (vertices) to the application model (directed graph)
		 */
		application.addAppModule("client", 10); // adding module Client to the application model
		application.addAppModule("concentration_calculator", 10); // adding module Concentration Calculator to the
																	// application model
		application.addAppModule("connector", 10); // adding module Connector to the application model

		/*
		 * Connecting the application modules (vertices) in the application model
		 * (directed graph) with edges
		 */
		if (EEG_TRANSMISSION_TIME == 10)
			application.addAppEdge("EEG", "client", 2000, 500, "EEG", FogFaaSTuple.UP, FogFaaSAppEdge.SENSOR); // adding
																												// edge
																												// from
																												// EEG
																												// (sensor)
																												// to
																												// Client
																												// module
																												// carrying
																												// tuples
																												// of
																												// type
																												// EEG
		else
			application.addAppEdge("EEG", "client", 3000, 500, "EEG", FogFaaSTuple.UP, FogFaaSAppEdge.SENSOR);
		application.addAppEdge("client", "concentration_calculator", 3500, 500, "_SENSOR", FogFaaSTuple.UP,
				FogFaaSAppEdge.MODULE); // adding edge from Client to Concentration Calculator module carrying tuples of
										// type _SENSOR
		application.addAppEdge("concentration_calculator", "connector", 100, 1000, 1000, "PLAYER_GAME_STATE",
				FogFaaSTuple.UP, FogFaaSAppEdge.MODULE); // adding periodic edge (period=1000ms) from Concentration
															// Calculator to Connector module carrying tuples of type
															// PLAYER_GAME_STATE
		application.addAppEdge("concentration_calculator", "client", 14, 500, "CONCENTRATION", FogFaaSTuple.DOWN,
				FogFaaSAppEdge.MODULE); // adding edge from Concentration Calculator to Client module carrying tuples of
										// type CONCENTRATION
		application.addAppEdge("connector", "client", 100, 28, 1000, "GLOBAL_GAME_STATE", FogFaaSTuple.DOWN,
				FogFaaSAppEdge.MODULE); // adding periodic edge (period=1000ms) from Connector to Client module carrying
										// tuples of type GLOBAL_GAME_STATE
		application.addAppEdge("client", "DISPLAY", 1000, 500, "SELF_STATE_UPDATE", FogFaaSTuple.DOWN,
				FogFaaSAppEdge.ACTUATOR); // adding edge from Client module to Display (actuator) carrying tuples of
											// type SELF_STATE_UPDATE
		application.addAppEdge("client", "DISPLAY", 1000, 500, "GLOBAL_STATE_UPDATE", FogFaaSTuple.DOWN,
				FogFaaSAppEdge.ACTUATOR); // adding edge from Client module to Display (actuator) carrying tuples of
											// type GLOBAL_STATE_UPDATE

		/*
		 * Defining the input-output relationships (represented by selectivity) of the
		 * application modules.
		 */
		application.addTupleMapping("client", "EEG", "_SENSOR", new FractionalSelectivity(0.9)); // 0.9 tuples of type
																									// _SENSOR are
																									// emitted by Client
																									// module per
																									// incoming tuple of
																									// type EEG
		application.addTupleMapping("client", "CONCENTRATION", "SELF_STATE_UPDATE", new FractionalSelectivity(1.0)); // 1.0
																														// tuples
																														// of
																														// type
																														// SELF_STATE_UPDATE
																														// are
																														// emitted
																														// by
																														// Client
																														// module
																														// per
																														// incoming
																														// tuple
																														// of
																														// type
																														// CONCENTRATION
		application.addTupleMapping("concentration_calculator", "_SENSOR", "CONCENTRATION",
				new FractionalSelectivity(1.0)); // 1.0 tuples of type CONCENTRATION are emitted by Concentration
													// Calculator module per incoming tuple of type _SENSOR
		application.addTupleMapping("client", "GLOBAL_GAME_STATE", "GLOBAL_STATE_UPDATE",
				new FractionalSelectivity(1.0)); // 1.0 tuples of type GLOBAL_STATE_UPDATE are emitted by Client module
													// per incoming tuple of type GLOBAL_GAME_STATE

		/*
		 * Defining application loops to monitor the latency of. Here, we add only one
		 * loop for monitoring : EEG(sensor) -> Client -> Concentration Calculator ->
		 * Client -> DISPLAY (actuator)
		 */
		final FogFaaSAppLoop loop1 = new FogFaaSAppLoop(new ArrayList<String>() {
			{
				add("EEG");
				add("client");
				add("concentration_calculator");
				add("client");
				add("DISPLAY");
			}
		});
		List<FogFaaSAppLoop> loops = new ArrayList<FogFaaSAppLoop>() {
			{
				add(loop1);
			}
		};
		application.setLoops(loops);

		return application;
	}
}