package org.fog.serverless.placement;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.serverless.application.FogFaaSAppEdge;
import org.fog.serverless.application.FogFaaSAppLoop;
import org.fog.serverless.application.FogFaaSAppModule;
import org.fog.serverless.application.FogFaaSApplication;
import org.fog.serverless.entities.FogFaaSActuator;
import org.fog.serverless.entities.FogFaaSDevice;
import org.fog.serverless.entities.FogFaaSSensor;
import org.fog.serverless.faas.FogFaaSRequest;
import org.fog.serverless.placement.FogFaaSModulePlacement;
import org.fog.utils.Config;
import org.fog.utils.FogEvents;
import org.fog.utils.FogUtils;
import org.fog.utils.NetworkUsageMonitor;
import org.fog.utils.TimeKeeper;

import java.util.*;

/**
 * Controller class for FoGFaaS extension. This class represents a Controller who uses the Cloud data center.
 *
 * @author Ghaseminya Created on 12/16/2024
 */

public class FogFaaSController extends SimEntity{
	public static boolean ONLY_CLOUD = false;
    
    private Map<String, FogFaaSApplication> applications;
	private List<FogFaaSDevice> fogDevices;
	private List<FogFaaSSensor> sensors;
	private List<FogFaaSActuator> actuators;
    public Queue<Double> requestArrivalTime = new LinkedList<Double>();
    public Queue<FogFaaSRequest> requestQueue = new LinkedList<FogFaaSRequest>();

	private Map<String, Integer> appLaunchDelays;

	private Map<String, FogFaaSModulePlacement> appModulePlacementPolicy;
//	private List<FogFaaSRequest> faaSRequests=new ArrayList<FogFaaSRequest>();
	
	public int controllerId=0;
    public int containerId = 1;;
    private boolean reschedule = false;
    int dcount = 1;
    public int exsitingContCount = 0;
    public int noOfTasks = 0;
    public int noOfTasksReturned = 0;
	public FogFaaSController(String name, List<FogFaaSDevice> fogDevices, List<FogFaaSSensor> sensors, List<FogFaaSActuator> actuators) {
//		super(name, fogDevices, sensors, actuators);
		super(name);
		this.applications = new HashMap<String, FogFaaSApplication>();
		setAppLaunchDelays(new HashMap<String, Integer>());
		setAppModulePlacementPolicy(new HashMap<String, FogFaaSModulePlacement>());
		for(FogFaaSDevice fogDevice : fogDevices){
			fogDevice.setControllerId(getId());
		}
		setFogDevices(fogDevices);
		setActuators(actuators);
		setSensors(sensors);
		
		
		connectWithLatencies();
		
		// TODO Auto-generated constructor stub
	}
	private FogFaaSDevice getFogDeviceById(int id){
		for(FogFaaSDevice fogDevice : getFogDevices()){
			if(id==fogDevice.getId())
				return fogDevice;
		}
		return null;
	}
	private void connectWithLatencies(){
		for(FogFaaSDevice fogDevice : getFogDevices()){
			FogFaaSDevice parent = getFogDeviceById(fogDevice.getParentId());
			if(parent == null)
				continue;
			double latency = fogDevice.getUplinkLatency();
			parent.getChildToLatencyMap().put(fogDevice.getId(), latency);
			parent.getChildrenIds().add(fogDevice.getId());
		}
	}

	@Override
	public void startEntity() {
		System.out.println(applications);
		System.out.println(applications.size());
		for(String appId : applications.keySet()){
			if(getAppLaunchDelays().get(appId)==0)
				processAppSubmit(applications.get(appId));
			else
				send(getId(), getAppLaunchDelays().get(appId), FogEvents.APP_SUBMIT, applications.get(appId));
		}
		send(getId(), Config.RESOURCE_MANAGE_INTERVAL, FogEvents.CONTROLLER_RESOURCE_MANAGE);
		send(getId(), Config.MAX_SIMULATION_TIME, FogEvents.STOP_SIMULATION);
		for(FogFaaSDevice dev : getFogDevices())
			sendNow(dev.getId(), FogEvents.RESOURCE_MGMT);
	}
//	@Override
	public void submitApplication(FogFaaSApplication application, FogFaaSModulePlacement modulePlacement){
		submitApplication(application, 0, modulePlacement);
	}
//	@Override
	public void submitApplication(FogFaaSApplication application, int delay, FogFaaSModulePlacement modulePlacement){
		FogUtils.appIdToGeoCoverageMap.put(application.getAppId(), application.getGeoCoverage());
		getApplications().put(application.getAppId(), application);
		getAppLaunchDelays().put(application.getAppId(), delay);
		getAppModulePlacementPolicy().put(application.getAppId(), modulePlacement);
		
		for(FogFaaSSensor sensor : sensors){
			sensor.setApp(getApplications().get(sensor.getAppId()));
		}
		for(FogFaaSActuator ac : actuators){
			ac.setApp(getApplications().get(ac.getAppId()));
		}		
		for(FogFaaSAppEdge edge : application.getEdges()){
			if(edge.getEdgeType() == FogFaaSAppEdge.ACTUATOR){
				String moduleName = edge.getSource();
				for(FogFaaSActuator actuator : getActuators()){
					if(actuator.getActuatorType().equalsIgnoreCase(edge.getDestination()))
						application.getModuleByName(moduleName).subscribeActuator(actuator.getId(), edge.getTupleType());
				}
			}
		}	
	}
	public void submitFogList(List<FogFaaSDevice> fogdevice) {
		this.fogDevices=fogdevice;
	}
//	public List<FogFaaSRequest> getFaaSRequests() {
//		return faaSRequests;
//	}
//
//	public void setFaaSRequests(List<FogFaaSRequest> faaSRequests) {
//		this.faaSRequests = faaSRequests;
//	}

	private void processTupleFinished(SimEvent ev) {
	}
	@Override
	public void processEvent(SimEvent ev) {
		Scanner sc=new Scanner(System.in);
		switch(ev.getTag()){
		case FogEvents.APP_SUBMIT:
			processAppSubmit(ev);
			break;
		case FogEvents.TUPLE_FINISHED:
			processTupleFinished(ev);
			break;
		case FogEvents.CONTROLLER_RESOURCE_MANAGE:
			manageResources(ev);
			break;
		case FogEvents.STOP_SIMULATION:
			CloudSim.stopSimulation();
			printTimeDetails();
			printPowerDetails();
			printCostDetails();
			printNetworkUsageDetails();
			System.exit(0);
			break;
			
		}
	}

	private void processAppSubmit(SimEvent ev){
		FogFaaSApplication app = (FogFaaSApplication) ev.getData();
		processAppSubmit(app);
	}
//	@Override
	public void processAppSubmit(FogFaaSApplication application) {
		System.out.println(CloudSim.clock()+" Submitted application "+ application.getAppId());
		FogUtils.appIdToGeoCoverageMap.put(application.getAppId(), application.getGeoCoverage());
		getApplications().put(application.getAppId(), application);
		
		FogFaaSModulePlacement modulePlacement = getAppModulePlacementPolicy().get(application.getAppId());
		for(FogFaaSDevice fogDevice : fogDevices){
			sendNow(fogDevice.getId(), FogEvents.ACTIVE_APP_UPDATE, application);
		}
		
		Map<Integer, List<FogFaaSAppModule>> deviceToModuleMap = modulePlacement.getDeviceToModuleMap();
		for(Integer deviceId : deviceToModuleMap.keySet()){
			for(FogFaaSAppModule module : deviceToModuleMap.get(deviceId)){
				sendNow(deviceId, FogEvents.APP_SUBMIT, application);
				sendNow(deviceId, FogEvents.LAUNCH_MODULE, module);
			}
		}
	}

	private void printTimeDetails() {
		System.out.println("=========================================");
		System.out.println("============== RESULTS ==================");
		System.out.println("=========================================");
		System.out.println("EXECUTION TIME : "+ (Calendar.getInstance().getTimeInMillis() - TimeKeeper.getInstance().getSimulationStartTime()));
		System.out.println("=========================================");
		System.out.println("APPLICATION LOOP DELAYS");
		System.out.println("=========================================");
		for(Integer loopId : TimeKeeper.getInstance().getLoopIdToTupleIds().keySet()){
			/*double average = 0, count = 0;
			for(int tupleId : TimeKeeper.getInstance().getLoopIdToTupleIds().get(loopId)){
				Double startTime = 	TimeKeeper.getInstance().getEmitTimes().get(tupleId);
				Double endTime = 	TimeKeeper.getInstance().getEndTimes().get(tupleId);
				if(startTime == null || endTime == null)
					break;
				average += endTime-startTime;
				count += 1;
			}
			System.out.println(getStringForLoopId(loopId) + " ---> "+(average/count));*/
			System.out.println(getStringForLoopId(loopId) + " ---> "+TimeKeeper.getInstance().getLoopIdToCurrentAverage().get(loopId));
		}
		System.out.println("=========================================");
		System.out.println("TUPLE CPU EXECUTION DELAY");
		System.out.println("=========================================");
		
		for(String tupleType : TimeKeeper.getInstance().getTupleTypeToAverageCpuTime().keySet()){
			System.out.println(tupleType + " ---> "+TimeKeeper.getInstance().getTupleTypeToAverageCpuTime().get(tupleType));
		}
		
		System.out.println("=========================================");
	}
	private String getStringForLoopId(int loopId){
		for(String appId : getApplications().keySet()){
			FogFaaSApplication app = getApplications().get(appId);
			for(FogFaaSAppLoop loop : app.getLoops()){
				if(loop.getLoopId() == loopId)
					return loop.getModules().toString();
			}
		}
		return null;
	}
	private void printPowerDetails() {
		for(FogFaaSDevice fogDevice : getFogDevices()){
			System.out.println(fogDevice.getName() + " : Energy Consumed = "+fogDevice.getEnergyConsumption());
		}
	}
	private void printCostDetails(){
		System.out.println("Cost of execution in cloud = "+getCloud().getTotalCost());
	}

	private void printNetworkUsageDetails() {
		System.out.println("Total network usage = "+NetworkUsageMonitor.getNetworkUsage()/Config.MAX_SIMULATION_TIME);		
	}

	private FogFaaSDevice getCloud(){
		for(FogFaaSDevice dev : getFogDevices())
			if(dev.getName().equals("cloud"))
				return dev;
		return null;
	}

	public List<FogFaaSDevice> getFogDevices() {
		return fogDevices;
	}

	public void setFogDevices(List<FogFaaSDevice> fogDevices) {
		this.fogDevices = fogDevices;
	}

	public Map<String, Integer> getAppLaunchDelays() {
		return appLaunchDelays;
	}

	public void setAppLaunchDelays(Map<String, Integer> appLaunchDelays) {
		this.appLaunchDelays = appLaunchDelays;
	}

	public Map<String, FogFaaSApplication> getApplications() {
		return applications;
	}

	public void setApplications(Map<String, FogFaaSApplication> applications) {
		this.applications = applications;
	}

	public List<FogFaaSSensor> getSensors() {
		return sensors;
	}

	public void setSensors(List<FogFaaSSensor> sensors) {
		for(FogFaaSSensor sensor : sensors)
			sensor.setControllerId(getId());
		this.sensors = sensors;
	}

	public List<FogFaaSActuator> getActuators() {
		return actuators;
	}

	public void setActuators(List<FogFaaSActuator> actuators) {
		this.actuators = actuators;
	}

	public Map<String, FogFaaSModulePlacement> getAppModulePlacementPolicy() {
		return appModulePlacementPolicy;
	}

	public void setAppModulePlacementPolicy(Map<String, FogFaaSModulePlacement> appModulePlacementPolicy) {
		this.appModulePlacementPolicy = appModulePlacementPolicy;
	}



	protected void manageResources(SimEvent ev){
		


      
		send(getId(), Config.RESOURCE_MANAGE_INTERVAL, FogEvents.CONTROLLER_RESOURCE_MANAGE);
	}
	
	
	@Override
	public void shutdownEntity() {	
	}
}