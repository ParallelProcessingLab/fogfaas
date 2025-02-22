package org.fog.serverless.placement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.core.CloudSim;
import org.fog.serverless.application.FogFaaSAppModule;
import org.fog.serverless.application.FogFaaSApplication;
import org.fog.serverless.entities.FogFaaSDevice;

public abstract class FogFaaSModulePlacement {


	public static int ONLY_CLOUD = 1;
	public static int EDGEWARDS = 2;
	public static int USER_MAPPING = 3;

	private List<FogFaaSDevice> fogDevices;
	private FogFaaSApplication application;
	private Map<String, List<Integer>> moduleToDeviceMap;
	private Map<Integer, List<FogFaaSAppModule>> deviceToModuleMap;
	private Map<Integer, Map<String, Integer>> moduleInstanceCountMap;

	protected Map<Integer, Map<Integer,List<String>>> modulesOnPath;
	protected Map<Integer, List<String>> modulesOnDevice;
	protected Boolean clusteringFeature;

	protected abstract void mapModules();

	protected boolean canBeCreated(FogFaaSDevice fogDevice, FogFaaSAppModule module){
		return fogDevice.getVmAllocationPolicy().allocateHostForVm(module);
	}

	protected int getParentDevice(int fogDeviceId){
		return ((FogFaaSDevice)CloudSim.getEntity(fogDeviceId)).getParentId();
	}

	protected FogFaaSDevice getFogDeviceById(int fogDeviceId){
		return (FogFaaSDevice)CloudSim.getEntity(fogDeviceId);
	}

	protected boolean createModuleInstanceOnDevice(FogFaaSAppModule _module, final FogFaaSDevice device, int instanceCount){
		return false;
	}

	protected boolean createModuleInstanceOnDevice(FogFaaSAppModule _module, final FogFaaSDevice device){
		FogFaaSAppModule module = null;
		if(getModuleToDeviceMap().containsKey(_module.getName()))
			module = new FogFaaSAppModule(_module);
		else
			module = _module;

		if(canBeCreated(device, module)){
			System.out.println("Creating "+module.getName()+" on device "+device.getName());

			if(!getDeviceToModuleMap().containsKey(device.getId()))
				getDeviceToModuleMap().put(device.getId(), new ArrayList<FogFaaSAppModule>());
			getDeviceToModuleMap().get(device.getId()).add(module);

			if(!getModuleToDeviceMap().containsKey(module.getName()))
				getModuleToDeviceMap().put(module.getName(), new ArrayList<Integer>());
			getModuleToDeviceMap().get(module.getName()).add(device.getId());
			return true;
		} else {
			System.err.println("Module "+module.getName()+" cannot be created on device "+device.getName());
			System.err.println("Terminating");
			return false;
		}
	}

	protected FogFaaSDevice getDeviceByName(String deviceName) {
		for(FogFaaSDevice dev : getFogDevices()){
			if(dev.getName().equals(deviceName))
				return dev;
		}
		return null;
	}

	protected FogFaaSDevice getDeviceById(int id){
		for(FogFaaSDevice dev : getFogDevices()){
			if(dev.getId() == id)
				return dev;
		}
		return null;
	}

	public List<FogFaaSDevice> getFogDevices() {
		return fogDevices;
	}

	public void setFogDevices(List<FogFaaSDevice> fogDevices) {
		this.fogDevices = fogDevices;
	}

	public FogFaaSApplication getApplication() {
		return application;
	}

	public void setApplication(FogFaaSApplication application) {
		this.application = application;
	}

	public Map<String, List<Integer>> getModuleToDeviceMap() {
		return moduleToDeviceMap;
	}

	public void setModuleToDeviceMap(Map<String, List<Integer>> moduleToDeviceMap) {
		this.moduleToDeviceMap = moduleToDeviceMap;
	}

	public Map<Integer, List<FogFaaSAppModule>> getDeviceToModuleMap() {
		return deviceToModuleMap;
	}

	public void setDeviceToModuleMap(Map<Integer, List<FogFaaSAppModule>> deviceToModuleMap) {
		this.deviceToModuleMap = deviceToModuleMap;
	}

	public Map<Integer, Map<String, Integer>> getModuleInstanceCountMap() {
		return moduleInstanceCountMap;
	}

	public void setModuleInstanceCountMap(Map<Integer, Map<String, Integer>> moduleInstanceCountMap) {
		this.moduleInstanceCountMap = moduleInstanceCountMap;
	}

	public Map<Integer, Map<Integer, List<String>>> getModulesOnPath() {
		return modulesOnPath;
	}

	public void setModulesOnPath(Map<Integer, Map<Integer, List<String>>> modulesOnPath) {
		this.modulesOnPath = modulesOnPath;
	}

	public Map<Integer, List<String>> getModulesOnDevice() {
		return modulesOnDevice;
	}

	public void setModulesOnDevice(Map<Integer, List<String>> modulesOnDevice) {
		this.modulesOnDevice = modulesOnDevice;
	}

	public Boolean getClusteringFeature() {
		return clusteringFeature;
	}

	public void setClusteringFeature(Boolean clusteringFeature) {
		this.clusteringFeature = clusteringFeature;
	}


}
