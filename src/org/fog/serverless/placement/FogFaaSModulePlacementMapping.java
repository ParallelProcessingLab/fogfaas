package org.fog.serverless.placement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fog.serverless.application.FogFaaSAppModule;
import org.fog.serverless.application.FogFaaSApplication;
import org.fog.serverless.entities.FogFaaSDevice;

public class FogFaaSModulePlacementMapping extends FogFaaSModulePlacement{

	private FogFaaSModuleMapping moduleMapping;
	
	@Override
	protected void mapModules() {
		Map<String, List<String>> mapping = moduleMapping.getModuleMapping();
		for(String deviceName : mapping.keySet()){
			FogFaaSDevice device = getDeviceByName(deviceName);
			for(String moduleName : mapping.get(deviceName)){
				
				FogFaaSAppModule module = getApplication().getModuleByName(moduleName);
				if(module == null)
					continue;
				createModuleInstanceOnDevice(module, device);
				//getModuleInstanceCountMap().get(device.getId()).put(moduleName, mapping.get(deviceName).get(moduleName));
			}
		}
	}

	public FogFaaSModulePlacementMapping(List<FogFaaSDevice> fogDevices, FogFaaSApplication application, 
			FogFaaSModuleMapping moduleMapping){
		this.setFogDevices(fogDevices);
		this.setApplication(application);
		this.setModuleMapping(moduleMapping);
		this.setModuleToDeviceMap(new HashMap<String, List<Integer>>());
		this.setDeviceToModuleMap(new HashMap<Integer, List<FogFaaSAppModule>>());
		this.setModuleInstanceCountMap(new HashMap<Integer, Map<String, Integer>>());
		for(FogFaaSDevice device : getFogDevices())
			getModuleInstanceCountMap().put(device.getId(), new HashMap<String, Integer>());
		mapModules();
	}
	
	
	public FogFaaSModuleMapping getModuleMapping() {
		return moduleMapping;
	}
	public void setModuleMapping(FogFaaSModuleMapping moduleMapping) {
		this.moduleMapping = moduleMapping;
	}

	
}
