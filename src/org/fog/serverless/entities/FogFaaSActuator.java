package org.fog.serverless.entities;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.serverless.application.FogFaaSAppLoop;
import org.fog.serverless.application.FogFaaSApplication;
import org.fog.utils.FogEvents;
import org.fog.utils.GeoLocation;
import org.fog.utils.Logger;
import org.fog.serverless.utils.FogFaaSTimeKeeper;

public class FogFaaSActuator extends SimEntity{

	private int gatewayDeviceId;
	private double latency;
	private GeoLocation geoLocation;
	private String appId;
	private int userId;
	private String actuatorType;
	private FogFaaSApplication app;
	
	public FogFaaSActuator(String name, int userId, String appId, int gatewayDeviceId, double latency, GeoLocation geoLocation, String actuatorType, String srcModuleName) {
		super(name);
		this.setAppId(appId);
		this.gatewayDeviceId = gatewayDeviceId;
		this.geoLocation = geoLocation;
		setUserId(userId);
		setActuatorType(actuatorType);
		setLatency(latency);
	}
	
	public FogFaaSActuator(String name, int userId, String appId, String actuatorType) {
		super(name);
		this.setAppId(appId);
		setUserId(userId);
		setActuatorType(actuatorType);
	}

	@Override
	public void startEntity() {
		sendNow(gatewayDeviceId, FogEvents.ACTUATOR_JOINED, getLatency());
	}

	@Override
	public void processEvent(SimEvent ev) {
		switch(ev.getTag()){
		case FogEvents.TUPLE_ARRIVAL:
			processTupleArrival(ev);
			break;
		}		
	}

	private void processTupleArrival(SimEvent ev) {
		FogFaaSTuple tuple = (FogFaaSTuple)ev.getData();
		Logger.debug(getName(), "Received tuple "+tuple.getCloudletId()+"on "+tuple.getDestModuleName());
		String srcModule = tuple.getSrcModuleName();
		String destModule = tuple.getDestModuleName();
		FogFaaSApplication app = getApp();
		
		for(FogFaaSAppLoop loop : app.getLoops()){
			if(loop.hasEdge(srcModule, destModule) && loop.isEndModule(destModule)){
				
				Double startTime = FogFaaSTimeKeeper.getInstance().getEmitTimes().get(tuple.getActualTupleId());
				if(startTime==null)
					break;
				if(!FogFaaSTimeKeeper.getInstance().getLoopIdToCurrentAverage().containsKey(loop.getLoopId())){
					FogFaaSTimeKeeper.getInstance().getLoopIdToCurrentAverage().put(loop.getLoopId(), 0.0);
					FogFaaSTimeKeeper.getInstance().getLoopIdToCurrentNum().put(loop.getLoopId(), 0);
				}
				double currentAverage = FogFaaSTimeKeeper.getInstance().getLoopIdToCurrentAverage().get(loop.getLoopId());
				int currentCount = FogFaaSTimeKeeper.getInstance().getLoopIdToCurrentNum().get(loop.getLoopId());
				double delay = CloudSim.clock()- FogFaaSTimeKeeper.getInstance().getEmitTimes().get(tuple.getActualTupleId());
				FogFaaSTimeKeeper.getInstance().getEmitTimes().remove(tuple.getActualTupleId());
				double newAverage = (currentAverage*currentCount + delay)/(currentCount+1);
				FogFaaSTimeKeeper.getInstance().getLoopIdToCurrentAverage().put(loop.getLoopId(), newAverage);
				FogFaaSTimeKeeper.getInstance().getLoopIdToCurrentNum().put(loop.getLoopId(), currentCount+1);
				break;
			}
		}
	}

	@Override
	public void shutdownEntity() {
		
	}

	public int getGatewayDeviceId() {
		return gatewayDeviceId;
	}

	public void setGatewayDeviceId(int gatewayDeviceId) {
		this.gatewayDeviceId = gatewayDeviceId;
	}

	public GeoLocation getGeoLocation() {
		return geoLocation;
	}

	public void setGeoLocation(GeoLocation geoLocation) {
		this.geoLocation = geoLocation;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getActuatorType() {
		return actuatorType;
	}

	public void setActuatorType(String actuatorType) {
		this.actuatorType = actuatorType;
	}

	public FogFaaSApplication getApp() {
		return app;
	}

	public void setApp(FogFaaSApplication app) {
		this.app = app;
	}

	public double getLatency() {
		return latency;
	}

	public void setLatency(double latency) {
		this.latency = latency;
	}

}
