package org.fog.serverless.faas;
/**
 * Function Class for FoGFaaS extension.
 *
 * @author Ghaseminya
 * Created on 12/16/2024
 */
public class FogFaaSFunction {
	private String functionId;
	private double cpuRequired;
	private double memoryRequired;
	private double executionTime;

	public FogFaaSFunction(String functionId, double cpuRequired, double memoryRequired, double executionTime) {
		this.functionId = functionId;
		this.cpuRequired = cpuRequired;
		this.memoryRequired = memoryRequired;
		this.executionTime = executionTime;
		System.out.println("Created function: " + functionId);
		System.out.println("    CPU Required: " + cpuRequired);
		System.out.println("    RAM Required: " + memoryRequired);
	}

	public String getFunctionId() {
		return functionId;
	}

	public double getCpuRequired() {
		return cpuRequired;
	}

	public double getMemoryRequired() {
		return memoryRequired;
	}

	public double getExecutionTime() {
		return executionTime;
	}
}
