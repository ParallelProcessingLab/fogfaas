package org.fog.serverless.faas;

import org.cloudbus.cloudsim.UtilizationModel;
/**
 * Utilization Model Partial Class for FoGFaaS extension.
 *
 * @author Ghaseminya
 * Created on 12/16/2024
 */
public class FogFaaSUtilizationModelPartial implements UtilizationModel {
    @Override
    public double getUtilization(double time) {
        return 0;
    }

    public double getCpuUtilization(FogFaaSRequest request) {
        return request.getCpuShareRequest();
    }
    public double getMemUtilization(FogFaaSRequest request) {
        return request.getMemShareRequest();
    }


}