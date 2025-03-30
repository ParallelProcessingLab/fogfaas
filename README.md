# FogFaaS Simulator Project

This project leverages a Java-based simulator, built upon **iFogSim**, to model and analyze [mention specific scenarios or applications you're simulating. For example: intelligent video surveillance in data center networks, or EEG-based control systems]. This simulator extends iFogSim with FogFaaS features, as implemented in the [ParallelProcessingLab/fogfaas](https://github.com/ParallelProcessingLab/fogfaas) repository. It provides a flexible environment for simulating fog computing deployments and evaluating the performance of distributed applications.

## What is iFogSim (and FogFaaS Extension)?

iFogSim is a toolkit for simulating fog and cloud computing environments. It allows researchers and developers to:

*   **Model Fog and Cloud Infrastructure:** Define and simulate the architecture of fog and cloud deployments, including devices, fog nodes, and cloud data centers.
*   **Simulate Application Workloads:** Model the behavior of applications running in the fog environment, including task dependencies and resource requirements.
*   **Evaluate Performance Metrics:** Measure key performance indicators (KPIs) such as latency, energy consumption, and network bandwidth utilization.
*   **Cost Analysis:** Analyze the cost associated with deploying and running applications in a fog environment.

**FogFaaS Extension:** This project extends the core iFogSim functionality with specific features related to Fog Functions as a Service (FogFaaS), enabling you to:

*   **Model Function Deployment:** Simulate the deployment of lightweight functions (FaaS) to fog nodes.
*   **Experiment with Function Scheduling:** Evaluate different function scheduling policies for optimizing performance.
*   **Analyze Function Performance:** Track the execution time and resource consumption of individual functions.

## Project Overview

This project provides example simulation scenarios located in the `src/org/fog/serverless/examples` directory.([https://github.com/ParallelProcessingLab/fogfaas/tree/main/src/org/fog/serverless/examples](https://github.com/ParallelProcessingLab/fogfaas/tree/main/src/org/fog/serverless/examples)) contains the following example implementations:

*   **DCNS_FogFaaS.java:** Implements a Data Center Network Intelligent Servileness Simulation (DCNS) using FogFaaS.
*   **VRGame_FogFaaS.java:** Implements a EEG simulation using FogFaaS. This demonstrates the use of FogFaaS in a latency-sensitive application.


These examples demonstrate key FogFaaS features and provide a starting point for developing your own simulations.

## Getting Started

These instructions will guide you on how to set up and run the simulations in this project.

### Prerequisites

*   **Java Development Kit (JDK):** Ensure you have a compatible JDK (e.g., JDK 8 or later) installed on your system.
*   **Eclipse IDE:** This project is designed to be built and run within the Eclipse IDE.

### Installation

1.  **Clone the Repository:**

    

```bash
    git clone https://github.com/ParallelProcessingLab/fogfaas
    cd fogfaas
```
Import into Eclipse: Import the project into your Eclipse IDE as an existing Java project.

## Running the Simulations

Navigate to the Source Code: In Eclipse, navigate to the src/org/fog/serverless/examples directory in the Project Explorer.
Run the Simulation: Right-click on either DCNS_FogFaaS.java or VRGame_FogFaaS.java and select "Run As" -> "Java Application".

Refer to the comments within the Java code for specific instructions and configuration options for each simulation.

## Project Structure

README.md: This file.
src/main/java or src: Source code directory (Java). The core simulation logic is located within the src/org/fog/serverless/ package.

## Citation
If you use this code, please cite the research paper:
```
@ARTICLE{Ghaseminya2025,
  title    = "Fogfaas: providing serverless computing simulation for iFogSim
              and edge cloud",
  author   = "Ghaseminya, Mohammad Mahdi and Shahzadeh Fazeli, Seyed Abolfazl 
              and Abouei, Jamshid and Abbasi, Elham",
  journal  = "The Journal of Supercomputing",
  volume   =  81,
  number   =  5,
  pages    = "666",
  month    =  mar,
  year     =  2025
}


```

## Further Information:

iFogSim Official Documentation: [https://github.com/Cloudslab/iFogSim](https://github.com/Cloudslab/iFogSim)

FogFaaS Repository: [https://github.com/ParallelProcessingLab/fogfaas](https://github.com/ParallelProcessingLab/fogfaas)

## Contact:
Pull Request!
