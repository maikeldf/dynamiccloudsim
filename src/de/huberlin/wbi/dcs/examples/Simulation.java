package de.huberlin.wbi.dcs.examples;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.commons.math3.util.Pair;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import de.huberlin.wbi.dcs.CloudletSchedulerGreedyDivided;
import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.DynamicModel;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.HeterogeneousCloudlet;
import de.huberlin.wbi.dcs.VmAllocationPolicyRandom;
import de.huberlin.wbi.dcs.workflow.Workflow;
import de.huberlin.wbi.dcs.workflow.io.AlignmentTraceFileReader;
import de.huberlin.wbi.dcs.workflow.io.CuneiformLogFileReader;
import de.huberlin.wbi.dcs.workflow.io.DaxFileReader;
import de.huberlin.wbi.dcs.workflow.io.MontageTraceFileReader;
import de.huberlin.wbi.dcs.workflow.scheduler.C3;
import de.huberlin.wbi.dcs.workflow.scheduler.ERA;
import de.huberlin.wbi.dcs.workflow.scheduler.GreedyQueueScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.HEFTScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.LATEScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.StaticRoundRobinScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.AbstractWorkflowScheduler;

public class Simulation {

	public static void main(String[] args) {
		double totalRuntime = 0d;
		SimulationParameters.parseParameters(args);
		SimulationParameters.experiment = SimulationParameters.Experiment.HETEROGENEOUS_TEST_WORKFLOW;
		SimulationParameters.scheduler = SimulationParameters.Scheduler.ERA;
//		SimulationParameters.cpuHeterogeneityDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.ioHeterogeneityDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.bwHeterogeneityDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.cpuDynamicsDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.ioDynamicsDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.bwDynamicsDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.cpuNoiseDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.ioNoiseDistribution = SimulationParameters.Distribution.EXPONENTIAL;
//		SimulationParameters.bwNoiseDistribution = SimulationParameters.Distribution.EXPONENTIAL;
		SimulationParameters.outputDatacenterEvents = true;
		SimulationParameters.numberOfRuns = 100;
		SimulationParameters.ram =  (int) (0.5 * 1024);
		//Parameters.numberOfPes = 3;
		//Parameters.runtimeFactorInCaseOfFailure = 50d;
		try {
			for (int i = 0; i < SimulationParameters.numberOfRuns; i++) {
				if (!SimulationParameters.outputDatacenterEvents) {
					Log.disable();
				}
				// Initialize the CloudSim package
				int num_user = 1; // number of grid users
				Calendar calendar = Calendar.getInstance();
				boolean trace_flag = false; // mean trace events
				CloudSim.init(num_user, calendar, trace_flag);

				createDatacenter("Datacenter");
				AbstractWorkflowScheduler scheduler = Simulation.createScheduler(i);
				createVms(i, scheduler);
				Workflow workflow = buildWorkflow(scheduler);
				submitWorkflow(workflow, scheduler);

				// Start the simulation
				CloudSim.startSimulation();
				CloudSim.stopSimulation();

				totalRuntime += scheduler.getRuntime();
				System.out.println(scheduler.getRuntime() / 60);
			}

			writeEntry(CloudSim.getEntry());
			Log.setDisabled(false);
			Log.printLine("Average runtime in minutes: " + totalRuntime / SimulationParameters.numberOfRuns / 60);
			Log.printLine("Total Workload: " + HeterogeneousCloudlet.getTotalMi() + "mi " + HeterogeneousCloudlet.getTotalIo() + "io "
			    + HeterogeneousCloudlet.getTotalBw() + "bw");
			Log.printLine("Total VM Performance: " + DynamicHost.getTotalMi() + "mips " + DynamicHost.getTotalIo() + "iops " + DynamicHost.getTotalBw() + "bwps");
			Log.printLine("minimum minutes (quotient): " + HeterogeneousCloudlet.getTotalMi() / DynamicHost.getTotalMi() / 60 + " "
			    + HeterogeneousCloudlet.getTotalIo() / DynamicHost.getTotalIo() / 60 + " " + HeterogeneousCloudlet.getTotalBw() / DynamicHost.getTotalBw() / 60);
		} catch (Exception e) {
//			Log.printLine(e.getStackTrace().toString());
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}

	}

	private static void writeEntry(HashMap<Integer, Pair<Integer,List<Double>>> entry) throws IOException {
		final String fileName = "out.csv";
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
		writer.write(",Vm,High,Low");
		writer.newLine();
		for (Map.Entry<Integer, Pair<Integer,List<Double>>> entry_ :
				entry.entrySet()) {

			// put key and value separated by a colon
			writer.write(entry_.getKey() + ","
					+ entry_.getValue().getKey() + ","
					+ entry_.getValue().getValue().get(0) + ","
					+ entry_.getValue().getValue().get(1));

			writer.newLine();
		}

		writer.flush();
		writer.close();
	}

	public static AbstractWorkflowScheduler createScheduler(int i) {
		try {
			switch (SimulationParameters.scheduler) {
			case STATIC_ROUND_ROBIN:
				return new StaticRoundRobinScheduler("StaticRoundRobinScheduler", SimulationParameters.taskSlotsPerVm);
			case LATE:
				return new LATEScheduler("LATEScheduler", SimulationParameters.taskSlotsPerVm);
			case HEFT:
				return new HEFTScheduler("HEFTScheduler", SimulationParameters.taskSlotsPerVm);
			case JOB_QUEUE:
				return new GreedyQueueScheduler("GreedyQueueScheduler", SimulationParameters.taskSlotsPerVm);
			case C3:
				return new C3("C3", SimulationParameters.taskSlotsPerVm);
			case ERA:
				return new ERA("ERA", SimulationParameters.taskSlotsPerVm, i);
			default:
				return new GreedyQueueScheduler("GreedyQueueScheduler", SimulationParameters.taskSlotsPerVm);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void createVms(int run, AbstractWorkflowScheduler scheduler) {
		// Create VMs
		List<Vm> vmlist = createVMList(scheduler.getId(), run);
		scheduler.submitVmList(vmlist);
	}

	public static Workflow buildWorkflow(AbstractWorkflowScheduler scheduler) {
		switch (SimulationParameters.experiment) {
		case MONTAGE_TRACE_1:
			return new MontageTraceFileReader().parseLogFile(scheduler.getId(), "examples/montage.m17.1.trace", true, true, ".*jpg");
		case MONTAGE_TRACE_12:
			return new MontageTraceFileReader().parseLogFile(scheduler.getId(), "examples/montage.m17.12.trace", true, true, ".*jpg");
		case ALIGNMENT_TRACE:
			return new AlignmentTraceFileReader().parseLogFile(scheduler.getId(), "examples/alignment.caco.geo.chr22.trace2", true, true, null);
		case MONTAGE_25:
			return new DaxFileReader().parseLogFile(scheduler.getId(), "examples/Montage_25.xml", true, true, null);
		case MONTAGE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(), "examples/Montage_1000.xml", true, true, null);
		case CYBERSHAKE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(), "examples/CyberShake_1000.xml", true, true, null);
		case EPIGENOMICS_997:
			return new DaxFileReader().parseLogFile(scheduler.getId(), "examples/Epigenomics_997.xml", true, true, null);
		case CUNEIFORM_VARIANT_CALL:
			return new CuneiformLogFileReader().parseLogFile(scheduler.getId(), "examples/i1_s11756_r7_greedyQueue.log", true, true, null);
		case HETEROGENEOUS_TEST_WORKFLOW:
			return new CuneiformLogFileReader().parseLogFile(scheduler.getId(), "examples/heterogeneous_test_workflow.log", true, true, null);
		default:
		}
		return null;
	}

	public static void submitWorkflow(Workflow workflow, AbstractWorkflowScheduler scheduler) {
		// Create Cloudlets and send them to Scheduler
		if (SimulationParameters.outputWorkflowGraph) {
			workflow.visualize(1920, 1200);
		}
		scheduler.submitWorkflow(workflow);
	}

	// all numbers in 1000 (e.g. kb/s)
	public static Datacenter createDatacenter(String name) {
		Random numGen;
		numGen = SimulationParameters.numGen;
		List<DynamicHost> hostList = new ArrayList<>();
		int hostId = 0;
		long storage = 1024 * 1024;

		int ram = 2 * 1024 * SimulationParameters.nCusPerCoreOpteron270 * SimulationParameters.nCoresOpteron270;
		for (int i = 0; i < SimulationParameters.nOpteron270; i++) {
			double mean = 1d;
			double dev = SimulationParameters.bwHeterogeneityCV;
			ContinuousDistribution dist = SimulationParameters.getDistribution(SimulationParameters.bwHeterogeneityDistribution, mean, SimulationParameters.bwHeterogeneityAlpha,
			    SimulationParameters.bwHeterogeneityBeta, dev, SimulationParameters.bwHeterogeneityShape, SimulationParameters.bwHeterogeneityLocation, SimulationParameters.bwHeterogeneityShift,
			    SimulationParameters.bwHeterogeneityMin, SimulationParameters.bwHeterogeneityMax, SimulationParameters.bwHeterogeneityPopulation);
			long bwps = 0;
			while (bwps <= 0) {
				bwps = (long) (dist.sample() * SimulationParameters.bwpsPerPe);
			}
			mean = 1d;
			dev = SimulationParameters.ioHeterogeneityCV;
			dist = SimulationParameters.getDistribution(SimulationParameters.ioHeterogeneityDistribution, mean, SimulationParameters.ioHeterogeneityAlpha, SimulationParameters.ioHeterogeneityBeta, dev,
			    SimulationParameters.ioHeterogeneityShape, SimulationParameters.ioHeterogeneityLocation, SimulationParameters.ioHeterogeneityShift, SimulationParameters.ioHeterogeneityMin,
			    SimulationParameters.ioHeterogeneityMax, SimulationParameters.ioHeterogeneityPopulation);
			long iops = 0;
			while (iops <= 0) {
				iops = (long) (dist.sample() * SimulationParameters.iopsPerPe);
			}
			mean = 1d;
			dev = SimulationParameters.cpuHeterogeneityCV;
			dist = SimulationParameters.getDistribution(SimulationParameters.cpuHeterogeneityDistribution, mean, SimulationParameters.cpuHeterogeneityAlpha, SimulationParameters.cpuHeterogeneityBeta, dev,
			    SimulationParameters.cpuHeterogeneityShape, SimulationParameters.cpuHeterogeneityLocation, SimulationParameters.cpuHeterogeneityShift, SimulationParameters.cpuHeterogeneityMin,
			    SimulationParameters.cpuHeterogeneityMax, SimulationParameters.cpuHeterogeneityPopulation);
			long mips = 0;
			while (mips <= 0) {
				mips = (long) (dist.sample() * SimulationParameters.mipsPerCoreOpteron270);
			}
			if (numGen.nextDouble() < SimulationParameters.likelihoodOfStraggler) {
				bwps *= SimulationParameters.stragglerPerformanceCoefficient;
				iops *= SimulationParameters.stragglerPerformanceCoefficient;
				mips *= SimulationParameters.stragglerPerformanceCoefficient;
			}
			hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage, SimulationParameters.nCusPerCoreOpteron270, SimulationParameters.nCoresOpteron270, mips));
		}

		ram = 2 * 1024 * SimulationParameters.nCusPerCoreOpteron2218 * SimulationParameters.nCoresOpteron2218;
		for (int i = 0; i < SimulationParameters.nOpteron2218; i++) {
			double mean = 1d;
			double dev = SimulationParameters.bwHeterogeneityCV;
			ContinuousDistribution dist = SimulationParameters.getDistribution(SimulationParameters.bwHeterogeneityDistribution, mean, SimulationParameters.bwHeterogeneityAlpha,
			    SimulationParameters.bwHeterogeneityBeta, dev, SimulationParameters.bwHeterogeneityShape, SimulationParameters.bwHeterogeneityLocation, SimulationParameters.bwHeterogeneityShift,
			    SimulationParameters.bwHeterogeneityMin, SimulationParameters.bwHeterogeneityMax, SimulationParameters.bwHeterogeneityPopulation);
			long bwps = 0;
			while (bwps <= 0) {
				bwps = (long) (dist.sample() * SimulationParameters.bwpsPerPe);
			}
			mean = 1d;
			dev = SimulationParameters.ioHeterogeneityCV;
			dist = SimulationParameters.getDistribution(SimulationParameters.ioHeterogeneityDistribution, mean, SimulationParameters.ioHeterogeneityAlpha, SimulationParameters.ioHeterogeneityBeta, dev,
			    SimulationParameters.ioHeterogeneityShape, SimulationParameters.ioHeterogeneityLocation, SimulationParameters.ioHeterogeneityShift, SimulationParameters.ioHeterogeneityMin,
			    SimulationParameters.ioHeterogeneityMax, SimulationParameters.ioHeterogeneityPopulation);
			long iops = 0;
			while (iops <= 0) {
				iops = (long) (dist.sample() * SimulationParameters.iopsPerPe);
			}
			mean = 1d;
			dev = SimulationParameters.cpuHeterogeneityCV;
			dist = SimulationParameters.getDistribution(SimulationParameters.cpuHeterogeneityDistribution, mean, SimulationParameters.cpuHeterogeneityAlpha, SimulationParameters.cpuHeterogeneityBeta, dev,
			    SimulationParameters.cpuHeterogeneityShape, SimulationParameters.cpuHeterogeneityLocation, SimulationParameters.cpuHeterogeneityShift, SimulationParameters.cpuHeterogeneityMin,
			    SimulationParameters.cpuHeterogeneityMax, SimulationParameters.cpuHeterogeneityPopulation);
			long mips = 0;
			while (mips <= 0) {
				mips = (long) (dist.sample() * SimulationParameters.mipsPerCoreOpteron2218);
			}
			if (numGen.nextDouble() < SimulationParameters.likelihoodOfStraggler) {
				bwps *= SimulationParameters.stragglerPerformanceCoefficient;
				iops *= SimulationParameters.stragglerPerformanceCoefficient;
				mips *= SimulationParameters.stragglerPerformanceCoefficient;
			}
			hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage, SimulationParameters.nCusPerCoreOpteron2218, SimulationParameters.nCoresOpteron2218, mips));
		}

		ram = 2 * 1024 * SimulationParameters.nCusPerCoreXeonE5430 * SimulationParameters.nCoresXeonE5430;
		for (int i = 0; i < SimulationParameters.nXeonE5430; i++) {
			double mean = 1d;
			double dev = SimulationParameters.bwHeterogeneityCV;
			ContinuousDistribution dist = SimulationParameters.getDistribution(SimulationParameters.bwHeterogeneityDistribution, mean, SimulationParameters.bwHeterogeneityAlpha,
			    SimulationParameters.bwHeterogeneityBeta, dev, SimulationParameters.bwHeterogeneityShape, SimulationParameters.bwHeterogeneityLocation, SimulationParameters.bwHeterogeneityShift,
			    SimulationParameters.bwHeterogeneityMin, SimulationParameters.bwHeterogeneityMax, SimulationParameters.bwHeterogeneityPopulation);
			long bwps = 0;
			while (bwps <= 0) {
				bwps = (long) (dist.sample() * SimulationParameters.bwpsPerPe);
			}
			mean = 1d;
			dev = SimulationParameters.ioHeterogeneityCV;
			dist = SimulationParameters.getDistribution(SimulationParameters.ioHeterogeneityDistribution, mean, SimulationParameters.ioHeterogeneityAlpha, SimulationParameters.ioHeterogeneityBeta, dev,
			    SimulationParameters.ioHeterogeneityShape, SimulationParameters.ioHeterogeneityLocation, SimulationParameters.ioHeterogeneityShift, SimulationParameters.ioHeterogeneityMin,
			    SimulationParameters.ioHeterogeneityMax, SimulationParameters.ioHeterogeneityPopulation);
			long iops = 0;
			while (iops <= 0) {
				iops = (long) (dist.sample() * SimulationParameters.iopsPerPe);
			}
			mean = 1d;
			dev = SimulationParameters.cpuHeterogeneityCV;
			dist = SimulationParameters.getDistribution(SimulationParameters.cpuHeterogeneityDistribution, mean, SimulationParameters.cpuHeterogeneityAlpha, SimulationParameters.cpuHeterogeneityBeta, dev,
			    SimulationParameters.cpuHeterogeneityShape, SimulationParameters.cpuHeterogeneityLocation, SimulationParameters.cpuHeterogeneityShift, SimulationParameters.cpuHeterogeneityMin,
			    SimulationParameters.cpuHeterogeneityMax, SimulationParameters.cpuHeterogeneityPopulation);
			long mips = 0;
			while (mips <= 0) {
				mips = (long) (dist.sample() * SimulationParameters.mipsPerCoreXeonE5430);
			}
			if (numGen.nextDouble() < SimulationParameters.likelihoodOfStraggler) {
				bwps *= SimulationParameters.stragglerPerformanceCoefficient;
				iops *= SimulationParameters.stragglerPerformanceCoefficient;
				mips *= SimulationParameters.stragglerPerformanceCoefficient;
			}
			hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage, SimulationParameters.nCusPerCoreXeonE5430, SimulationParameters.nCoresXeonE5430, mips));
		}

		String arch = "x86";
		String os = "Linux";
		String vmm = "Xen";
		double time_zone = 10.0;
		double cost = 3.0;
		double costPerMem = 0.05;
		double costPerStorage = 0.001;
		double costPerBw = 0.0;
		LinkedList<Storage> storageList = new LinkedList<>();

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicyRandom(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	public static List<Vm> createVMList(int userId, int run) {

		// Creates a container to store VMs. This list is passed to the broker
		// later
		LinkedList<Vm> list = new LinkedList<>();

		// VM Parameters
		long storage = 10000;
		String vmm = "Xen";

		// create VMs
		Vm[] vm = new DynamicVm[SimulationParameters.nVms];

		for (int i = 0; i < SimulationParameters.nVms; i++) {
			DynamicModel dynamicModel = new DynamicModel();
			vm[i] = new DynamicVm(i, userId, SimulationParameters.numberOfCusPerPe, SimulationParameters.numberOfPes, SimulationParameters.ram, storage, vmm, new CloudletSchedulerGreedyDivided(),
			    dynamicModel, "output/run_" + run + "_vm_" + i + ".csv", SimulationParameters.taskSlotsPerVm);
			list.add(vm[i]);
		}

		return list;
	}
}
