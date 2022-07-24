package de.huberlin.wbi.dcs.workflow.scheduler;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import de.huberlin.wbi.dcs.examples.SimulationParameters;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.DataDependency;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;
import org.cloudbus.cloudsim.lists.VmList;

public abstract class AbstractWorkflowScheduler extends DatacenterBroker implements WorkflowScheduler {

	protected static Random numGen = new Random(Parameters.seed);

	protected List<Workflow> workflows;
	protected Map<Integer, Vm> availableVms;
	protected int taskSlotsPerVm;
	protected Queue<Vm> idleTaskSlots;
	protected double workflowRuntime;

	protected Map<Integer, Task> runningTasks;

	public AbstractWorkflowScheduler(String name, int taskSlotsPerVm) throws Exception {
		super(name);
		workflows = new ArrayList<>();
		availableVms = new HashMap<>();
		this.taskSlotsPerVm = taskSlotsPerVm;
		idleTaskSlots = new LinkedList<>();
		runningTasks = new HashMap<>();
	}

	public List<Workflow> getWorkflows() {
		return workflows;
	}

	public int getTaskSlotsPerVm() {
		return taskSlotsPerVm;
	}

	public void submitWorkflow(Workflow workflow) {
		workflows.add(workflow);
	}

	protected void send(String message) throws IOException {

		DatagramSocket client_socket = new DatagramSocket();

		InetAddress IPAddress =  InetAddress.getByName("127.0.0.1");

		DatagramPacket send_packet = new DatagramPacket(message.getBytes(),
				message.getBytes().length,
				IPAddress, 5000);

		client_socket.send(send_packet);
	}
	protected void submitTask(Task task, Vm vm) {
		double totalRequestedMips = 0;
		for (Vm vm_ : getVmsCreatedList()) {
			totalRequestedMips += vm_.getMips();
		}
		DynamicVm dVm = (DynamicVm) vm;
		dVm.setCpu(dVm.getMips() / totalRequestedMips * 100);
		String message = "CPU: " + dVm.getCpu();

		// Sending message to RL agent
		try {
			this.send(message);
		}
		catch (IOException e) {
			e.printStackTrace();
		}


		//Log.printLine("totalRequestedMips: "+ totalRequestedMips / getVmsCreatedList().size());

		Log.formatLine(CloudSim.clock() + ": " + getName() +
				": Mips: " + dVm.getMips()+
				" RAM: " + dVm.getRam()+
				" CPU: %.2f%%"+
				" VM #" + dVm.getId() +
				" starts executing Task # " + task.getCloudletId() + " \"" + task.getName()
		    + " " + task.getParams() + " \"",dVm.getCpu());

		task.setVmId(dVm.getId());
		//There is a terminatingDegradedVms (Datacenter)
//		if (dVm.getCpu() > 100){
//			DynamicHost host = (DynamicHost) dVm.getHost();
//			host.vmDestroy(dVm);
//			sendNow(getVmsToDatacentersMap().get(dVm.getId()), CloudSimTags.VM_DESTROY, dVm);
//			Log.formatLine(CloudSim.clock() + ": " + getName() +
//				": Destroying VM #%d "+
//				" CPU: %.2f%%" +
//				" RAM: %.2f" +
//				" Task #%d "  + " \"%d" + " \"" +
//				" HOST #%d",
//					dVm.getId(),
//					dVm.getCpu(),
//					host.getUtilizationOfRam(),
//					task.getCloudletId(),
//					task.getName(),
//					host.getId());
//
//		}
//		if (task.getCpu() < SimulationParameters.failureThreshold) {
//			task.setScheduledToFail(true);
//			task.setCloudletLength((long) (task.getCloudletLength() * Parameters.runtimeFactorInCaseOfFailure));
//
//			DynamicHost host = (DynamicHost) vm.getHost();
//			host.vmDestroy(vm);
//			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
//			Log.formatLine(CloudSim.clock() + ": " + getName() +
//					": Destroying VM #" + vm.getId() +
//					" CPU: %.2f%%" +
//					" RAM: " + host.getUtilizationOfRam() +
//					" Task # " + task.getCloudletId() + " \"" + task.getName() + " \"" +
//					" HOST #" + host.getId(), task.getCpu());
//
//			Log.printLine(CloudSim.clock() + ": " + getName() + ": Recreating VM #" + vm.getId());
//			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_CREATE, vm);
//
//			//getVmsCreatedList().remove(VmList.getById(getVmList(), vm.getId()));
//  	} else {
		//if (numGen.nextDouble() < Parameters.likelihoodOfFailure) {
		if (dVm.getId() == 1) { // Degrading VM#1
			if (numGen.nextDouble() < SimulationParameters.likelihoodOfFailure) {
				dVm.setDegrading(true);
				task.setScheduledToFail(true);
				task.setCloudletLength((long) (task.getCloudletLength() * Parameters.runtimeFactorInCaseOfFailure));
			}
		} else {
			task.setScheduledToFail(false);
		}
		sendNow(getVmsToDatacentersMap().get(dVm.getId()), CloudSimTags.CLOUDLET_SUBMIT, task);
	}

	/**
	 * Submit cloudlets to the created VMs. This function is called after Vms have been created.
	 * 
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void submitCloudlets() {
		for (Vm vm : getVmsCreatedList()) {
			availableVms.put(vm.getId(), vm);
			for (int i = 0; i < getTaskSlotsPerVm(); i++) {
				idleTaskSlots.add(vm);
			}
		}
		for (Workflow workflow : workflows) {
			Collection<Task> tasks = workflow.getTasks();
			reschedule(tasks, availableVms.values());
			for (Task task : tasks) {
				if (task.readyToExecute()) {
					taskReady(task);
				}
			}
		}
		submitTasks();
	}

	protected void submitTasks() {
		Queue<Vm> taskSlotsKeptIdle = new LinkedList<>();
		while (tasksRemaining() && !idleTaskSlots.isEmpty()) {
			Vm vm = idleTaskSlots.remove();
			Task task = getNextTask(vm);
			// task will be null if scheduler has tasks to be executed, but not
			// for this VM (e.g., if it abides to a static schedule or this VM
			// is a straggler, which the scheduler does not want to assign tasks
			// to)
			if (task == null) {
				taskSlotsKeptIdle.add(vm);
			} else {
				runningTasks.put(task.getCloudletId(), task);
				submitTask(task, vm);
			}
		}
		idleTaskSlots.addAll(taskSlotsKeptIdle);
	}

	@Override
	protected void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			if (vm instanceof DynamicVm) {
				DynamicVm dVm = (DynamicVm) vm;
				dVm.closePerformanceLog();
			}
		}
		super.clearDatacenters();
		workflowRuntime = CloudSim.clock();
	}

	public double getRuntime() {
		return workflowRuntime;
	}

	protected static void resetTask(Task task) {
		task.setCloudletFinishedSoFar(0);
		try {
			task.setCloudletStatus(Cloudlet.CREATED);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// determine what kind of task was finished,
		Task task = (Task) ev.getData();
		Vm vm = availableVms.get(task.getVmId());
		Host host = vm.getHost();

		if (task.getCloudletStatus() == Cloudlet.SUCCESS) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + task.getVmId() + " completed Task # " + task.getCloudletId() + " \"" + task.getName()
			    + " " + task.getParams() + " \"");

			// free task slots occupied by finished / cancelled tasks
			idleTaskSlots.add(vm);
			taskSucceeded(task, vm);

			// update the task queue by traversing the successor nodes in the
			// workflow
			for (DataDependency outgoingEdge : task.getWorkflow().getGraph().getOutEdges(task)) {
				if (host instanceof DynamicHost) {
					DynamicHost dHost = (DynamicHost) host;
					dHost.addFile(outgoingEdge.getFile());
				}
				Task child = task.getWorkflow().getGraph().getDest(outgoingEdge);
				child.decNDataDependencies();
				if (child.readyToExecute()) {
					taskReady(child);
				}
			}

		} else {

			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + task.getVmId() + " encountered an error with Task # " + task.getCloudletId() + " \""
			    + task.getName() + " " + task.getParams() + " \"");
			runningTasks.remove(task.getCloudletId());

			if (!task.isSpeculativeCopy()) {
				resetTask(task);
				taskReady(task);
			}

			idleTaskSlots.add(vm);
			taskFailed(task, vm);
		}

		if (tasksRemaining()) {
			submitTasks();
		} else if (signalFinished() || (idleTaskSlots.size() == getVmsCreatedList().size() * getTaskSlotsPerVm())) {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All Tasks executed. Finishing...");
			terminate();
			clearDatacenters();
			finishExecution();
		}
	}

	@Override
	public boolean signalFinished() {
		return false;
	}

}
