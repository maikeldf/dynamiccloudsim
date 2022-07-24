package de.huberlin.wbi.dcs;

import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import de.huberlin.wbi.dcs.provisioners.BwProvisionerFull;
import org.cloudbus.cloudsim.lists.VmList;

public class DynamicHost extends Host {

	/** The I/O capacity of this host (in byte per second). */
	private long io;

	/** The amount of compute units this host provides per Pe. */
	private double numberOfCusPerPe;

	private double mipsPerPe;

	private static long totalMi;
	private static long totalIo;
	private static long totalBw;

	private Set<File> localFiles;

	private List<Double> listCpu;

	public DynamicHost(int id, int ram, long bandwidth, long io, long storage, double numberOfCusPerPe, int numberOfPes, double mipsPerPe) {
		super(id, new RamProvisionerSimple(ram), new BwProvisionerFull(bandwidth), storage, new ArrayList<Pe>(), null);
		setIo(io);
		setMipsPerPe(mipsPerPe);
		setNumberOfCusPerPe(numberOfCusPerPe);
		List<Pe> peList = new ArrayList<>();
		for (int i = 0; i < numberOfPes; i++) {
			peList.add(new Pe(i, new PeProvisionerSimple(mipsPerPe)));
		}
		setPeList(peList);
		setVmScheduler(new VmSchedulerTimeShared(peList));
		setFailed(false);
		localFiles = new HashSet<>();
		listCpu = new ArrayList<>();
	}

	@Override
	public boolean vmCreate(Vm vm) {
		if (vm instanceof DynamicVm) {
			DynamicVm dVm = (DynamicVm) vm;
			dVm.setMips((dVm.getNumberOfCusPerPe() / getNumberOfCusPerPe()) * getMipsPerPe());
			dVm.setBw(getBw());
			dVm.setIo(getIo());
			totalMi += dVm.getMips();
			totalIo += dVm.getIo();
			totalBw += dVm.getBw();
		}
		return super.vmCreate(vm);
	}

	public static long getTotalBw() {
		return totalBw;
	}

	public static long getTotalIo() {
		return totalIo;
	}

	public static long getTotalMi() {
		return totalMi;
	}

	public long getIo() {
		return io;
	}

	public double getMipsPerPe() {
		return mipsPerPe;
	}

	public double getNumberOfCusPerPe() {
		return numberOfCusPerPe;
	}

	public void setIo(long io) {
		this.io = io;
	}

	public void setMipsPerPe(double mipsPerPe) {
		this.mipsPerPe = mipsPerPe;
	}

	public void setNumberOfCusPerPe(double numberOfCusPerPe) {
		this.numberOfCusPerPe = numberOfCusPerPe;
	}

	public void addFile(File file) {
		localFiles.add(file);
	}

	public boolean containsFile(File file) {
		return localFiles.contains(file);
	}

	public double getUtilizationOfRam() {
		return getRamProvisioner().getUsedRam();
	}

	@Override
	public double updateVmsProcessing(double currentTime) {
		double smallerTime = super.updateVmsProcessing(currentTime);

		DynamicVm dVm = null;
		if (getVmList().size() > 0)
			dVm = (DynamicVm)getVmList().get(0);

		/*
		* When it is a non-degrading Vm, the CPU is randomly set by increasing/decreasing
		* a bit, emulating a normal task process. If the Vm is flagged as degrading,
		* CPU performance will increase by 10% over time.
		 */
		if (dVm != null) {
			if (dVm.getCpu() < 100) {
				Log.formatLine("VM: #%d CPU: %.2f Performance degrading: %b", dVm.getId(), dVm.getCpu(), dVm.getDegrading());
				if (dVm.getDegrading()) {
					dVm.setCpu(dVm.getCpu() / 0.9);
				} else {
					long seed = System.currentTimeMillis();
					Random numGen = new Random(seed);
					Double factor = (float) (numGen.nextInt(10)*0.01) + 0.9;

					if (Math.abs(numGen.nextInt()) % 2 == 0) {
						dVm.setCpu(dVm.getCpu() / factor);
					} else {
						dVm.setCpu(dVm.getCpu() * factor);
					}
				}
			}
		}
		return smallerTime;
	}
}
