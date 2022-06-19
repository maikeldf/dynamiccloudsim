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

	private List<Double> entry;

	//private static int count = 0;

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
		entry = new ArrayList<>();
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
		listCpu.addAll(fillEntry(1));
		//listCpu.addAll(fillEntry(2));

		if (listCpu.size() == 4) {
			Collections.sort(listCpu);
			//String str = String.format("#"+count+",%.2f,%.2f,%.2f",currentTime,listCpu.get(3),listCpu.get(0));
			//Log.printLine(str);
			//count++;
			entry.add(listCpu.get(3));
			entry.add(listCpu.get(0));
			listCpu.clear();
		}
//		for (Vm vm : getVmList()) {
//			if (vm.getId() == 1) {
//
//			}
//
//		}
		//Log.printLine("updateVmsProcessing: "+currentTime);
		return smallerTime;
	}

	private List<Double> fillEntry(int id) {
		DynamicVm dVm = (DynamicVm)(VmList.getById(getVmList(), id));
		List<Double> l = new ArrayList<>();
		if (dVm != null) {
			//if (dVm.getDegrading()) {
				if (dVm.getCpu() < 100) {
					Log.formatLine("VM #%d CPU %.2f", dVm.getId(), dVm.getCpu());
					dVm.setCpu(dVm.getCpu() / 0.9);
					l.add(dVm.getCpu());
				}
			//}
		}

		return l;
	}

	public List<Double> getEntry() {
		return entry;
	}
}
