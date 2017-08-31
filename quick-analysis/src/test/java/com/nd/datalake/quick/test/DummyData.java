package com.nd.datalake.quick.test;

import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Test;

public class DummyData {
	private static final int MAX = 60000;
	private static final int SUSPECT_MAX = 2000;

	@Test
	public void createCellSiteMaster() throws IOException {
		String areaCode = "40419-1000-";
		String location = "TVM-";
		PrintWriter pw = new PrintWriter("cell-site-master.csv");
		for (int i = 10000; i <= MAX; i++) {
			pw.println(areaCode + i + "," + location + i);
		}
		pw.close();
	}

	@Test
	public void createSuspectList() throws IOException {
		long msisdn = 9020178928l;
		String name = "Suspect-";
		PrintWriter pw = new PrintWriter("suspectList.csv");
		for (int i = 0; i <= SUSPECT_MAX; i++) {
			pw.println(msisdn + i + "," + name + i);
		}
		pw.close();
	}
}
