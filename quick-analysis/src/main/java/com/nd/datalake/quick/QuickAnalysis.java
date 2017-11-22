package com.nd.datalake.quick;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.jdbc.core.JdbcTemplate;

public class QuickAnalysis {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	private int nightStartTime = 20;
	private int nightEndTime = 6;

	public void analyse() throws IOException {
		String cell_master = FileUtils.readFileToString(new File(
				"/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/db/create_cell_site_master.sql"),
				"UTF-8");
		String suspect = FileUtils.readFileToString(new File(
				"/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/db/create_suspect_list.sql"),
				"UTF-8");
		String loadData = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/db/load_data.sql"),
				"UTF-8");
		String dropTables = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/db/drop_tables.sql"),
				"UTF-8");

		String sql2 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/frequent_calls.sql"),
				"UTF-8");
		String sql3 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/full_data.sql"),
				"UTF-8");
		String sql4 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/imei_used.sql"),
				"UTF-8");
		String sql5 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/imsi_used.sql"),
				"UTF-8");
		String sql6 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/long_call.sql"),
				"UTF-8");
		String sql7 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/night_call.sql"),
				"UTF-8");
		String sql8 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/short_call.sql"),
				"UTF-8");
		String sql9 = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/suspect_call.sql"),
				"UTF-8");

		long l = System.currentTimeMillis();
		jdbcTemplate.execute(cell_master);
		jdbcTemplate.execute(suspect);
		jdbcTemplate.execute(loadData);

		jdbcTemplate.execute(sql2);
		jdbcTemplate.execute(sql3);
		jdbcTemplate.execute(sql4);
		jdbcTemplate.execute(sql5);
		jdbcTemplate.execute(sql6);
		jdbcTemplate.execute(sql7);
		jdbcTemplate.execute(sql8);
		jdbcTemplate.execute(sql9);
		jdbcTemplate.execute(dropTables);
		System.out.println("Done:" + (System.currentTimeMillis() - l));
		// jdbcTemplate.qu
	}

	public static void main(String[] args) {
		SpringApplication sa = new SpringApplication(Context.class);
		sa.setWebEnvironment(false);
		sa.run();
		System.out.println("Done");
	}
}
