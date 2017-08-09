package com.nd.datalake.quick;

import java.io.File;
import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.jdbc.core.JdbcTemplate;

public class QuickAnalysis {
	@Autowired
	private JdbcTemplate jdbcTemplate;

	@PostConstruct
	public void analyse() throws IOException {
		long l = System.currentTimeMillis();
		String sql = FileUtils.readFileToString(
				new File("/home/krish/eclipse-workspace/datalake/stream-sink/src/main/resources/csv_read_idea.sql"),
				"UTF-8");
		jdbcTemplate.execute(sql);
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
