package com.mikhailkarpov.springbatchdemo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@SpringBatchTest
@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
class SpringBatchDemoApplicationTests {

	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;

	@Autowired
	private JobRepositoryTestUtils jobRepositoryTestUtils;

	@BeforeEach
	void setUp() {
		jobRepositoryTestUtils.removeJobExecutions();
	}

	@Test
	void testJobExecution(CapturedOutput output) throws Exception {
		// given
		JobParameters jobParameters = jobLauncherTestUtils
				.getUniqueJobParametersBuilder()
				.addString("input.file", "/some/input/file")
				.toJobParameters();

		// when
		JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

		// then
		Assertions.assertTrue(output.getOut().contains("Processing billing information from file /some/input/file"));
		Assertions.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
	}

}
