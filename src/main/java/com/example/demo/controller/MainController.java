package com.example.demo.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.BasicBatchConfigurer;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RestController
@RequiredArgsConstructor
public class MainController {
	@Autowired 
	JobLauncher jobLauncher;
	
	@Autowired
	@Qualifier("uploadJob")
	Job uploadJob;
	
	@Autowired
	@Qualifier("downloadJob")
	Job downloadJob;

	@Autowired
	BasicBatchConfigurer basicBatchConfigurer;
	
	@RequestMapping(value="/loadCSV", method=RequestMethod.GET)
	public BatchStatus loadCSV() 
			throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		
		Map<String, JobParameter> maps = new HashMap<>();
		maps.put("time", new JobParameter(System.currentTimeMillis()));
		maps.put("pathToFile", new JobParameter("/home/test/input.csv"));
		
		JobParameters parameters = new JobParameters(maps);
		//비동기 처리 async
		SimpleJobLauncher simpleJobLauncher = (SimpleJobLauncher) basicBatchConfigurer.getJobLauncher();
		simpleJobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        
		JobExecution jobExecution =  simpleJobLauncher.run(uploadJob, parameters);
		
		return jobExecution.getStatus();
	
	}
	
	@RequestMapping(value="/writeCSV", method=RequestMethod.GET)
	public BatchStatus writeCSV() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException{
		
		Map<String, JobParameter> maps = new HashMap<>();
		maps.put("time", new JobParameter(System.currentTimeMillis()));
		maps.put("sql", new JobParameter("select * from user"));
		maps.put("pathToFile", new JobParameter("/home/test/output.csv"));
		
		JobParameters parameters = new JobParameters(maps);
		JobExecution jobExecution =  jobLauncher.run(downloadJob, parameters);

		return jobExecution.getStatus();
	}
}
