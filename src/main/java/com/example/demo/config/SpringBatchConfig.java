package com.example.demo.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.example.demo.entity.User;
import com.example.demo.service.UserService;


@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

	@Autowired
	JobBuilderFactory jobBuilderFactory;
	@Autowired
	StepBuilderFactory stepBuilderFactory;

	@Bean(name = "uploadJob")
	public Job uploadJob(
			@Qualifier("uploadItemReader")ItemReader<User> itemReader
			, @Qualifier("uploadItemProcessor")ItemProcessor<User, User> itemProcessor
			, @Qualifier("uploadItemWriter")ItemWriter<User> itemWriter			 
		) {
		Step step = stepBuilderFactory.get("ETL-file-load")
			 		.startLimit(2) //step 실패시 재시작 횟수
					.<User, User>chunk(200)
					.reader(itemReader)
					.processor(itemProcessor)
					.writer(itemWriter)
//					.faultTolerant()
					
//					.retryLimit(1) //retry 횟수, retry 사용시 필수 설정, 해당 Retry 이후 Exception시 Fail 처리
//					.retry(SQLException.class) // SQLException에 대해선 Retry 수행
//					.noRetry(NullPointerException.class) // NullPointerException에 no Retry
					
//					.skipLimit(1) // skip 허용 횟수, 해당 횟수 초과시 Error 발생, Skip 사용시 필수 설정
//	              .skip(NullPointerException.class)// NullPointerException에 대해선 Skip
//	              .noSkip(SQLException.class) // SQLException에 대해선 noSkip
					
//	              .noRollback(NullPointerException.class) // NullPointerException 발생  rollback이 되지 않게 설정

					.build();

		return jobBuilderFactory.get("ETL-Load")
					.incrementer(new RunIdIncrementer())
					.start(step)
					.build();

	}
	
	
	
	@Bean(name = "uploadItemProcessor")
	public ItemProcessor<User, User> itemProcessor(){
		return new ItemProcessor<User, User>() {
			@Override
			public User process(User userVO) throws Exception {
				//TODO 데이터 수정 및 설정
				return userVO;
			}
		};
	}
	
	@Bean(name = "uploadItemWriter")
	public ItemWriter<User> itemWriter(){
		return new ItemWriter<User>() {
			@Autowired
			UserService userService;
			@Override
			public void write(List<? extends User> items) throws Exception {
				userService.saveAll(items);
			}
		};
	}
	
	
	@Bean(name = "uploadItemReader")
	@StepScope
	public FlatFileItemReader<User> fileItemReader(
			@Value("#{jobParameters[pathToFile]}") String pathToFile
			){
		FlatFileItemReader<User> flatfileItemReader = new FlatFileItemReader<>();
		flatfileItemReader.setResource(new FileSystemResource(pathToFile));
		flatfileItemReader.setName("CSV-Reader");
		flatfileItemReader.setLinesToSkip(1); //첫줄은 헤더
		flatfileItemReader.setLineMapper(lineMapper());
		return flatfileItemReader;
	}

	@Bean
	public LineMapper<User> lineMapper() {
		DefaultLineMapper<User> defaultLineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames(new String[] {"id", "pwd"});
		
		BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(User.class);
		
		defaultLineMapper.setLineTokenizer(lineTokenizer);
		defaultLineMapper.setFieldSetMapper(fieldSetMapper);
		
		return defaultLineMapper;
	}

	/*************************************************** download ***************************************************/
	
	@Bean(name = "downloadJob")
	public Job downloadJob(
			@Qualifier("fileItemWriter") FlatFileItemWriter<User> fileItemWriter
			, @Qualifier("jdbcCursorItemReader") JdbcCursorItemReader<User> jdbcCursorItemReader
			) {
			Step step = stepBuilderFactory.get("ETL-file-down")
							.<User, User>chunk(200)
							.reader(jdbcCursorItemReader)
							.writer(fileItemWriter)
							.build();
		return jobBuilderFactory.get("ETL-Down")
			.incrementer(new RunIdIncrementer())
			.start(step)
			.build();
	}
	
	
	@Bean(name = "jdbcCursorItemReader")
	@StepScope
	public JdbcCursorItemReader<User> jdbcCursorItemReader(@Value("#{jobParameters[sql]}") String sql){
		
		JdbcCursorItemReader<User> cursorItemReader = new JdbcCursorItemReader<>();

		DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
		driverManagerDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		driverManagerDataSource.setUrl("jdbc:mysql://localhost:3306/demo?autoReconnect=true&allowMultiQueries=true");
		driverManagerDataSource.setUsername("root");
		driverManagerDataSource.setPassword("428428");
		cursorItemReader.setDataSource(driverManagerDataSource);		
		cursorItemReader.setSql(sql);
		cursorItemReader.setRowMapper(new RowMapper<User>() {
			@Override
			public User mapRow(ResultSet rs, int rowNum) throws SQLException {
				User user = new User();
				user.setId(rs.getString("id"));
				user.setPwd(rs.getString("pwd"));
				return user;
			}
			
		});
		return cursorItemReader;
	}
	
	@Bean(name = "fileItemWriter")
	@StepScope
	public FlatFileItemWriter<User> fileItemWriter(@Value("#{jobParameters[pathToFile]}") String pathToFile){
		FlatFileItemWriter<User> flatFileItemWriter = new FlatFileItemWriter<>();
		flatFileItemWriter.setResource(new FileSystemResource(pathToFile));
		DelimitedLineAggregator<User> aggregator = new DelimitedLineAggregator<>();
		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] {"id","pwd"});
		aggregator.setFieldExtractor(fieldExtractor);
		flatFileItemWriter.setLineAggregator(aggregator);
		
		return flatFileItemWriter;
	
	}

}




