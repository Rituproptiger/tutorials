package org.baeldung.taskletsvschunks.config;

import org.baeldung.taskletsvschunks.tasklets.LinesProcessor;
import org.baeldung.taskletsvschunks.tasklets.LinesProcessor2;
import org.baeldung.taskletsvschunks.tasklets.LinesReader;
import org.baeldung.taskletsvschunks.tasklets.LinesWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class TaskletsConfig {

    @Autowired private JobBuilderFactory jobs;

    @Autowired private StepBuilderFactory steps;

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }

    @Bean
    public JobRepository jobRepository() throws Exception {
        MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean();
        factory.setTransactionManager(transactionManager());
        return (JobRepository) factory.getObject();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }
    
    @Bean
	public ThreadPoolTaskExecutor jobTaskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(2);
		taskExecutor.setMaxPoolSize(4);
		taskExecutor.setQueueCapacity(10000);
		return taskExecutor;
	}

    @Bean
    public JobLauncher jobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setTaskExecutor(jobTaskExecutor());
        jobLauncher.setJobRepository(jobRepository());
        return jobLauncher;
    }

    @Bean
    public LinesReader linesReader() {
        return new LinesReader();
    }

    @Bean
    public LinesProcessor linesProcessor() {
        return new LinesProcessor();
    }
    
    @Bean
    public LinesProcessor2 linesProcessor2() {
        return new LinesProcessor2();
    }

    @Bean
    public LinesWriter linesWriter() {
        return new LinesWriter();
    }

    @Bean
    protected Step readLines() {
        return steps
          .get("readLines")
          .tasklet(linesReader())
          .build();
    }

    @Bean
    protected Step processLines() {
        return steps
          .get("processLines")
          .tasklet(linesProcessor())
          .build();
    }

    @Bean
    protected Step writeLines() {
        return steps
          .get("writeLines")
          .tasklet(linesWriter())
          .build();
    }

    @Bean(name = "taskletsJob")
    public Job job() {
    	Flow masterFlow = new FlowBuilder<SimpleFlow>("masterFlow").start(readLines()).build();
        return (jobs
          .get("taskletsJob").incrementer(new RunIdIncrementer())
          .start(masterFlow).next(splitFlow())
//          .split(taskExecutor()).add(flow1(), flow2())
          .next(writeLines())
          .build()).build();
//        return jobs
//                .get("taskletsJob")
//                .start(readLines())
//                .next(processLines())
//                .next(writeLines())
//                .build();
    }
    
    @Bean
    public Flow splitFlow() {
        return new FlowBuilder<SimpleFlow>("splitFlow")
            .split(taskExecutor())
            .add(flow1(), flow2())
            .build();
    }

    @Bean
    public Flow flow1() {
        return new FlowBuilder<SimpleFlow>("flow1")
            .start(processLines1())
            .build();
    }

    @Bean
    public Flow flow2() {
        return new FlowBuilder<SimpleFlow>("flow2")
            .start(processLines2())
            .build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        return new SimpleAsyncTaskExecutor("spring_batch");
    }
    
    @Bean
    protected Step processLines1() {
        return steps
          .get("processLines1")
          .tasklet(linesProcessor())
          .build();
    }
    
    @Bean
    protected Step processLines2() {
        return steps
          .get("processLines2")
          .tasklet(linesProcessor2())
          .build();
    }
    
}
