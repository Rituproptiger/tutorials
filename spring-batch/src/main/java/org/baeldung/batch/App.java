package org.baeldung.batch;

import org.baeldung.taskletsvschunks.config.TaskletsConfig;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class App {
    public static void main(final String[] args) {
        // Spring Java config
        final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
//        context.register(SpringConfig.class);
        context.register(TaskletsConfig.class);
//        context.register(SpringBatchConfig.class);
        context.refresh();

        // Spring xml config
        // ApplicationContext context = new ClassPathXmlApplicationContext("spring-batch.xml");

        final JobLauncher jobLauncher = (JobLauncher) context.getBean("jobLauncher");
        final Job job = (Job) context.getBean("taskletsJob");
        System.out.println("Starting the batch job");
        try {
        	JobParametersBuilder jobBuilder= new JobParametersBuilder();
            jobBuilder.addLong("NumLines", 1L);
            JobParameters jobParameters =jobBuilder.toJobParameters();
            
            JobParametersBuilder jobBuilder1= new JobParametersBuilder();
            jobBuilder1.addLong("NumLines", 2L);
            JobParameters jobParameters1 =jobBuilder1.toJobParameters();
            
            final JobExecution execution = jobLauncher.run(job, jobParameters);
            final JobExecution execution1 = jobLauncher.run(job, jobParameters1);
            System.out.println("Job Status : " + execution.getStatus());
            System.out.println("Job Status 1 : " + execution1.getStatus());
            System.out.println("Job succeeded");
        } catch (final Exception e) {
            e.printStackTrace();
            System.out.println("Job failed");
        }
    }
}