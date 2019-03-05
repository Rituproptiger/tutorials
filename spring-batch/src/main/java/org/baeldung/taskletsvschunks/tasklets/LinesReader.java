package org.baeldung.taskletsvschunks.tasklets;

import org.baeldung.taskletsvschunks.model.Line;
import org.baeldung.taskletsvschunks.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LinesReader implements Tasklet, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesReader.class);

    private List<Line> lines;
    private FileUtils fu;
    Long numLines = 0L;

    @Override
	public void beforeStep(StepExecution stepExecution) {
		lines = new ArrayList<Line>();
		fu = new FileUtils("taskletsvschunks/input/tasklets-vs-chunks.csv");
		logger.debug("Lines Reader initialized.");
		JobParameters jobParameters = stepExecution.getJobExecution().getJobParameters();
		Map<String, JobParameter> parameters = jobParameters.getParameters();
		JobParameter jobParameter = parameters.get("NumLines");
		numLines = (Long)jobParameter.getValue();
	}

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Line line = fu.readLine();
        long i=0;
        while (line != null && i<numLines) {
            lines.add(line);
            logger.debug("Read line: " + line.toString());
            line = fu.readLine();
            i++;
        }
        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        fu.closeReader();
        stepExecution
          .getJobExecution()
          .getExecutionContext()
          .put("lines", this.lines);
        logger.debug("Lines Reader ended.");
        return ExitStatus.COMPLETED;
    }
}
