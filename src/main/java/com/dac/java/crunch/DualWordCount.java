package com.dac.java.crunch;

import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.dist.collect.EmptyPTable;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.types.PTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.crunch.types.writable.Writables.*;

/**
 * @author <a href="mailto:dan.andrei.carp@gmail.com">Dan Andrei Carp</a>
 */
public class DualWordCount extends Configured implements Tool, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DualWordCount.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new DualWordCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: hadoop jar crunch-demo-1.0-SNAPSHOT-job.jar"
                    + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String[] inputPaths = new String[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            inputPaths[i] = args[i];
            System.err.println("in: " + args[i]);
        }

        String outputPath = args[args.length - 1];
        System.err.println("out: " + outputPath);

        boolean success = computeCommonWordCount(inputPaths, outputPath);
        boolean altSuccess = altComputeCommonWordCount(inputPaths, "alt-" + outputPath);

        return success == altSuccess ? 0 : 1;
    }

    private boolean computeCommonWordCount(String[] inputPaths, String outputPath) {
        MRPipeline pipeline = new MRPipeline(DualWordCount.class, getConf());
        PTable<String, Long> allWordCounts = new EmptyPTable<String, Long>(pipeline, tableOf(strings(), longs()));
        boolean firstRun = true;

        for (String path : inputPaths) {
            PTable<String, Long> wordCounts = computeWordCounts(pipeline, path);

            JoinStrategy<String, Long, Long> strategy = new DefaultJoinStrategy<String, Long, Long>();
            JoinType joinType = JoinType.INNER_JOIN;
            if (firstRun) {
                joinType = JoinType.RIGHT_OUTER_JOIN;
                firstRun = false;
            }
            allWordCounts = strategy.join(allWordCounts, wordCounts, joinType).parallelDo(new DoFn<Pair<String, Pair<Long, Long>>, Pair<String, Long>>() {
                @Override
                public void process(Pair<String, Pair<Long, Long>> input, Emitter<Pair<String, Long>> emitter) {
                    Long leftCount = input.second().first() != null ? input.second().first() : 0;
                    Long rghtCount = input.second().second() != null ? input.second().second() : 0;
                    emitter.emit(new Pair<String, Long>(input.first(), leftCount + rghtCount));
                }
            }, tableOf(strings(), longs()));
        }

        pipeline.write(allWordCounts, To.textFile(outputPath + "-allWordCounts"), Target.WriteMode.APPEND);

        return pipeline.done().succeeded();
    }

    private PTable<String, Long> computeWordCounts(Pipeline pipeline, String path) {
        PCollection<String> lines = pipeline.readTextFile(path);
        PCollection<String> words = lines.parallelDo(new DoFn<String, String>() {
            @Override
            public void process(String input, Emitter<String> emitter) {
                for (String word : input.split("\\s+")) {
                    emitter.emit(word);
                }
            }
        }, strings());
        return words.count();
    }

    private boolean altComputeCommonWordCount(String[] inputPaths, String outputPath) {
        Pipeline pipeline = new MRPipeline(DualWordCount.class, "Mapper", getConf());

        // for every input file create a word count output
        for (String path : inputPaths) {
            PTable<String, Long> wordCounts = computeWordCounts(pipeline, path);
            pipeline.write(wordCounts, To.textFile(outputPath), Target.WriteMode.APPEND);
        }
        boolean success = pipeline.done().succeeded();

        // for every word count input compute the count for common words
        if (success) {
            pipeline = new MRPipeline(DualWordCount.class, "Aggregator", getConf());
            PTable<String, Long> fileWordCounts = readWordCountsFiles(outputPath, pipeline);
            PTable<String, Long> aggWordCounts = computeCountForCommonWords(fileWordCounts);

            pipeline.write(aggWordCounts, To.textFile(outputPath + "-agg"), Target.WriteMode.OVERWRITE);
            success = pipeline.done().succeeded();
        }

        return success;
    }

    private PTable<String, Long> readWordCountsFiles(String outputPath, Pipeline pipeline) {
        PTableType<String, Long> aggWordCountsType = tableOf(strings(), longs());
        PCollection<String> fileContent = pipeline.read(From.textFile(outputPath));

        return fileContent.parallelDo(new DoFn<String, Pair<String, Long>>() {
            @Override
            public void process(String s, Emitter<Pair<String, Long>> emitter) {
                String[] line = s.split("\\t");
                emitter.emit(new Pair<String, Long>(line[0], Long.parseLong(line[1])));
            }
        }, aggWordCountsType);
    }

    private PTable<String, Long> computeCountForCommonWords(PTable<String, Long> fileWordCounts) {
        return fileWordCounts.groupByKey().combineValues(Aggregators.SUM_LONGS()).filter(new FilterFn<Pair<String, Long>>() {
            @Override
            public boolean accept(Pair<String, Long> input) {
                return input.second() > 1;
            }
        });
    }
}
