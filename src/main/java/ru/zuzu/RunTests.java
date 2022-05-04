package ru.zuzu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.net.URI;

public class RunTests {

    public static void main(String[] args) throws Exception {

//        Logger logger = Logger.getLogger(RunAll.class);

        String Prep = args[1] + "/Preprocessing";
        String allWordsJob = args[1] + "/allWords";
        String categoryTextsJob = args[1] + "/categoryTexts";
        String jobBPath = args[1] + "/B";
        String jobB2Path = args[1] + "/B2";
        String jobAPath = args[1] + "/A";
        String jobCPath = args[1] + "/C";
        String countsPath = args[1] + "/S";
        String countsCategoriesPath = args[1] + "/categories";
        String JobSum = args[1] + "/Sum";
        String jobChiPath = args[1] + "/Chi";
        String JobTop150 = args[1] + "/Top150";
        String JobLine = args[1] + "/Line";

        //Preprocessing Job - reading in inputs file and stopwords file and filtering for desired word formats
        Configuration conf = new Configuration();
        Job getTextFromJSONJob = Job.getInstance(conf, "word count");
        getTextFromJSONJob.setNumReduceTasks(1);
        getTextFromJSONJob.setJarByClass(GetFromJSONJob.class);
        getTextFromJSONJob.setMapperClass(GetFromJSONJob.TokenizerMapperWithJson.class);
//        getTextFromJSONJob.setReducerClass(GetFromJSONJob.IntSumReducer.class);
        getTextFromJSONJob.setOutputKeyClass(NullWritable.class);
        getTextFromJSONJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(getTextFromJSONJob, new Path(args[0]));

        FileOutputFormat.setOutputPath(getTextFromJSONJob, new Path(Prep));

        //JobSum taking Input from Input

//        Job jobSum = Job.getInstance(new Configuration(), "jobSum");
//        jobSum.setJarByClass(JobSum.class);
//        jobSum.setMapperClass(JobSum.MapperSum.class);
//        jobSum.setReducerClass(JobSum.ReducerSum.class);
//        jobSum.setOutputKeyClass(TextPair.class);
//        jobSum.setOutputValueClass(TextPairLong.class);
//        jobSum.setMapOutputKeyClass(TextPair.class);
//        jobSum.setMapOutputValueClass(LongWritable.class);
//        jobSum.setInputFormatClass(TextInputFormat.class);
//        jobSum.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(jobSum, new Path(args[0]));
//        FileOutputFormat.setOutputPath(jobSum, new Path(JobSum));

        //ControlledJob controlledJobPrep = new ControlledJob(jobPreprocessing.getConfiguration());

        if (!getTextFromJSONJob.waitForCompletion(true))
            System.exit(1);
//        if (!jobSum.waitForCompletion(true))
//            System.exit(1);

        Configuration conf3 = new Configuration();
        Job getCategoriesJob = Job.getInstance(conf3, "categories");
        getCategoriesJob.setNumReduceTasks(1);
        getCategoriesJob.getConfiguration().set("mapreduce.output.basename", "result");
        getCategoriesJob.setJarByClass(GetCategoriesJob.class);
        getCategoriesJob.setMapperClass(GetCategoriesJob.TokenizerMapper.class);
        getCategoriesJob.setReducerClass(GetCategoriesJob.WordReducer.class);
        getCategoriesJob.setOutputKeyClass(Text.class);
        getCategoriesJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(getCategoriesJob, new Path(args[0]));

        FileOutputFormat.setOutputPath(getCategoriesJob, new Path(countsCategoriesPath));

        ControlledJob controlledgetCategoriesJob = new ControlledJob(getCategoriesJob.getConfiguration());


        Configuration conf2 = new Configuration();
        Job jobCount = Job.getInstance(conf2, "jobCount");
        jobCount.setNumReduceTasks(1);
        jobCount.getConfiguration().set("mapreduce.output.basename", "result2");
        jobCount.setJarByClass(CountReviewsJob.class);
        jobCount.setMapperClass(CountReviewsJob.TokenizerMapperWithJson.class);
        jobCount.setReducerClass(CountReviewsJob.IntSumReducer.class);
        jobCount.setOutputKeyClass(NullWritable.class);
        jobCount.setOutputValueClass(Text.class);
//        jobCount.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobCount, new Path(args[0]));

        FileOutputFormat.setOutputPath(jobCount, new Path(countsPath));

        ControlledJob controlledjobCount = new ControlledJob(jobCount.getConfiguration());

        // JobA taking Input from Preprocessing

        Job filteringJob = Job.getInstance(new Configuration(), "jobA123");
        filteringJob.setNumReduceTasks(1);
        filteringJob.setJarByClass(FilteringJob.class);
        filteringJob.setMapperClass(FilteringJob.TokenizerMapper.class);
//        jobA.setCombinerClass(WordCount.IntSumReducer.class);
        filteringJob.setReducerClass(FilteringJob.WordReducer.class);
        filteringJob.setOutputKeyClass(NullWritable.class);
        filteringJob.setOutputValueClass(Text.class);
        filteringJob.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(filteringJob, new Path(Prep));
        FileOutputFormat.setOutputPath(filteringJob, new Path(allWordsJob));


//        Job jobA = Job.getInstance(new Configuration(), "jobA");
//        jobA.setJarByClass(WordCount.class);
////        jobA.setSortComparatorClass(WordCount.Comparator.class);
//        jobA.setMapOutputKeyClass(TextPair.class);
//        jobA.setMapOutputValueClass(TextPair.class);
//        jobA.setOutputKeyClass(TextPair.class);
//        jobA.setOutputValueClass(LongWritable.class);
//        jobA.setMapperClass(WordCount.TokenizerMapper.class);
//        jobA.setReducerClass(WordCount.IntSumReducer.class);
//        jobA.setInputFormatClass(SequenceFileInputFormat.class);
//        jobA.setOutputFormatClass(SequenceFileOutputFormat.class);
//        SequenceFileInputFormat.setInputPaths(jobA, new Path(Prep));
//        SequenceFileOutputFormat.setOutputPath(jobA, new Path(JobA));

        ControlledJob controlledFilteringJob = new ControlledJob(filteringJob.getConfiguration());

        Job categotyTextsJob = Job.getInstance(new Configuration(), "jobCategoryTexts");
        categotyTextsJob.setNumReduceTasks(1);
        categotyTextsJob.setJarByClass(CategoryTextsJob.class);
        categotyTextsJob.setMapperClass(CategoryTextsJob.CategoryTextsJobMapper.class);
        categotyTextsJob.setReducerClass(CategoryTextsJob.CategoryTextsJobReducer.class);
//        categotyTextsJob.setInputFormatClass(SequenceFileInputFormat.class);
        categotyTextsJob.setOutputFormatClass(SequenceFileOutputFormat.class);
//        filteringJob.setReducerClass(FilteringJob.IntSumReducer.class);
        categotyTextsJob.setOutputKeyClass(Text.class);
        categotyTextsJob.setOutputValueClass(CategoryWritable.class);
        categotyTextsJob.setMapOutputKeyClass(Text.class);
        categotyTextsJob.setMapOutputValueClass(CategoryWritable.class);
//        categotyTextsJob.setMapOutputKeyClass(Text.class);
//        categotyTextsJob.setMapOutputValueClass(CategoryWritable.class);
        FileInputFormat.addInputPath(categotyTextsJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(categotyTextsJob, new Path(categoryTextsJob));

        ControlledJob controlledcategotyTextsJob = new ControlledJob(categotyTextsJob.getConfiguration());
//
//        Job chiTestJob = Job.getInstance(new Configuration(), "jobCategoryTexts");
//        chiTestJob.setNumReduceTasks(1);
//        chiTestJob.setJarByClass(ChiTestJob.class);
//        chiTestJob.setMapperClass(ChiTestJob.ChiJobMapper.class);
//        chiTestJob.setReducerClass(ChiTestJob.ChiJobReducer.class);
////        filteringJob.setReducerClass(FilteringJob.IntSumReducer.class);
//        chiTestJob.addCacheFile(new Path(allWordsJob + "/part-r-00000").toUri());
//        chiTestJob.setOutputKeyClass(Text.class);
//        chiTestJob.setOutputValueClass(CategoryWritable.class);
//        FileInputFormat.addInputPath(chiTestJob, new Path(args[0]));
//        FileOutputFormat.setOutputPath(chiTestJob, new Path(categoryTextsJob));
//
//        ControlledJob controlledChiTestJob = new ControlledJob(chiTestJob.getConfiguration());

        //JobB taking Input from JobA

        Job jobA = Job.getInstance(new Configuration(), "jobA");
        jobA.setJarByClass(JobA.class);
        jobA.setMapperClass(JobA.JobAMapper.class);
        jobA.setNumReduceTasks(1);
        jobA.setInputFormatClass(SequenceFileInputFormat.class);
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.addCacheFile(new Path(allWordsJob + "/part-r-00000").toUri());
        FileInputFormat.addInputPath(jobA, new Path(categoryTextsJob + "/part-r-00000"));
        FileOutputFormat.setOutputPath(jobA, new Path(jobAPath));
        jobA.setOutputFormatClass(SequenceFileOutputFormat.class);

        ControlledJob controlledJobA = new ControlledJob(jobA.getConfiguration());
        controlledJobA.addDependingJob(controlledcategotyTextsJob);


        Job jobB = Job.getInstance(new Configuration(), "jobB");
        jobB.setJarByClass(JobB.class);
        jobB.setMapperClass(JobB.JobBMapper.class);
        jobB.setReducerClass(JobB.JobBReducer.class);
        jobB.setNumReduceTasks(1);
        jobB.setInputFormatClass(SequenceFileInputFormat.class);
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);
        jobB.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobB.addCacheFile(new Path(allWordsJob + "/part-r-00000").toUri());
        FileInputFormat.addInputPath(jobB, new Path(categoryTextsJob + "/part-r-00000"));
        FileOutputFormat.setOutputPath(jobB, new Path(jobBPath));

        ControlledJob controlledJobB = new ControlledJob(jobB.getConfiguration());
        controlledJobB.addDependingJob(controlledcategotyTextsJob);

        Job jobB2 = Job.getInstance(new Configuration(), "jobB2");
        jobB2.setJarByClass(JobB2.class);
        jobB2.setMapperClass(JobB2.TokenizerMapper.class);
        jobB2.setNumReduceTasks(1);
        jobB2.setInputFormatClass(SequenceFileInputFormat.class);
        jobB2.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobB2.setOutputKeyClass(Text.class);
        jobB2.setOutputValueClass(IntWritable.class);
        jobB2.addCacheFile(new Path(countsCategoriesPath + "/result-r-00000").toUri());
        FileInputFormat.addInputPath(jobB2, new Path(jobBPath + "/part-r-00000"));
        FileOutputFormat.setOutputPath(jobB2, new Path(jobB2Path));

        ControlledJob controlledJobB2 = new ControlledJob(jobB2.getConfiguration());
        controlledJobB2.addDependingJob(controlledgetCategoriesJob);
        controlledJobB2.addDependingJob(controlledJobB);


        Job jobC = Job.getInstance(new Configuration(), "jobC");
        jobC.setJarByClass(JobC.class);
        jobC.setMapperClass(JobC.JobCMapper.class);
        jobC.setNumReduceTasks(1);
        jobC.setInputFormatClass(SequenceFileInputFormat.class);
        jobC.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobC.setOutputKeyClass(Text.class);
        jobC.setOutputValueClass(IntWritable.class);
        jobC.addCacheFile(new Path(allWordsJob + "/part-r-00000").toUri());
        FileInputFormat.addInputPath(jobC, new Path(categoryTextsJob + "/part-r-00000"));
        FileOutputFormat.setOutputPath(jobC, new Path(jobCPath));

        ControlledJob controlledJobC = new ControlledJob(jobC.getConfiguration());
        controlledJobC.addDependingJob(controlledcategotyTextsJob);

//
//
//        //JobB depends on JobA
//        ControlledJob controlledJobB = new ControlledJob(jobB.getConfiguration());
//        controlledJobB.addDependingJob(controlledJobA);


        //JobC1 taking Input from Preprocessing

//        Job jobC1 = Job.getInstance(new Configuration(), "jobC1");
//        jobC1.setJarByClass(JobC1.class);
//        jobC1.setMapperClass(JobC1.MapperC1.class);
//        jobC1.setReducerClass(JobC1.ReducerC1.class);
//        jobC1.setOutputKeyClass(TextPair.class);
//        jobC1.setOutputValueClass(LongWritable.class);
//        jobC1.setMapOutputKeyClass(Text.class);
//        jobC1.setMapOutputValueClass(TextPairText.class);
//        jobC1.setInputFormatClass(SequenceFileInputFormat.class);
//        jobC1.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        SequenceFileInputFormat.setInputPaths(jobC1, new Path(Prep));
//        SequenceFileOutputFormat.setOutputPath(jobC1, new Path(JobC1));
//
//        //JobC1  depends only on Preprocessing
//        ControlledJob controlledJobC1 = new ControlledJob(jobC1.getConfiguration());
//
//        // JobC2 taking Input from JobA and JobC1
//
//        Job jobC2 = Job.getInstance(new Configuration(), "jobC2");
//        jobC2.setJarByClass(JobC2.class);
//        jobC2.setOutputKeyClass(TextPair.class);
//        jobC2.setOutputValueClass(TextPairLong.class);
//        jobC2.setMapOutputKeyClass(TextPair.class);
//        jobC2.setMapOutputValueClass(TextPairLong.class);
//        jobC2.setMapperClass(JobC2.MapperC2.class);
//        jobC2.setReducerClass(JobC2.ReducerC2.class);
//        jobC2.setInputFormatClass(SequenceFileInputFormat.class);
//        jobC2.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        MultipleInputs.addInputPath(jobC2, new Path(JobA), SequenceFileInputFormat.class, JobC2.MapperA.class);
//        MultipleInputs.addInputPath(jobC2, new Path(JobC1), SequenceFileInputFormat.class, JobC2.MapperC1.class);
//        SequenceFileOutputFormat.setOutputPath(jobC2, new Path(JobC2));
//
//        ControlledJob controlledJobC2 = new ControlledJob(jobC2.getConfiguration());
//        controlledJobC2.addDependingJob(controlledJobA);
//        controlledJobC2.addDependingJob(controlledJobC1);
//
//        //JobChi taking Input from JobA,B, C2 and Sum
//
        Job jobChi = Job.getInstance(new Configuration(), "jobChi");
        jobChi.setJarByClass(ChiTestJob.class);
//        jobChi.setSortComparatorClass(TextPair.Comparator.class);
        jobChi.addCacheFile(new Path(countsPath +"/result2-r-00000").toUri());
//        jobChi.setMapperClass(ChiTestJob.MapperChi.class);
        jobChi.setReducerClass(ChiTestJob.ChiJobReducer.class);
        jobChi.setOutputKeyClass(Text.class);
        jobChi.setOutputValueClass(Text.class);
//        jobChi.setMapOutputKeyClass(TextPair.class);
//        jobChi.setMapOutputValueClass(TextPairLong.class);
//        jobChi.setInputFormatClass(SequenceFileInputFormat.class);
//        jobChi.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(jobChi, new Path(jobAPath), SequenceFileInputFormat.class, ChiTestJob.ChiJobAMapper.class);
        MultipleInputs.addInputPath(jobChi, new Path(jobB2Path), SequenceFileInputFormat.class, ChiTestJob.ChiJobBMapper.class);
        MultipleInputs.addInputPath(jobChi, new Path(jobCPath), SequenceFileInputFormat.class, ChiTestJob.ChiJobCMapper.class);
//
        SequenceFileOutputFormat.setOutputPath(jobChi, new Path(jobChiPath));
//
        ControlledJob controlledChiTestJob = new ControlledJob(jobChi.getConfiguration());
        controlledChiTestJob.addDependingJob(controlledJobA);
        controlledChiTestJob.addDependingJob(controlledJobB2);
        controlledChiTestJob.addDependingJob(controlledJobC);
        controlledChiTestJob.addDependingJob(controlledjobCount);
//
//
//        //JobTop150 taking Input from JobChi
//
//        Job jobTop150 = Job.getInstance(new Configuration(), "jobTop150");
//        jobTop150.setJarByClass(JobTop150.class);
//        jobTop150.setMapperClass(JobTop150.MapperTop150.class);
//        jobTop150.setReducerClass(JobTop150.ReducerTop150.class);
//        jobTop150.setGroupingComparatorClass(CatComp.class);
//        jobTop150.setSortComparatorClass(ChiComp.class);
//        jobTop150.setOutputKeyClass(Text.class);
//        jobTop150.setOutputValueClass(Text.class);
//        jobTop150.setMapOutputKeyClass(PairDouble.class);
//        jobTop150.setMapOutputValueClass(PairDouble.class);
//        jobTop150.setInputFormatClass(SequenceFileInputFormat.class);
//        jobTop150.setOutputFormatClass(TextOutputFormat.class);
//        SequenceFileInputFormat.setInputPaths(jobTop150, new Path(JobChi));
//        TextOutputFormat.setOutputPath(jobTop150, new Path(JobTop150));
//
//        ControlledJob controlledJobTop150 = new ControlledJob(jobTop150.getConfiguration());
//        controlledJobTop150.addDependingJob(controlledJobChi);
//
//        //JopLine taking Input from JobTop150
//
//        Job jobLine = Job.getInstance(new Configuration(), "jobLine");
//        jobLine.setJarByClass(JobLine.class);
//        jobLine.setReducerClass(JobLine.ReducerFinal.class);
//        jobLine.setMapperClass(JobLine.MapperFinal.class);
//        jobLine.setMapOutputKeyClass(Text.class);
//        jobLine.setMapOutputValueClass(Text.class);
//        jobLine.setOutputKeyClass(Text.class);
//        jobLine.setOutputValueClass(Text.class);
//        jobLine.setInputFormatClass(TextInputFormat.class);
//        jobLine.setOutputFormatClass(TextOutputFormat.class);
//        FileInputFormat.setInputPaths(jobLine, new Path(JobTop150));
//        TextOutputFormat.setOutputPath(jobLine, new Path(JobLine));
//
//        ControlledJob controlledJobLine = new ControlledJob(jobLine.getConfiguration());
//        controlledJobLine.addDependingJob(controlledJobTop150);

        // Job control to check whether dependant jobs are executable if previous job has finished.
        JobControl jobControl = new JobControl("jobControl");
//        jobControl.addJob(controlledJobC1);
        jobControl.addJob(controlledFilteringJob);
        jobControl.addJob(controlledjobCount);
        jobControl.addJob(controlledcategotyTextsJob);
        jobControl.addJob(controlledJobA);
        jobControl.addJob(controlledJobB);
        jobControl.addJob(controlledJobC);
        jobControl.addJob(controlledChiTestJob);
        jobControl.addJob(controlledgetCategoriesJob);
        jobControl.addJob(controlledJobB2);
//        jobControl.addJob(controlledJobB);
//        jobControl.addJob(controlledJobC2);
//        jobControl.addJob(controlledJobChi);
//        jobControl.addJob(controlledJobTop150);
//        jobControl.addJob(controlledJobLine);
        Thread starter = new Thread(jobControl);
        starter.start();


        while (!jobControl.allFinished()) {
            Thread.sleep(1000);
        }


        //OutputText takes Input from JobLine and JobTop150 to produce final output file output.txt using File Syste Data Output Stream and Buffered Reader.
//        Path p = new Path(args[2] + "/output.txt");
//        FileSystem f = FileSystem.get(new Configuration());
//        FSDataOutputStream fsDataOutputStream = f.create(p);
//        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
//
//        String[] inputs = {JobTop150 + "/part-r-00000", JobLine + "/part-r-00000"};
//        for (String input : inputs) {
//            Path path = new Path(input);
//            FSDataInputStream fsDataInputStream = f.open(path);
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new DataInputStream(fsDataInputStream)));
//            String text;
//            while ((text = bufferedReader.readLine()) != null) {
//                logger.info(text);
//                bufferedWriter.write(text);
//                bufferedWriter.newLine();
//            }
//            bufferedWriter.newLine();
//            bufferedReader.close();
//        }
//        bufferedWriter.flush();
//        bufferedWriter.close();
//        System.exit(0);
        System.exit(0);
    }

}
