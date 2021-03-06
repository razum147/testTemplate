//package ru.zuzu;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.DefaultCodec;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.log4j.Logger;
//
//import java.io.*;
//
///*
//This is the main class to run all jobs in dependant order.
//Path locations are provided by String arguments and passed in methods to retrieve the inputs directly from files via cached files
//or as outputs directly from the previous jobs in SequenceFileOutputFormat.
//The Job controller controls the correct execution of the files as it checks whether the previous job has generated an output already.
//
//
// */
//public class RunAll {
//
//    public static void main(String[] args) throws Exception {
//
//        Logger logger = Logger.getLogger(RunAll.class);
//
//        String Prep = args[2]+"/Preprocessing";
//        String JobA = args[2]+"/A";
//        String JobB = args[2]+"/B";
//        String JobC1 = args[2]+"/C1";
//        String JobC2 = args[2]+"/C2";
//        String JobSum = args[2]+"/Sum";
//        String JobChi = args[2]+"/Chi";
//        String JobTop150 = args[2]+"/Top150";
//        String JobLine= args[2]+"/Line";
//
//        //Preprocessing Job - reading in inputs file and stopwords file and filtering for desired word formats
//        Configuration configuration = new Configuration() ;
//        Job jobPreprocessing = Job.getInstance(configuration, "jobPreprocessing");
//
//        jobPreprocessing.setJarByClass(Preprocessing.class);
//        jobPreprocessing.setSortComparatorClass(TextPair.Comparator.class);
//        jobPreprocessing.setMapperClass(Preprocessing.MyMapper.class);
//        jobPreprocessing.setMapOutputKeyClass(Text.class);
//        jobPreprocessing.setMapOutputValueClass(Text.class);
//        jobPreprocessing.setOutputKeyClass(Text.class);
//        jobPreprocessing.setOutputValueClass(TextPair.class);
//        jobPreprocessing.setInputFormatClass(TextInputFormat.class);
//        jobPreprocessing.setOutputFormatClass(SequenceFileOutputFormat.class);
//        jobPreprocessing.addCacheFile(new Path(args[1]).toUri());
//        FileInputFormat.addInputPath(jobPreprocessing, new Path(args[0]));
//
//        // Compression for the big dataset of 50 GB --> from 130 MB only 20 MB are used for calculations
//
//        FileOutputFormat.setCompressOutput(jobPreprocessing, true);
//        FileOutputFormat.setOutputCompressorClass(jobPreprocessing, DefaultCodec.class);
//        SequenceFileOutputFormat.setOutputCompressionType(jobPreprocessing, SequenceFile.CompressionType.BLOCK);
//
//        SequenceFileOutputFormat.setOutputPath(jobPreprocessing, new Path(Prep));
//
//        //JobSum taking Input from Input
//
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
//
//        //ControlledJob controlledJobPrep = new ControlledJob(jobPreprocessing.getConfiguration());
//
//        if (!jobPreprocessing.waitForCompletion(true))
//            System.exit(1);
//        if (!jobSum.waitForCompletion(true))
//            System.exit(1);
//
//
//        // JobA taking Input from Preprocessing
//
//        Job jobA = Job.getInstance(new Configuration(), "jobA");
//        jobA.setJarByClass(JobA.class);
//        jobA.setSortComparatorClass(TextPair.Comparator.class);
//        jobA.setMapOutputKeyClass(TextPair.class);
//        jobA.setMapOutputValueClass(TextPair.class);
//        jobA.setOutputKeyClass(TextPair.class);
//        jobA.setOutputValueClass(LongWritable.class);
//        jobA.setMapperClass(JobA.MapperA.class);
//        jobA.setReducerClass(JobA.ReducerA.class);
//        jobA.setInputFormatClass(SequenceFileInputFormat.class);
//        jobA.setOutputFormatClass(SequenceFileOutputFormat.class);
//        SequenceFileInputFormat.setInputPaths(jobA, new Path(Prep));
//        SequenceFileOutputFormat.setOutputPath(jobA, new Path(JobA));
//
//        ControlledJob controlledJobA = new ControlledJob(jobA.getConfiguration());
//
//        //JobB taking Input from JobA
//
//        Job jobB = Job.getInstance(new Configuration(), "jobB");
//        jobB.setJarByClass(JobB.class);
//        jobB.setMapperClass(JobB.MapperB.class);
//        jobB.setReducerClass(JobB.ReducerB.class);
//        jobB.setOutputKeyClass(TextPair.class);
//        jobB.setOutputValueClass(LongWritable.class);
//        jobB.setMapOutputKeyClass(Text.class);
//        jobB.setMapOutputValueClass(TextPairLong.class);
//        jobB.setInputFormatClass(SequenceFileInputFormat.class);
//        jobB.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        SequenceFileInputFormat.setInputPaths(jobB, new Path(JobA));
//        SequenceFileOutputFormat.setOutputPath(jobB, new Path(JobB));
//
//
//        //JobB depends on JobA
//        ControlledJob controlledJobB = new ControlledJob(jobB.getConfiguration());
//        controlledJobB.addDependingJob(controlledJobA);
//
//
//        //JobC1 taking Input from Preprocessing
//
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
//        Job jobChi = Job.getInstance(new Configuration(), "jobChi");
//        jobChi.setJarByClass(JobChi.class);
//        jobChi.setSortComparatorClass(TextPair.Comparator.class);
//        jobChi.addCacheFile(new Path(JobSum +"/part-r-00000").toUri());
//        jobChi.setMapperClass(JobChi.MapperChi.class);
//        jobChi.setReducerClass(JobChi.ReducerChi.class);
//        jobChi.setOutputKeyClass(TextPair.class);
//        jobChi.setOutputValueClass(DoubleWritable.class);
//        jobChi.setMapOutputKeyClass(TextPair.class);
//        jobChi.setMapOutputValueClass(TextPairLong.class);
//        jobChi.setInputFormatClass(SequenceFileInputFormat.class);
//        jobChi.setOutputFormatClass(SequenceFileOutputFormat.class);
//        MultipleInputs.addInputPath(jobChi, new Path(JobA), SequenceFileInputFormat.class, JobChi.MapperA.class);
//        MultipleInputs.addInputPath(jobChi, new Path(JobB), SequenceFileInputFormat.class, JobChi.MapperB.class);
//        MultipleInputs.addInputPath(jobChi, new Path(JobC2),SequenceFileInputFormat.class, JobChi.MapperChi.class);
//
//        SequenceFileOutputFormat.setOutputPath(jobChi, new Path(JobChi));
//
//        ControlledJob controlledJobChi = new ControlledJob(jobChi.getConfiguration());
//        controlledJobChi.addDependingJob(controlledJobA);
//        controlledJobChi.addDependingJob(controlledJobB);
//        controlledJobChi.addDependingJob(controlledJobC2);
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
//
//        // Job control to check whether dependant jobs are executable if previous job has finished.
//        JobControl jobControl = new JobControl("jobControl");
//        jobControl.addJob(controlledJobC1);
//        jobControl.addJob(controlledJobA);
//        jobControl.addJob(controlledJobB);
//        jobControl.addJob(controlledJobC2);
//        jobControl.addJob(controlledJobChi);
//        jobControl.addJob(controlledJobTop150);
//        jobControl.addJob(controlledJobLine);
//        Thread starter = new Thread(jobControl);
//        starter.start();
//
//
//        while(!jobControl.allFinished()) {
//            Thread.sleep(1000);
//        }
//
//
//        //OutputText takes Input from JobLine and JobTop150 to produce final output file output.txt using File Syste Data Output Stream and Buffered Reader.
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
//    }
//}