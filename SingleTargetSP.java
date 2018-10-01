package comp9313.ass2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTargetSP {

//    record the update of the shortest distance;
    public static enum UpdateCounter {
        UPDATE;
    };

//    1st mapreduce: unpack the original map; input: the original map; output: adjacent list and initial the distance;
    public static class UnpackMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            context.write(new Text(data[1]), new Text(data[1] + "," + data[3]));
            context.write(new Text(data[2]), new Text(data[1] + "," + data[3]));
        }
    }

    public static class UnpackReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String TARGET = conf.get("TARGET");

//            initial the shortest distances and construct adjacent lists;
            StringBuilder tmp = new StringBuilder();
            if (key.toString().equals(TARGET)) {
                tmp.append("0.0;");
                tmp.append("");
                tmp.append(";");
            } else {
                tmp.append("inf;unknow;");
            }
            for (Text val : values) {
                tmp.append(val.toString());
                tmp.append(":");
            }
            context.write(key, new Text(tmp.toString().substring(0, tmp.length() - 1)));
        }
    }

//    2nd mapreduce: find the shortest routes; input: adjacent list and initial distance; output: shortest routes;
    public static class ProcessMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] info = data[1].split(";");
            if (!info[0].equals("inf")) {
                Double dist = Double.parseDouble(info[0]);
                String[] adj = info[2].split(":");
                for (String a : adj) {
                    String[] pair = a.split((","));
                    context.write(new LongWritable(Long.parseLong(pair[0])), new Text(String.valueOf(dist + Double.parseDouble(pair[1])) + ";" + data[0] + "->" + info[1]));
                }
            }
            context.write(new LongWritable(Long.parseLong(data[0])), new Text(data[1]));
        }
    }

    public static class ProcessReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> data = new ArrayList<>();
            for (Text val : values) {
                String refresh = val.toString();
                data.add(refresh);
            }
            if (data.size() == 1) {
                context.write(key, new Text(data.get(0)));
            } else {
//                update shortest distances;
                Double dist = Double.MAX_VALUE;
                String orgNode = "ORGNODE";
                String preNode = "PRENODE";
                String adj = "ADJ";
                for (String datum : data) {
                    String[] info = datum.split(";");
                    if (info.length == 3) {
                        orgNode = info[1];
                        adj = info[2];
                    }
                    if (!info[0].equals("inf")) {
                        if (Double.parseDouble(info[0]) <= dist) {
                            dist = Double.parseDouble(info[0]);
                            preNode = info[1];
                        }
                    }
                }
//                if any distances are updated this round, continue to preceed next round; if no update, stop iteration;
                if (!preNode.equals(orgNode)) {
                    context.getCounter(UpdateCounter.UPDATE).increment(1);
                }
                context.write(key, new Text(String.valueOf(dist) + ";" + preNode + ";" + adj));
            }
        }
    }

//    3rd mapreduce: convert the output to the desired format;
    public static class OutputMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            context.write(new LongWritable(Long.parseLong(data[0])), new Text(data[1]));
        }
    }

    public static class OutputReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] data = val.toString().split(";");
                if (!data[0].equals("inf")) {
                    String result = Double.parseDouble(data[0]) + "\t" + key.toString() + "->" + data[1];
                    context.write(key, new Text(result.substring(0, result.length() - 2)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        int iter = 0;
        boolean FLAG = true;
        String IN = args[0];
        String OUT = args[1];
        String INTMP;
        String OUTTMP = IN + iter;

        Configuration conf = new Configuration();
        conf.set("TARGET", args[2]);

//        1st mapreduce to unpack the original map;
        Job jobUnpack = Job.getInstance(conf, "UnpackMap");
        jobUnpack.setJarByClass(SingleTargetSP.class);
        jobUnpack.setMapperClass(UnpackMapper.class);
        jobUnpack.setReducerClass(UnpackReducer.class);
        jobUnpack.setOutputKeyClass(Text.class);
        jobUnpack.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobUnpack, new Path(IN));
        FileOutputFormat.setOutputPath(jobUnpack, new Path(OUTTMP));
        jobUnpack.waitForCompletion(true);

//        2nd mapreduce to get the shortest distances and routes;
        FileSystem fs = FileSystem.get(conf);
        while (FLAG) {
            FLAG = false;
            iter++;
            INTMP = OUTTMP;
            OUTTMP = IN + iter;
            Job jobProcess = Job.getInstance(conf, "ProcessMap");
            jobProcess.setJarByClass(SingleTargetSP.class);
            jobProcess.setMapperClass(ProcessMapper.class);
            jobProcess.setReducerClass(ProcessReducer.class);
            jobProcess.setOutputKeyClass(LongWritable.class);
            jobProcess.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(jobProcess, new Path(INTMP));
            FileOutputFormat.setOutputPath(jobProcess, new Path(OUTTMP));
            jobProcess.waitForCompletion(true);
            if (jobProcess.getCounters().findCounter(UpdateCounter.UPDATE).getValue() > 0) {
                FLAG = true;
            }
            if (fs.exists(new Path(INTMP))) {
                fs.delete(new Path(INTMP), true);
            }
        }

//        3rd mapreduce to convert the output to desired format;
        Job jobOutput = Job.getInstance(conf, "OutputMap");
        jobOutput.setJarByClass(SingleTargetSP.class);
        jobOutput.setMapperClass(OutputMapper.class);
        jobOutput.setReducerClass(OutputReducer.class);
        jobOutput.setOutputKeyClass(LongWritable.class);
        jobOutput.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobOutput, new Path(OUTTMP));
        FileOutputFormat.setOutputPath(jobOutput, new Path(OUT));
        jobOutput.waitForCompletion(true);

        if (fs.exists(new Path(OUTTMP))) {
            fs.delete(new Path(OUTTMP), true);
        }
    }

}

