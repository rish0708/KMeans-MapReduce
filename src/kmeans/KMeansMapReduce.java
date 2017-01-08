package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/*
    KMeans clustering algorithm in map reduce paradigm. Runs iteratively until the
    clusters shift less than a small threshold distance
 */
public class KMeansMapReduce extends Configured implements Tool {

    // Various input and output paths
    private static final String dataPath = "normalizedcsv.csv";
    private static final String clusterPath = "cluster.csv";
    private static final String outputPath = "/user/rishabh/KMeans/output/iter0/";

    // number of clusters
    private static final int K = 3;

    // the algorithm stops if clusters shift less than the DIFF value
    private static final double DIFF = 1E-4;

    private static final int ATTR_COUNT = 15;

    // counter to check whether new iteration needed or not
    public enum Counter {
        IS_CLUSTER_SHIFTED
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KMeansMapReduce(), args);
        System.exit(res);
    }

    // this method assigns the job configurations and inputs the initial cluster data
    public int run(String[] args) throws Exception {

        //this part opens up the cluster file and inputs the K initial clusters as
        // configuration objects for later use in the program

        Configuration conf = new Configuration();

        // connect to hdfs
        FileSystem fs = FileSystem.get(conf);
        // gets raw data
        DataInputStream ds = new DataInputStream(fs.open(new Path(clusterPath)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(ds));
        String clusterPoint;
        int count = 1;

        // read the clusters as conf objects
        for (; count <= K; count++) {
            clusterPoint = reader.readLine();
            conf.set("cluster" + count, clusterPoint);
        }

        ds.close();

        // set job parameters
        Job job = new Job(conf);
        job.setJarByClass(KMeansMapReduce.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(dataPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // run the job  for the first time
        job.waitForCompletion(true);

        // Steps to be taken to see whether new iteration needed or not
        int iterations = 1;
        long counterValue = job.getCounters().findCounter(Counter.IS_CLUSTER_SHIFTED).getValue();

        while (counterValue > 0) {

            // change parameters of conf object for new iteration
            conf = new Configuration();

            // set configuration objects
            // set new cluster path
            String newClusterPath = "/user/rishabh/KMeans/output/iter" + (iterations - 1) + "/part-r-00000";
            DataInputStream ds1 = new DataInputStream(fs.open(new Path(newClusterPath)));
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(ds1));
            String clusterPoint1;
            int count1 = 1;

            // read the clusters as conf objects
            for (; count1 <= K; count1++) {
                clusterPoint1 = reader1.readLine();
                conf.set("cluster" + count1, clusterPoint1);
            }

            ds1.close();

            //create new job object
            job = new Job(conf);
            job.setJobName("KMeansIteration" + iterations);

            // set more parameters
            job.setJarByClass(KMeansMapReduce.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            //path for data
            String newOutputPath = "/user/rishabh/KMeans/output/iter" + (iterations);

            // set input output formats
            FileInputFormat.addInputPath(job, new Path(dataPath));
            FileOutputFormat.setOutputPath(job, new Path(newOutputPath));

            // key value formats
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // wait for job completion
            job.waitForCompletion(true);
            iterations++;

            counterValue = job.getCounters().findCounter(Counter.IS_CLUSTER_SHIFTED).getValue();

        }
        return 0;
    }

    // Map phase begins here
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private List<MultiPoint> centers;

        // overriding configure method to prepare mapper with getting cluster centers from conf objects
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);
            Configuration conf = context.getConfiguration();
            if (this.centers == null) {
                this.centers = new ArrayList<>();
                MultiPoint point;
                String clusterID;
                double[] attr = new double[15];

                // Load clusters from configuration objects into centers data structure
                for (int i = 1; i <= K; i++) {

                    String nameAndAttr = conf.get("cluster" + i);
                    String[] split = nameAndAttr.split(",");
                    clusterID = split[0];

                    // excluding the name of university
                    int len = split.length - 1;
                    for (int j = 0; j < len; j++)
                        attr[j] = Double.parseDouble(split[j + 1]);

                    // create new point and add it to the list of centres
                    point = new MultiPoint(clusterID, attr);
                    centers.add(point);
                }
            }
        }

        // map function
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // parse the input to get the university name and their attributes
            String[] split = value.toString().split(",");
            int len = split.length - 1;
            double[] attr = new double[len];
            for (int j = 0; j < len; j++)
                attr[j] = Double.parseDouble(split[j + 1]);

            // create a point
            MultiPoint point = new MultiPoint(split[0], attr);
            double minDistance = Double.MAX_VALUE;
            int minIndex = 0;

            // comparing distance with each cluster
            for (int i = 0; i < K; i++) {
                double dist = point.distance(centers.get(i));
                if (dist < minDistance) {
                    minIndex = i;
                    minDistance = dist;
                }
            }
            context.write(new Text(centers.get(minIndex).toString()), new Text(point.toString()));
        }

    }

    // Here starts the reduce part of the algorithm
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        // same thing to do as the Mapper
        private List<MultiPoint> centers;

        // overriding configure method to prepare Reducer with getting cluster centers from conf objects
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();


            if (this.centers == null) {
                this.centers = new ArrayList<>();
                MultiPoint point;
                String clusterID;
                double[] attr = new double[ATTR_COUNT];

                // Load clusters from configuration objects into centers data structure
                for (int i = 1; i <= K; i++) {
                    String nameAndAttr = conf.get("cluster" + i);
                    String[] splits = nameAndAttr.split(",");
                    clusterID = splits[0];
                    int len = splits.length - 1;
                    for (int j = 0; j < len; j++)
                        attr[j] = Double.parseDouble(splits[j + 1]);

                    //create new point and add it to the list of centers
                    point = new MultiPoint(clusterID, attr);
                    centers.add(point);
                }
            }
        }

        // reducer method
        // calculate new averages and see whether to recurse or not
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // calculating new cluster centres
            double[] newAttr = new double[ATTR_COUNT];
            StringBuilder listOfColleges = new StringBuilder();
            int count = 0;

            for (Text value : values) {
                count++;
                String[] split = value.toString().split(",");
                listOfColleges.append(split[0]).append(",");
                int len = split.length - 1;
                for (int i = 0; i < len; i++) {
                    newAttr[i] += Double.parseDouble(split[i + 1]);
                }
            }
            // take average
            if (count != 0) {
                for (int j = 0; j < ATTR_COUNT; j++)
                    newAttr[j] /= count;
            }

            String clusterID = key.toString().split(",")[0];
            MultiPoint point = new MultiPoint(clusterID, newAttr);

            //check whether more iterations are needed
            String[] split = key.toString().split(",");
            for (int i = 0; i < ATTR_COUNT; i++) {
                if (Math.abs(Double.parseDouble(split[i + 1]) - newAttr[i]) > DIFF) {
                    context.getCounter(Counter.IS_CLUSTER_SHIFTED).increment(1);
                }
            }

            //if more iteration needed write the cluster, else write formatted final output
            long val = context.getCounter(Counter.IS_CLUSTER_SHIFTED).getValue();
            if (val == 0)
                context.write(new Text(point.toString() + "\n"), new Text(listOfColleges.toString()));
            else
                context.write(null, new Text(point.toString()));
        }
    }
}
