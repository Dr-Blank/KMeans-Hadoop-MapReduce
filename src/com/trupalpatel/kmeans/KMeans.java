package com.trupalpatel.kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.trupalpatel.geometry.Vector;
import com.trupalpatel.kmeans.hadoop.KMeansCombiner;
import com.trupalpatel.kmeans.hadoop.KMeansMapper;
import com.trupalpatel.kmeans.hadoop.KMeansReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Performs K-means clustering using hadoop MapReduce
 * 
 * Usage for the jar file:
 * {@code hadoop jar <jar_file> <input_file> <output_folder>}
 * 
 * @see <a href=
 *      "https://github.com/seraogianluca/k-means-mapreduce/tree/master/k-means">Inspiration</a>
 * @see {@link Vector} for other implementation details
 * 
 * @version 1.0.3
 */
public class KMeans {

  private static final Path CONFIG_FILE = new Path("config.xml");

  /**
   * Checks for convergence wrt threshold
   */
  private static boolean convergenceAchieved(
      Vector[] oldCentroids, Vector[] newCentroids,
      float threshold) {
    for (int i = 0; i < oldCentroids.length; i++) {
      if (oldCentroids[i].distanceTo(newCentroids[i]) <= threshold) {
        return true;
      }
    }
    return false;
  }

  /**
   * Selects k number of vectors from the input file randomly
   */
  private static Vector[] getRandomCentroids(Configuration conf, String inputFilePathStr, int k, int dataSetSize)
      throws IOException {
    Vector[] vectors = new Vector[k];

    // generate random indices
    List<Integer> randomIndices = new ArrayList<>();
    Random random = new Random();
    int index;
    while (randomIndices.size() < k) {
      index = random.nextInt(dataSetSize); // random number between 0 and dataSetSize
      if (!randomIndices.contains(index)) {
        randomIndices.add(index);
      }
    }
    Collections.sort(randomIndices);

    // read those random indices from the input files
    Path inputFilePath = new Path(inputFilePathStr);
    FileSystem hdfs = FileSystem.get(conf);
    FSDataInputStream f = hdfs.open(inputFilePath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(f));

    int currentLineOfInputFile = 0;
    int indexToGrab;

    int i = 0;
    while (i < randomIndices.size()) {
      indexToGrab = randomIndices.get(i);
      String vector = reader.readLine();
      if (currentLineOfInputFile == indexToGrab) {
        vectors[i] = new Vector(vector.split(","));
        i++;
      }
      currentLineOfInputFile++;
    }
    reader.close();

    return vectors;
  }

  /**
   * 📖 Reads all the part-0000i files from the temp folder of hdfs and loads them
   * into the variable
   */
  private static Vector[] readCentroids(Configuration conf, int k, String tempFolderPath)
      throws IOException, FileNotFoundException {
    Vector[] vectors = new Vector[k];
    FileSystem hdfs = FileSystem.get(conf);
    FileStatus[] status = hdfs.listStatus(new Path(tempFolderPath));

    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().toString().endsWith("_SUCCESS")) {
        continue;
      }

      BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
      String[] keyValueSplit = reader.readLine().split("\t"); // split into index and vector
      int centroidId = Integer.parseInt(keyValueSplit[0]);
      vectors[centroidId] = new Vector(keyValueSplit[1].split(","));
      reader.close();
    }
    // Delete temp directory
    hdfs.delete(new Path(tempFolderPath), true);

    return vectors;
  }

  /**
   * Writes the finalized centroids to hdfs
   */
  private static void writeCentroids(Configuration conf, Vector[] centroids, String outputFolder) throws IOException {

    FileSystem hdfs = FileSystem.get(conf);
    FSDataOutputStream f = hdfs.create(new Path(outputFolder + "/centroids.txt"), true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(f));

    for (Vector vector : centroids) {
      writer.write(vector.toString() + "\n");
    }

    writer.close();
    hdfs.close();
  }

  public static void main(String[] args) throws Exception {
    long startTime = System.currentTimeMillis();
    long endTime;

    Configuration conf = new Configuration();
    conf.addResource(CONFIG_FILE);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // Help 🆘
    if (otherArgs.length != 2) {
      System.err.println("Usage: <input_file> <output_folder>");
      System.exit(1);
    }

    // configurations ⚙️
    final String INPUT_FILE = otherArgs[0];
    final String TEMPORARY_OUTPUT_FOLDER = otherArgs[1] + "/temp";
    final int DATASET_SIZE = conf.getInt("dataset.size", 1000);
    final int K = conf.getInt("k", 3);
    final float THRESHOLD = conf.getFloat("threshold", 0.1f);
    final int MAX_ITERATIONS = conf.getInt("max.iterations", 5);

    Vector[] oldCentroids = new Vector[K];
    Vector[] newCentroids = new Vector[K];

    // Initial centroids
    newCentroids = getRandomCentroids(conf, INPUT_FILE, K, DATASET_SIZE);

    for (int i = 0; i < K; i++) {
      conf.set("centroid." + i, newCentroids[i].toString());
    }

    int i = 0;
    while (true) {
      i++;

      // Hadoop stuff 🐘
      Job iteration = Job.getInstance(conf, "iter_" + i);
      iteration.setJarByClass(KMeans.class);
      iteration.setMapperClass(KMeansMapper.class);
      iteration.setCombinerClass(KMeansCombiner.class);
      iteration.setReducerClass(KMeansReducer.class);
      iteration.setNumReduceTasks(K); // ❗ per centroid 1 reducer
      iteration.setOutputKeyClass(IntWritable.class);
      iteration.setOutputValueClass(Vector.class);
      FileInputFormat.addInputPath(iteration, new Path(INPUT_FILE));
      FileOutputFormat.setOutputPath(iteration, new Path(TEMPORARY_OUTPUT_FOLDER));
      iteration.setInputFormatClass(TextInputFormat.class);
      iteration.setOutputFormatClass(TextOutputFormat.class);

      // check for failure 🛑
      if (!iteration.waitForCompletion(true)) {
        System.err.println("Iteration" + i + "failed.");
        System.exit(1);
      }

      // update centroids 📥
      for (int j = 0; j < K; j++) {
        oldCentroids[j] = Vector.copy(newCentroids[j]);
      }

      newCentroids = readCentroids(conf, K, TEMPORARY_OUTPUT_FOLDER);

      // check for convergence 🎯
      if (convergenceAchieved(oldCentroids, newCentroids, THRESHOLD) || i >= (MAX_ITERATIONS - 1)) {
        writeCentroids(conf, newCentroids, otherArgs[1]);
        break;
      }

      // update centroids in conf 📥⚙️
      for (int d = 0; d < K; d++) {
        conf.unset("centroid." + d);
        conf.set("centroid." + d, newCentroids[d].toString());
      }
    }

    endTime = System.currentTimeMillis() - startTime;

    System.out.println("=======================  📄 Summary   =======================");
    System.out.println(String.format("Centroids (K = %d) : ", K));
    for (int j = 0; j < newCentroids.length; j++) {
      System.out.println(String.format("%d. %s", j + 1, newCentroids[j].toString().replace(",", ", ")));
    }
    System.out.println("-------------------------------------------------------------");
    System.out.println(String.format("🔁 Iterations:\t%d", i));
    System.out.println("-------------------------------------------------------------");
    System.out.println(String.format("⏳ Time taken:\t%d (ms)", endTime));
    System.out.println("=============================================================");

    System.exit(0);
  }

}