package com.trupalpatel.kmeans;

// reference: https://github.com/seraogianluca/k-means-mapreduce/tree/master/k-means

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

import com.trupalpatel.kmeans.hadoop.KMeansCombiner;
import com.trupalpatel.kmeans.hadoop.KMeansMapper;
import com.trupalpatel.kmeans.hadoop.KMeansReducer;
import com.trupalpatel.geometry.Vector;

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

public class KMeans {

  private static boolean convergenceAchieved(
      Vector[] oldCentroids, Vector[] newCentroids,
      float threshold) {
    boolean check = true;
    for (int i = 0; i < oldCentroids.length; i++) {
      check = oldCentroids[i].distance(newCentroids[i]) <= threshold;
      if (!check) {
        return false;
      }
    }
    return true;
  }

  private static Vector[] getRandomCentroids(Configuration conf, String inputFileStringPath, int k, int dataSetSize)
      throws IOException {
    // Vector[] vectors = new Vector[k];
    Vector[] vectors = new Vector[k];

    List<Integer> centroidIndices = new ArrayList<>();
    Random random = new Random();
    int index;
    while (centroidIndices.size() < k) {
      index = random.nextInt(dataSetSize); // random number between 0 and dataSetSize
      if (!centroidIndices.contains(index)) {
        centroidIndices.add(index);
      }
    }
    Collections.sort(centroidIndices);

    // File reading utils
    Path inputFilePath = new Path(inputFileStringPath);
    FileSystem hdfs = FileSystem.get(conf);
    FSDataInputStream f = hdfs.open(inputFilePath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(f));

    // Get centroids from the file
    int row = 0;
    int i = 0;
    int position;

    while (i < centroidIndices.size()) {
      position = centroidIndices.get(i);
      String vector = reader.readLine();
      if (row == position) {
        // vectors.set(i, new Vector(vector.split(",")));
        vectors[i] = new Vector(vector.split(","));
        i++;
      }
      row++;
    }
    reader.close();

    return vectors;
  }

  private static Vector[] readCentroids(Configuration conf, int k, String pathString)
      throws IOException, FileNotFoundException {
    Vector[] vectors = new Vector[k];
    FileSystem hdfs = FileSystem.get(conf);
    FileStatus[] status = hdfs.listStatus(new Path(pathString));

    for (int i = 0; i < status.length; i++) {
      // Read the centroids from the hdfs
      if (status[i].getPath().toString().endsWith("_SUCCESS")) {
        continue;
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
      String[] keyValueSplit = reader.readLine().split("\t"); // Split line in K,V
      int centroidId = Integer.parseInt(keyValueSplit[0]);
      vectors[centroidId] = new Vector(keyValueSplit[1].split(","));
      reader.close();
    }
    // Delete temp directory
    hdfs.delete(new Path(pathString), true);

    return vectors;
  }

  private static void finalize(Configuration conf, Vector[] centroids, String output) throws IOException {
    FileSystem hdfs = FileSystem.get(conf);
    FSDataOutputStream dos = hdfs.create(new Path(output + "/centroids.txt"), true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(dos));

    // Write the result in a unique file
    for (int i = 0; i < centroids.length; i++) {
      writer.write(centroids[i].toString());
      writer.newLine();
    }

    writer.close();
    hdfs.close();
  }

  public static void main(String[] args) throws Exception {
    long startTime = 0;
    long endTime = 0;
    long startTimeCentroids = 0;
    long endTimeCentroids = 0;

    startTime = System.currentTimeMillis();
    Configuration conf = new Configuration();
    conf.addResource(new Path("config.xml")); // Configuration file for the parameters

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: <input_file> <output_folder>");
      System.exit(1);
    }

    // Parameters setting
    final String INPUT_FILE = otherArgs[0];
    final String TEMPORARY_OUTPUT_FOLDER = otherArgs[1] + "/temp";
    final int DATASET_SIZE = conf.getInt("dataset.size", 10);
    // final int DISTANCE = conf.getInt("distance", 2);
    final int K = conf.getInt("k", 3);
    final float THRESHOLD = conf.getFloat("threshold", 0.0001f);
    final int MAX_ITERATIONS = conf.getInt("max.iteration", 6);

    Vector[] oldCentroids = new Vector[K];
    Vector[] newCentroids = new Vector[K];

    // Initial centroids
    startTimeCentroids = System.currentTimeMillis();
    newCentroids = getRandomCentroids(conf, INPUT_FILE, K, DATASET_SIZE);
    endTimeCentroids = System.currentTimeMillis();

    for (int i = 0; i < K; i++) {
      conf.set("centroid." + i, newCentroids[i].toString());
    }

    // MapReduce workflow
    boolean succeeded = true;
    int i = 0;
    while (true) {
      i++;

      // Job configuration
      Job iteration = Job.getInstance(conf, "iter_" + i);
      iteration.setJarByClass(KMeans.class);
      iteration.setMapperClass(KMeansMapper.class);
      iteration.setCombinerClass(KMeansCombiner.class);
      iteration.setReducerClass(KMeansReducer.class);
      iteration.setNumReduceTasks(K); // one task for each centroid
      iteration.setOutputKeyClass(IntWritable.class);
      iteration.setOutputValueClass(Vector.class);
      FileInputFormat.addInputPath(iteration, new Path(INPUT_FILE));
      FileOutputFormat.setOutputPath(iteration, new Path(TEMPORARY_OUTPUT_FOLDER));
      iteration.setInputFormatClass(TextInputFormat.class);
      iteration.setOutputFormatClass(TextOutputFormat.class);

      succeeded = iteration.waitForCompletion(true);

      // If the job fails the application will be closed.
      if (!succeeded) {
        System.err.println("Iteration" + i + "failed.");
        System.exit(1);
      }

      // Save old centroids and read new centroids
      for (int j = 0; j < K; j++) {
        oldCentroids[j] = Vector.copy(newCentroids[j]);
      }

      newCentroids = readCentroids(conf, K, TEMPORARY_OUTPUT_FOLDER);

      // Check if centroids are changed
      if (convergenceAchieved(oldCentroids, newCentroids, THRESHOLD) || i == (MAX_ITERATIONS - 1)) {
        finalize(conf, newCentroids, otherArgs[1]);
        break;
      } else {
        // Set the new centroids in the configuration
        for (int d = 0; d < K; d++) {
          conf.unset("centroid." + d);
          conf.set("centroid." + d, newCentroids[d].toString());
        }
      }
    }

    endTime = System.currentTimeMillis();

    endTime -= startTime;
    endTimeCentroids -= startTimeCentroids;

    System.out.println("execution time: " + endTime + " ms");
    System.out.println("init centroid execution: " + endTimeCentroids + " ms");
    System.out.println("n_iter: " + i);

    System.exit(0);
  }

}