package com.trupalpatel.kmeans.hadoop;

import java.io.IOException;

// import com.trupalpatel.utils.Vector;
import it.unipi.hadoop.model.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Vector> {

    private Vector[] centroids;
    private final Vector vector = new Vector();
    private final IntWritable centroidIndex = new IntWritable();

    public void setup(Context context) {
        int k = Integer.parseInt(context.getConfiguration().get("k"));

        // Creating the array of centroids
        this.centroids = new Vector[k];
        for (int i = 0; i < k; i++) {
            String[] centroid = context.getConfiguration().getStrings("centroid." + i);
            this.centroids[i] = new Vector(centroid);
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Construct the Vector
        vector.setComponents(value.toString().split(","));

        // Initialize variables
        float minimumDistance = Float.POSITIVE_INFINITY;
        int closestCentroidIndex = -1;
        float currentDistance = 0f;

        // Find the closest centroid
        for (int i = 0; i < centroids.length; i++) {
            currentDistance = vector.distance(centroids[i]);
            if (currentDistance < minimumDistance) {
                closestCentroidIndex = i;
                minimumDistance = currentDistance;
            }
        }

        centroidIndex.set(closestCentroidIndex);
        context.write(centroidIndex, vector);
    }
}
