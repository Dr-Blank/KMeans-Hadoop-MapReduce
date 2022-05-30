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
    private int p;
    private final Vector point = new Vector();
    private final IntWritable centroid = new IntWritable();

    public void setup(Context context) {
        int k = Integer.parseInt(context.getConfiguration().get("k"));
        this.p = Integer.parseInt(context.getConfiguration().get("distance"));

        this.centroids = new Vector[k];
        for (int i = 0; i < k; i++) {
            String[] centroid = context.getConfiguration().getStrings("centroid." + i);
            this.centroids[i] = new Vector(centroid);
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Contruct the point
        String[] pointString = value.toString().split(",");
        point.setComponents(pointString);

        // Initialize variables
        float minDist = Float.POSITIVE_INFINITY;
        float distance = 0.0f;
        int nearest = -1;

        // Find the closest centroid
        for (int i = 0; i < centroids.length; i++) {
            distance = point.distance(centroids[i], p);
            if (distance < minDist) {
                nearest = i;
                minDist = distance;
            }
        }

        centroid.set(nearest);
        context.write(centroid, point);
    }
}
