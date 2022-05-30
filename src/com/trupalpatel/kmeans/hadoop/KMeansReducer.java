package com.trupalpatel.kmeans.hadoop;

import java.io.IOException;

import com.trupalpatel.geometry.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Vector, Text, Text> {

    private final Text centroidKey = new Text();
    private final Text centroidValue = new Text();

    public void reduce(IntWritable centroidIndex, Iterable<Vector> vectors, Context context)
            throws IOException, InterruptedException {

        // Sum the partial sums
        Vector sum = Vector.copy(vectors.iterator().next());
        while (vectors.iterator().hasNext()) {
            sum.add(vectors.iterator().next());
        }
        // Calculate the new centroid
        Vector newCentroid = sum.averageOut();

        centroidKey.set(centroidIndex.toString());
        centroidValue.set(newCentroid.toString());
        context.write(centroidKey, centroidValue);
    }
}