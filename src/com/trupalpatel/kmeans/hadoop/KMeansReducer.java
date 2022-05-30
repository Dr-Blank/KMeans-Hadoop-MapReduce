package com.trupalpatel.kmeans.hadoop;

import java.io.IOException;

// import com.trupalpatel.utils.Vector;
import it.unipi.hadoop.model.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Vector, Text, Text> {

    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();

    public void reduce(IntWritable centroid, Iterable<Vector> partialSums, Context context)
            throws IOException, InterruptedException {

        // Sum the partial sums
        Vector sum = Vector.copy(partialSums.iterator().next());
        while (partialSums.iterator().hasNext()) {
            sum.sum(partialSums.iterator().next());
        }
        // Calculate the new centroid
        sum.average();

        centroidId.set(centroid.toString());
        centroidValue.set(sum.toString());
        context.write(centroidId, centroidValue);
    }
}