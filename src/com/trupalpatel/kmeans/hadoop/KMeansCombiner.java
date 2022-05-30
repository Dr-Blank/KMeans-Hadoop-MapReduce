
package com.trupalpatel.kmeans.hadoop;

import java.io.IOException;

// import com.trupalpatel.utils.Vector;
import it.unipi.hadoop.model.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer<IntWritable, Vector, IntWritable, Vector> {

    public void reduce(IntWritable centroid, Iterable<Vector> points, Context context)
            throws IOException, InterruptedException {

        // Sum the points
        Vector sum = Vector.copy(points.iterator().next());
        while (points.iterator().hasNext()) {
            sum.sum(points.iterator().next());
        }

        context.write(centroid, sum);
    }
}