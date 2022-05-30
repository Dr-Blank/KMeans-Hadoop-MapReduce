
package com.trupalpatel.kmeans.hadoop;

import java.io.IOException;

// import com.trupalpatel.utils.Vector;
import it.unipi.hadoop.model.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer<IntWritable, Vector, IntWritable, Vector> {

    public void reduce(IntWritable centroidIndex, Iterable<Vector> vectors, Context context)
            throws IOException, InterruptedException {

        // Sum the Vectors
        Vector sum = Vector.copy(vectors.iterator().next());
        while (vectors.iterator().hasNext()) {
            sum.add(vectors.iterator().next());
        }

        context.write(centroidIndex, sum);
    }
}