package it.unipi.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Vector implements Writable {

    private float[] components = null;
    private int dim;
    private int numPoints; // For partial sums

    public Vector() {
        this.dim = 0;
    }

    public Vector(final float[] c) {
        this.set(c);
    }

    public Vector(final String[] s) {
        this.set(s);
    }

    public static Vector copy(final Vector p) {
        Vector ret = new Vector(p.components);
        ret.numPoints = p.numPoints;
        return ret;
    }

    public void set(final float[] c) {
        this.components = c;
        this.dim = c.length;
        this.numPoints = 1;
    }

    public void set(final String[] s) {
        this.components = new float[s.length];
        this.dim = s.length;
        this.numPoints = 1;
        for (int i = 0; i < s.length; i++) {
            this.components[i] = Float.parseFloat(s[i]);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numPoints = in.readInt();
        this.components = new float[this.dim];

        for (int i = 0; i < this.dim; i++) {
            this.components[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numPoints);

        for (int i = 0; i < this.dim; i++) {
            out.writeFloat(this.components[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dim; i++) {
            point.append(Float.toString(this.components[i]));
            if (i != dim - 1) {
                point.append(",");
            }
        }
        return point.toString();
    }

    public void sum(Vector p) {
        for (int i = 0; i < this.dim; i++) {
            this.components[i] += p.components[i];
        }
        this.numPoints += p.numPoints;
    }

    // public float distance(Vector p, int h) {
    // if (h < 0) {
    // // Consider only metric distances
    // h = 2;
    // }

    // if (h == 0) {
    // // Chebyshev
    // float max = -1f;
    // float diff = 0.0f;
    // for (int i = 0; i < this.dim; i++) {
    // diff = Math.abs(this.components[i] - p.components[i]);
    // if (diff > max) {
    // max = diff;
    // }
    // }
    // return max;

    // } else {
    // // Manhattan, Euclidean, Minkowsky
    // float dist = 0.0f;
    // for (int i = 0; i < this.dim; i++) {
    // dist += Math.pow(Math.abs(this.components[i] - p.components[i]), h);
    // }
    // dist = (float) Math.round(Math.pow(dist, 1f / h) * 100000) / 100000.0f;
    // return dist;
    // }
    // }

    public float distance(Vector otherVector) {
        // if (!(Vector.areCompatible(this, otherVector))) {
        //     throw new IllegalArgumentException("Dimension mismatch");
        // }
        // Euclidean distance
        float distance = 0.0f;
        for (int i = 0; i < this.dim; i++) {
            distance += Math.pow(Math.abs(this.components[i] - otherVector.components[i]), 2);
        }
        distance = (float) Math.round(Math.pow(distance, 1f / 2f) * 100000) / 100000.0f;
        return distance;
    }

    public float distance(Vector otherVector, int type) {
        return this.distance(otherVector);
    }

    public void average() {
        for (int i = 0; i < this.dim; i++) {
            float temp = this.components[i] / this.numPoints;
            this.components[i] = (float) Math.round(temp * 100000) / 100000.0f;
        }
        this.numPoints = 1;
    }
}