package it.unipi.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class Vector implements Writable {

    private float[] components = null;
    private int dim;
    private int numPoints = 1; // For partial sums

    public static boolean areCompatible(Vector v1, Vector v2) {
        return v1.dim == v2.dim;
    }

    public Vector() {
        this.dim = 0;
    }

    public Vector(final float[] components) {
        this.setComponents(components);
    }

    public Vector(final String[] components) {
        this.setComponents(components);
    }

    // Getters and Setters for Components.
    public float[] getComponents() {
        return components;
    }

    public void setComponents(float[] components) {
        this.components = components;
        this.setDimension();
    }

    public void setComponents(String[] stringComponents) {
        this.components = new float[stringComponents.length];
        for (int i = 0; i < stringComponents.length; i++) {
            this.components[i] = Float.parseFloat(stringComponents[i]);
        }
        this.setDimension();
    }

    private void setDimension() {
        this.dim = this.components.length;
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
        String s = Arrays.toString(this.components);
        return s.substring(1, s.length() - 1).replace(", ", ",");
    }

    public Vector add(Vector otherVector) {
        if (!(Vector.areCompatible(this, otherVector))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }

        for (int i = 0; i < this.dim; i++) {
            this.components[i] += otherVector.components[i];
        }

        this.numPoints += otherVector.numPoints;

        return this;
    }

    public static Vector add(Vector v1, Vector v2) {
        if (!(Vector.areCompatible(v1, v2))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }
        float[] newComponents = new float[v1.dim];
        for (int i = 0; i < v1.dim; i++) {
            newComponents[i] = v1.components[i] + v2.components[i];
        }
        return new Vector(newComponents);

    }

    public float distance(Vector otherVector) {
        if (!(Vector.areCompatible(this, otherVector))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }
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

    public Vector scale(float factor) {
        for (int i = 0; i < this.dim; i++) {
            this.components[i] = (float) this.components[i] * factor;
        }
        return this;
    }

    public Vector averageOut() {
        this.scale((float) 1 / this.numPoints);
        this.numPoints = 1;
        return this;
    }

    public static Vector copy(Vector v) {
        Vector newVector = new Vector(v.components);
        newVector.numPoints = v.numPoints;
        return newVector;
    }

}