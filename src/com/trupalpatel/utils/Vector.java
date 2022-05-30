package com.trupalpatel.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
// import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class Vector implements Writable {
    /**
     * Vector class to represent a vector in n dimensions.
     * 
     * to be used with hadoop
     */

    private float[] components; // the components of the vector, x y z ....
    private int dimension; // number of components
    public int weight = 1; // to keep track of the num times a vector was added to this vector.

    public static boolean areCompatible(Vector v1, Vector v2) {
        return v1.dimension == v2.dimension;
    }

    // Constructors
    public Vector() {
        this.dimension = 0;
    }

    public Vector(final float[] components) {
        this.setComponents(components);
    }

    public Vector(final String[] components) {
        this.setComponents(components);
    }

    public Vector(int dimension) {
        float[] emptyComponents = new float[dimension];
        this.setComponents(emptyComponents);
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

    public void set(float[] components) {
        this.setComponents(components);
    }

    public void set(String[] stringComponents) {
        this.setComponents(stringComponents);
    }

    private void setDimension() {
        this.dimension = this.components.length;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dimension = in.readInt();
        this.components = new float[this.dimension];

        for (int i = 0; i < this.dimension; i++) {
            this.components[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dimension);

        for (int i = 0; i < this.dimension; i++) {
            out.writeFloat(this.components[i]);
        }
    }

    // @Override
    // public String toString() {
    // String s = Arrays.toString(this.components);
    // return s.substring(1, s.length() - 1).replace(", ", ",");
    // }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dimension; i++) {
            point.append(Float.toString(this.components[i]));
            if (i != dimension - 1) {
                point.append(",");
            }
        }
        return point.toString();
    }

    public Vector add(Vector otherVector) {
        if (!(Vector.areCompatible(this, otherVector))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }

        for (int i = 0; i < this.dimension; i++) {
            this.components[i] += otherVector.components[i];
        }

        this.weight += otherVector.weight;

        return this;
    }

    public static Vector add(Vector v1, Vector v2) {
        if (!(Vector.areCompatible(v1, v2))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }
        float[] newComponents = new float[v1.dimension];
        for (int i = 0; i < v1.dimension; i++) {
            newComponents[i] = v1.components[i] + v2.components[i];
        }
        return new Vector(newComponents);

    }

    public Vector sum(Vector otherVector) {
        return this.add(otherVector);
    }

    public float distance(Vector otherVector) {
        if (!(Vector.areCompatible(this, otherVector))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }
        // Euclidean distance
        float distance = 0.0f;
        for (int i = 0; i < this.dimension; i++) {
            distance += Math.pow(Math.abs(this.components[i] - otherVector.components[i]), 2);
        }
        distance = (float) Math.round(Math.pow(distance, 1f / 2f) * 100000) / 100000.0f;
        return distance;
    }

    public float distance(Vector otherVector, int type) {
        return this.distance(otherVector);
    }

    public static Vector copy(Vector v) {
        Vector newVector = new Vector(v.components);
        newVector.weight = v.weight;
        return newVector;
    }

    public Vector scale(float factor) {
        for (int i = 0; i < this.dimension; i++) {
            this.components[i] = (float) this.components[i] * factor;
        }
        return this;
    }

    public void scale() {
        for (int i = 0; i < this.dimension; i++) {
            float temp = this.components[i] / this.weight;
            this.components[i] = (float) Math.round(temp * 100000) / 100000.0f;
        }
        this.weight = 1;
    }

    public Vector averageOut() {
        // this.scale((float) 1 / this.weight);
        // this.weight = 1;
        // return this;
        this.scale();
        return this;
    }

    public void average() {
        this.scale();
    }
}
