package com.trupalpatel.geometry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

/**
 * Represents a vector in {@code n} dimensions.
 * 
 * to be used with hadoop
 * 
 * @author Trupal Patel
 * @version 1.0.3
 */
public class Vector implements Writable {

    private float[] components = null;// the components of the vector, x y z ....
    private int dimensions;// number of components
    private int weight = 1; // to keep track of the number of times a vector was added to this vector

    public static boolean areCompatible(Vector v1, Vector v2) {
        return v1.dimensions == v2.dimensions;
    }

    // Constructors
    public Vector() {
        this.dimensions = 0;
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
        this.dimensions = this.components.length;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dimensions = in.readInt();
        this.weight = in.readInt();
        this.components = new float[this.dimensions];

        for (int i = 0; i < this.dimensions; i++) {
            this.components[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dimensions);
        out.writeInt(this.weight);

        for (int i = 0; i < this.dimensions; i++) {
            out.writeFloat(this.components[i]);
        }
    }

    @Override
    public String toString() {
        String s = Arrays.toString(this.components);
        return s.substring(1, s.length() - 1).replace(", ", ",");
    }

    public static Vector sum(Vector v1, Vector v2) {
        if (!(Vector.areCompatible(v1, v2))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }
        float[] newComponents = new float[v1.dimensions];
        for (int i = 0; i < v1.dimensions; i++) {
            newComponents[i] = v1.components[i] + v2.components[i];
        }
        return new Vector(newComponents);

    }

    public Vector add(Vector otherVector) {
        Vector summed = Vector.sum(this, otherVector);
        this.setComponents(summed.getComponents());
        this.weight += otherVector.weight;
        return this;
    }

    /**
     * @return Euclidean Distance
     */
    public float distanceTo(Vector otherVector) {
        if (!(Vector.areCompatible(this, otherVector))) {
            throw new IllegalArgumentException("Dimension mismatch");
        }

        float distance = 0.0f;
        for (int i = 0; i < this.dimensions; i++) {
            distance += Math.pow(Math.abs(this.components[i] - otherVector.components[i]), 2);
        }
        distance = (float) Math.round(Math.pow(distance, 1f / 2f) * 100000) / 100000.0f;
        return distance;
    }

    /**
     * Scales the {@code components} by the given {@code factor}
     */
    public Vector scaleBy(float factor) {
        for (int i = 0; i < this.dimensions; i++) {
            this.components[i] = (float) this.components[i] * factor;
        }
        return this;
    }

    /**
     * Used with the Reducer to find the mean of the sum of vectors
     */
    public Vector averageOut() {
        this.scaleBy((float) 1 / this.weight);
        this.weight = 1;
        return this;
    }

    public static Vector copy(Vector v) {
        Vector newVector = new Vector(v.components);
        newVector.weight = v.weight;
        return newVector;
    }

}