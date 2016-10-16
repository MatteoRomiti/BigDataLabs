package it.polito.bigdata.spark.example;

import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

public class AmazonReview implements Serializable {
    private int stars;
    private double help; //helpfulness
    private Vector features;
    public double isHelpfulDouble;
    public int helpDenom; //helpfulness denominator
    public int lenText; //length of the review in chars

    public int getLenText() {
        return lenText;
    }

    public void setLenText(int lenText) {
        this.lenText = lenText;
    }
    public int getHelpDenom() {
        return helpDenom;
    }

    public void setHelpDenom(int helpDenom) {
        this.helpDenom = helpDenom;
    }
    public double getIsHelpfulDouble() {
        return isHelpfulDouble;
    }

    public void setIsHelpfulDouble(double isHelpfulDouble) {
        this.isHelpfulDouble = isHelpfulDouble;
    }

    public Vector getFeatures() {
        return features;
    }

    public void setFeatures(Vector features) {
        this.features = features;
    }

    public int getStars() {
	return stars;
    }

    public void setStars(int stars) {
	this.stars = stars;
    }

    public double getHelp() {
	return help;
    }

    public void setHelp(double help) {
	this.help = help;
    }
}
