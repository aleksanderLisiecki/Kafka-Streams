package com.example.bigdata;

import java.util.ArrayList;

public class AnomalyAggregation {

    private Long numberOfRates;
    private Long ratesSum;
    private Double averageDegree;

    //============================================
    // Constructors
    //============================================

    public AnomalyAggregation(){
        this(0L, 0L, 0.0);
    }

    public AnomalyAggregation(Long numberOfRates, Long ratesSum, Double averageDegree) {
        this.numberOfRates = numberOfRates;
        this.ratesSum = ratesSum;
        this.averageDegree = averageDegree;
    }

    //============================================
    // Methods
    //============================================

    public AnomalyAggregation update(NetflixPrizeRecord record){
        this.numberOfRates++;
        this.ratesSum += record.getRate();
        this.averageDegree = (double)ratesSum/numberOfRates;

        return this;
    }

    @Override
    public String toString() {
        return "AnomalyAggregation{" +
                "numberOfRates=" + numberOfRates +
                ", ratesSum=" + ratesSum +
                ", averageDegree=" + averageDegree +
                '}';
    }

    //============================================
    // Getters and Setters
    //============================================


    public Long getNumberOfRates() {
        return numberOfRates;
    }

    public void setNumberOfRates(Long numberOfRates) {
        this.numberOfRates = numberOfRates;
    }

    public Long getRatesSum() {
        return ratesSum;
    }

    public void setRatesSum(Long ratesSum) {
        this.ratesSum = ratesSum;
    }

    public Double getAverageDegree() {
        return averageDegree;
    }

    public void setAverageDegree(Double averageDegree) {
        this.averageDegree = averageDegree;
    }
}
