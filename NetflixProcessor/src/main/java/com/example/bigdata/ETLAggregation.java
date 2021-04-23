package com.example.bigdata;

import java.util.ArrayList;

public class ETLAggregation {

    private Long rateCount;
    private Long rateSum;
    private Long numberOfUniqueUsers;

    private ArrayList<Long> users_ids = new ArrayList<>();

    //============================================
    // Constructors
    //============================================

    public ETLAggregation(){
        this(0L, 0L, 0L);
    }

    public ETLAggregation(Long rateCount, Long rateSum, Long numberOfUniqueUsers) {
        this.rateCount = rateCount;
        this.rateSum = rateSum;
        this.numberOfUniqueUsers = numberOfUniqueUsers;
    }

    //============================================
    // Methods
    //============================================

    public ETLAggregation update(NetflixPrizeRecord record){
        this.rateCount++;
        this.rateSum += record.getRate();

        Long user_id = record.getUser_id();
        if(!this.users_ids.contains(user_id)){
            this.users_ids.add(user_id);
            numberOfUniqueUsers++;
        }

        return this;
    }

    @Override
    public String toString() {
        return "rateCount: " + rateCount +
                ", rateSum: " + rateSum +
                ", NumberOfUniqueUsers:" + numberOfUniqueUsers;
    }

    //============================================
    // Getters and Setters
    //============================================

    public Long getRateCount() {
        return rateCount;
    }

    public void setRateCount(Long rateCount) {
        this.rateCount = rateCount;
    }

    public Long getRateSum() {
        return rateSum;
    }

    public void setRateSum(Long rateSum) {
        this.rateSum = rateSum;
    }

    public Long getNumberOfUniqueUsers() {
        return numberOfUniqueUsers;
    }

    public void setNumberOfUniqueUsers(Long numberOfUniqueUsers) {
        this.numberOfUniqueUsers = numberOfUniqueUsers;
    }
}
