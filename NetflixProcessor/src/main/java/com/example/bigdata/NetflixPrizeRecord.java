package com.example.bigdata;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;


public class NetflixPrizeRecord implements Serializable {

    private static final Logger logger = Logger.getLogger("NetflixPrize");

    private String date = "20";
    private Long film_id = 0L;
    private Long user_id = 0L;
    private Long rate = 0L;

    //============================================
    // Constructors
    //============================================

    public NetflixPrizeRecord(){

    }

    public NetflixPrizeRecord(String date, Long film_id, Long user_id, Long rate) {
        this.date = date;
        this.film_id = film_id;
        this.user_id = user_id;
        this.rate = rate;
    }

    //============================================
    // Methods
    //============================================

    public static NetflixPrizeRecord parseFromCSVLine(String line){
        if(line.isEmpty()){
            logger.log(Level.ALL,"Line is empty");
        }
        String[] record = line.split(",");

        if(record.length != 4){
            logger.log(Level.ALL,"Line size is not correct");
            return new NetflixPrizeRecord();
        }
        if(record[0].equals("date")){
            return new NetflixPrizeRecord();
        }

        return new NetflixPrizeRecord(record[0], Long.parseLong(record[1]), Long.parseLong(record[2]), Long.parseLong(record[3]));
    }

    @JsonIgnore
    public long getTimestampInMillis() {
        // 21/Jul/2014:9:55:27 -0800
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
        Date date;
        try {
            date = sdf.parse(this.date);
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }

    @JsonIgnore
    public String getYearAndMonth() {
        return this.date.substring(0,7);
    }

    @Override
    public String toString(){
        return  "Record: " +
                date + " " +
                film_id + " " +
                user_id  + " " +
                rate;

    }

    //============================================
    // Getters and Setters
    //============================================


    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getFilm_id() {
        return film_id;
    }

    public void setFilm_id(Long film_id) {
        this.film_id = film_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Long getRate() {
        return rate;
    }

    public void setRate(Long rate) {
        this.rate = rate;
    }

}
