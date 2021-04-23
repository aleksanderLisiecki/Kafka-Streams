package com.example.bigdata;

public class AggregationKey {

    private String date;
    private Long key_film_id;
    private String key_film_title;

    //============================================
    // Constructors
    //============================================

    public AggregationKey(){

    }

    public AggregationKey(String date, Long key_film_id, String key_film_title) {
        this.date = date;
        this.key_film_id = key_film_id;
        this.key_film_title = key_film_title;
    }

    public AggregationKey(String date, Long id, MovieTitleRecord movieTitleRecord) {
        this.date = date;
        this.key_film_id = id;
        this.key_film_title = movieTitleRecord.getTitle();
    }

    //============================================
    // Methods
    //============================================

    @Override
    public String toString() {
        return  date + " " +
                "id: " + key_film_id + " \"" +
                key_film_title + "\"";
    }

    //============================================
    // Getters and Setters
    //============================================


    public String getDate() { return date; }

    public void setDate(String date) { this.date = date; }

    public Long getKey_film_id() {
        return key_film_id;
    }

    public void setKey_film_id(Long key_film_id) {
        this.key_film_id = key_film_id;
    }

    public String getKey_film_title() {
        return key_film_title;
    }

    public void setKey_film_title(String key_film_title) {
        this.key_film_title = key_film_title;
    }
}
