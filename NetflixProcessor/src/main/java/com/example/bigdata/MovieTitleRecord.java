package com.example.bigdata;

public class MovieTitleRecord {

    private Long year;
    private String title;

    //============================================
    // Constructors
    //============================================

    public MovieTitleRecord(Long year, String title) {
        this.year = year;
        this.title = title;
    }

    public MovieTitleRecord(MovieTitleRecord record){
        this.year = record.getYear();
        this.title = record.getTitle();
    }

    @Override
    public String toString() {
        return "MovieTitleRecord{" +
                ", year=" + year +
                ", title='" + title + '\'' +
                '}';
    }

    //============================================
    // Getters and Setters
    //============================================

    public Long getYear() {
        return year;
    }

    public void setYear(Long year) {
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
