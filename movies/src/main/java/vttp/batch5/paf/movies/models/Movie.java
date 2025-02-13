package vttp.batch5.paf.movies.models;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Movie {
    private String imdbId;
    private String title = "";
    private List<String> directors = new ArrayList<>();
    private String overview = "";
    private String tagline = "";
    private List<String> genres = new ArrayList<>();
    private Float imdbRating = 0f;
    private Integer imdbVotes = 0;
    private Float voteAverage = 0f;
    private Integer voteCount = 0;
    private Date releaseDate;
    private Double revenue = 0.0;
    private Double budget = 1000000.0;
    private Integer runtime = 90;

    // Getters and Setters
    public String getImdbId() {
        return imdbId;
    }

    public void setImdbId(String imdbId) {
        this.imdbId = imdbId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title == null ? "" : title;
    }

    public List<String> getDirectors() {
        return directors;
    }

    public void setDirectors(List<String> directors) {
        this.directors = directors == null ? new ArrayList<>() : directors;
    }

    public String getOverview() {
        return overview;
    }

    public void setOverview(String overview) {
        this.overview = overview == null ? "" : overview;
    }

    public String getTagline() {
        return tagline;
    }

    public void setTagline(String tagline) {
        this.tagline = tagline == null ? "" : tagline;
    }

    public List<String> getGenres() {
        return genres;
    }

    public void setGenres(List<String> genres) {
        this.genres = genres == null ? new ArrayList<>() : genres;
    }

    public Float getImdbRating() {
        return imdbRating;
    }

    public void setImdbRating(Float imdbRating) {
        this.imdbRating = imdbRating == null ? 0f : imdbRating;
    }

    public Integer getImdbVotes() {
        return imdbVotes;
    }

    public void setImdbVotes(Integer imdbVotes) {
        this.imdbVotes = imdbVotes == null ? 0 : imdbVotes;
    }

    public Float getVoteAverage() {
        return voteAverage;
    }

    public void setVoteAverage(Float voteAverage) {
        this.voteAverage = voteAverage == null ? 0f : voteAverage;
    }

    public Integer getVoteCount() {
        return voteCount;
    }

    public void setVoteCount(Integer voteCount) {
        this.voteCount = voteCount == null ? 0 : voteCount;
    }

    public Date getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }

    public Double getRevenue() {
        return revenue;
    }

    public void setRevenue(Double revenue) {
        this.revenue = revenue == null ? 0.0 : revenue;
    }

    public Double getBudget() {
        return budget;
    }

    public void setBudget(Double budget) {
        this.budget = budget == null ? 1000000.0 : budget;
    }

    public Integer getRuntime() {
        return runtime;
    }

    public void setRuntime(Integer runtime) {
        this.runtime = runtime == null ? 90 : runtime;
    }
}