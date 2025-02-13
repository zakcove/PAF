package vttp.batch5.paf.movies.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;
import java.util.List;
import vttp.batch5.paf.movies.services.MovieService;

@Controller
public class MainController {

    @Autowired
    private MovieService movieService;

    @GetMapping("/")
    public String getIndex() {
        return "index";
    } 

    @GetMapping("/api/directors")
    public List<Map<String, Object>> getTopDirectors(
            @RequestParam(defaultValue = "10") int limit) {
        return movieService.getProlificDirectors(limit);
    } 

}
