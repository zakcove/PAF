package vttp.batch5.paf.movies.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class MainController {

    @GetMapping("/")
    public String getIndex() {
        return "index";
    }

    // TODO: Task 3
    
    // TODO: Task 4

}
