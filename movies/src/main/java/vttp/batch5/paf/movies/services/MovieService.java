package vttp.batch5.paf.movies.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

    @Autowired
    private MongoMovieRepository mongoRepo;

    @Autowired
    private MySQLMovieRepository mysqlRepo;

    public List<Map<String, Object>> getProlificDirectors(int limit) {
      
        List<Document> topDirectors = mongoRepo.getProlificDirectors(limit);
        
     
        List<String> directorNames = topDirectors.stream()
            .map(doc -> doc.getString("_id"))
            .collect(Collectors.toList());

        List<Map<String, Object>> financials = mysqlRepo.getDirectorsFinancials(directorNames);
        
        Map<String, Map<String, Object>> financialsMap = financials.stream()
            .collect(Collectors.toMap(
                m -> (String) m.get("director_name"),
                m -> m
            ));

        return topDirectors.stream()
            .map(doc -> {
                String directorName = doc.getString("_id");
                Map<String, Object> financial = financialsMap.getOrDefault(directorName, 
                    Map.of("total_revenue", 0L, "total_budget", 0L));

                Map<String, Object> result = new HashMap<>();
                result.put("director_name", directorName);
                result.put("movies_count", doc.getInteger("movies_count", 0));
                result.put("total_revenue", financial.get("total_revenue"));
                result.put("total_budget", financial.get("total_budget"));
                
                return result;
            })
            .collect(Collectors.toList());
    }

    // TODO: Task 4
    public void generatePDFReport() {
    }

}
