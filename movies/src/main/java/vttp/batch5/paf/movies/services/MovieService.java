package vttp.batch5.paf.movies.services;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
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

  // TODO: Task 2
  

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public List<Map<String, Object>> getProlificDirectors(int limit) {
    List<Document> topDirectors = mongoRepo.getTopDirectors(limit);
    
    return topDirectors.stream()
        .map(doc -> {
            String directorName = doc.getString("_id");
            Map<String, Object> financials = mysqlRepo.getDirectorFinancials(directorName);
            
            Map<String, Object> result = new HashMap<>();
            result.put("director_name", directorName);
            result.put("movies_count", doc.getInteger("movies_count", 0));
            result.put("total_revenue", financials.get("total_revenue"));
            result.put("total_budget", financials.get("total_budget"));
            
            return result;
        })
        .toList();
  }


  // TODO: Task 4
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() {

  }

}
