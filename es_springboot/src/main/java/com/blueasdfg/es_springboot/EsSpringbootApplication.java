package com.blueasdfg.es_springboot;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class EsSpringbootApplication {

     @Autowired
     private TransportClient client;

// @GetMapping是一个组合注解，是@RequestMapping(method = RequestMethod.GET)的缩写。
// @ResponseBody返回结果直接写入HTTP response body中，不会被解析为跳转路径。比如异步请求，希望响应的结果是json数据，那么加上@responsebody后，就会直接返回json数据。
     @GetMapping("/get/boot/novel")
     @ResponseBody
     public ResponseEntity get(@RequestParam(name = "id") String id){
          GetResponse result = this.client.prepareGet("megacorp", "employee", id)
                  .get();
          return new ResponseEntity(result.getSource(), HttpStatus.OK);
     }

//{"about":"I like to build cabinets","last_name":"Fir","interests":["forestry"],
// "first_name":"Douglas","age":35}
     @PostMapping("add/book/novel")
     @ResponseBody
     public ResponseEntity add(
             @RequestParam(name = "first_name") String first_name,
             @RequestParam(name = "last_name") String last_name,
             @RequestParam(name = "age") String age,
             @RequestParam(name = "about") String about,
             @RequestParam(name = "interests") String interests
     ){
          try {
               XContentBuilder content =  XContentFactory.jsonBuilder()
                       .startObject()
                       .field("first_name",first_name)
                       .field("last_name",last_name)
                       .field("age",age)
                       .field("about",about)
                       .field("interests",interests)
                       .endObject();
               IndexResponse result =  this.client.prepareIndex("megacorp", "employee")
                       .setSource(content)
                       .get();
               return new ResponseEntity(result.getId(), HttpStatus.OK);
          } catch (IOException e) {
               e.printStackTrace();
               return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
          }

     }


     @PutMapping("update/book/novel")
     @ResponseBody
     public ResponseEntity update(
             @RequestParam(name = "id") String id,
             @RequestParam(name = "first_name", required = false) String first_name,
             @RequestParam(name = "last_name", required = false) String last_name,
             @RequestParam(name = "age", required = false) String age,
             @RequestParam(name = "about", required = false) String about,
             @RequestParam(name = "interests", required = false) String interests
             ){
          UpdateRequest update = new UpdateRequest("megacorp", "employee", id);

          try {
               XContentBuilder builder = XContentFactory.jsonBuilder()
                       .startObject();

               if (first_name !=null){
                    builder.field("first_name", first_name);
                    builder.field("last_name", last_name);
                    builder.field("age", age);
                    builder.field("about", about);
                    builder.field("interests", interests);
               }
               builder.endObject();

               update.doc(builder);

          } catch (IOException e) {
               e.printStackTrace();
               return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
          }

          try {
               UpdateResponse result = this.client.update(update).get();
               return new ResponseEntity(result.getResult().toString(), HttpStatus.OK);
          } catch (Exception e) {
               e.printStackTrace();
               return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
          }

     }


     @DeleteMapping("delete/book/novel")
     @ResponseBody
     public ResponseEntity delete(@RequestParam(name = "id") String id){
          DeleteResponse result = this.client.prepareDelete("megacorp", "employee", id)
                  .get();
          return new ResponseEntity(result.getResult().toString(), HttpStatus.OK);
     }


     @PostMapping("query/book/novel")
     @ResponseBody
     public ResponseEntity query(
             @RequestParam(name = "first_name", required = false) String first_name,
             @RequestParam(name = "last_name", required = false) String last_name,
             @RequestParam(name = "gt_age", required = false) Integer gt_age,
             @RequestParam(name = "lt_age", required = false) Integer lt_age
     ){

           // 布尔类型的判断,例如：a=1
           BoolQueryBuilder boolQuery  = QueryBuilders.boolQuery();

           if (first_name != null){
                boolQuery.must(QueryBuilders.matchQuery("first_name", first_name));
           }

          if (last_name != null){
               boolQuery.must(QueryBuilders.matchQuery("last_name", last_name));
          }

          // 范围的判断,例如：a>1
          RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age")
                  .from(gt_age);
           if (lt_age != null && lt_age > 0){
                rangeQuery.to(lt_age);
           }

           // filter过滤器
           boolQuery.filter(rangeQuery);

          SearchRequestBuilder builder = this.client.prepareSearch("megacorp")
                  .setTypes("employee")
                  .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                  .setQuery(boolQuery)
                  .setFrom(0)
                  .setSize(10);
          System.out.println(builder);

          SearchResponse response = builder.get();

          // ArrayList动态数组,数据量可变
          // Map接口中键和值一一映射. 可以通过键来获取值
          List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

          for (SearchHit hit : response.getHits()){
               result.add(hit.getSource());
          }
          return new ResponseEntity(result, HttpStatus.OK);

     }


     @GetMapping("/")
     public String index(){
          return "index";
     }

     public static void main(String[] args) {
          SpringApplication.run(EsSpringbootApplication.class, args);
     }
}














