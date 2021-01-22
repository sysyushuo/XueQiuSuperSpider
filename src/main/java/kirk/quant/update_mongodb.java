package kirk.quant;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.decaywood.entity.trend.StockTrend;
import org.decaywood.mapper.stockFirst.StockToStockWithStockTrendMapper;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.decaywood.entity.Stock;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import static com.mongodb.client.model.Filters.gte;

class MongoDBUtilities {
    public static  MongoDatabase connect_to_mongodb(String name){
        MongoClient mongoClient = new MongoClient();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(name);
        return mongoDatabase;
    }

    public static MongoCollection<Document> connect_to_collection(String collection_name,MongoDatabase mongoDatabase){
        return mongoDatabase.getCollection(collection_name);
    }




    public static <e> void main(String args[]) {
        MongoClient mongoClient=null;
        List<Stock> stocks=new ArrayList<Stock>();
        StockToStockWithStockTrendMapper mapper = new StockToStockWithStockTrendMapper();

        try {
            String name="quant";
            MongoDatabase mongoDatabase = connect_to_mongodb(name);
            System.out.println("Connect to database successfully");
            MongoCollection<Document> collection = connect_to_collection("ts-stock_basic",mongoDatabase);

            FindIterable<Document> findIterable = collection.find();
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            while (mongoCursor.hasNext()) {
                Document tmp = mongoCursor.next();
                String[ ] tmp_str=tmp.getString("ts_code").split("\\.");
                stocks.add(new Stock(tmp.getString("name"),(tmp_str[1]+tmp_str[0]) ));
            }

            String collection_name="snowball_stock_daily";
            MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);


//            Bson query = Filters.eq("time", "1611244800000");
//
//            conn.deleteMany(query);

            Stream<StockTrend> res = stocks.stream()
                    .map(mapper.andThen(Stock::getStockTrend));

            res.forEach(x->{
                        Document object=new Document();
                        x.getHistory().forEach(trend->{
                            object.put("code", x.getStockNo());
                            object.put("open",trend.getOpen());
                            object.put("close", trend.getClose());
                            object.put("high",trend.getHigh());
                            object.put("low",trend.getLow());
                            object.put("ma5",trend.getMa5());
                            object.put("ma10",trend.getMa10());
                            object.put("ma20",trend.getMa20());
                            object.put("ma30",trend.getMa30());
                            object.put("dea",trend.getDea());
                            object.put("dif",trend.getDif());
                            object.put("macd",trend.getMacd());
                            object.put("chg",trend.getChg());
                            object.put("percent",trend.getPercent());
                            object.put("time",trend.getTime());
                            object.put("turnrate",trend.getTurnrate());
                            object.put("volume",trend.getVolume());

                            conn.insertOne( object);
                            object.clear();
                        });
                    System.out.println("insert "+x.getStockNo()+" done");
            });
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }
}