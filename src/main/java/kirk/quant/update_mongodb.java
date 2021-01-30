package kirk.quant;
/**
 * @author kirk
 * @email sysmenghuan@163.com
 * @date 2021/1/27 15:58
 */

import com.mongodb.Block;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.decaywood.collector.CommissionIndustryCollector;
import org.decaywood.entity.trend.StockTrend;
import org.decaywood.mapper.industryFirst.IndustryToStocksMapper;
import org.decaywood.mapper.stockFirst.StockToCapitalFlowEntryMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithCompanyInfoMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithStockTrendMapper;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.decaywood.entity.Stock;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class update_mongodb {
    public static  MongoDatabase connect_to_mongodb(String name){
        MongoClient mongoClient = new MongoClient();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(name);
        return mongoDatabase;
    }

    public static MongoCollection<Document> connect_to_collection(String collection_name,MongoDatabase mongoDatabase){
        return mongoDatabase.getCollection(collection_name);
    }
    public static void delet_day(String collection_name,String time){
        String name="quant";
        MongoDatabase mongoDatabase = connect_to_mongodb(name);
        MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);
        Bson query = Filters.eq("time", time);
        conn.deleteMany(query);
    }

    public static String timeToStamp(long date) {
        long current = date;
        long theDay= current - (current + TimeZone.getDefault().getRawOffset()) % (1000 * 3600 * 24);
        return String.valueOf(theDay);
    }

    public static List<Stock> generate_stock_list(){
        String name="quant";
        MongoDatabase mongoDatabase = connect_to_mongodb(name);
        List<Stock> stocks=new ArrayList<Stock>();
        MongoCollection<Document> collection = connect_to_collection("ts-stock_basic",mongoDatabase);

        FindIterable<Document> findIterable = collection.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while (mongoCursor.hasNext()) {
            Document tmp = mongoCursor.next();
            String[ ] tmp_str=tmp.getString("ts_code").split("\\.");
            stocks.add(new Stock(tmp.getString("name"),(tmp_str[1]+tmp_str[0]) ));
        }
        return stocks;
    }


    public static void update_day(){
        List<Stock> stocks=generate_stock_list();

        Calendar calendar = Calendar.getInstance();
        Date to = new Date();
        calendar.setTime(to);
        calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 1);
        Date from = calendar.getTime();

        StockToStockWithStockTrendMapper mapper = new StockToStockWithStockTrendMapper(StockTrend.Period.DAY,from,to);
        try {

            String name="quant";
            MongoDatabase mongoDatabase = connect_to_mongodb(name);
            String collection_name="snowball_stock_daily";
            MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);

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
                System.out.println("insert "+x.getStockNo()+" day done");
            });
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public static void update_week(){
        //上次更新时间为20210122
        List<Stock> stocks=generate_stock_list();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2000,1,1);
        Date from = calendar.getTime();
        Date to = new Date();

        StockToStockWithStockTrendMapper mapper = new StockToStockWithStockTrendMapper(StockTrend.Period.WEEK,  from,  to);
        try {
            String name="quant";
            MongoDatabase mongoDatabase = connect_to_mongodb(name);

            String collection_name="snowball_stock_week";
            MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);


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
                System.out.println("insert "+x.getStockNo()+" week done");
            });
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public static void update_month(){
        //上次更新时间为20210122
        MongoClient mongoClient=null;
        List<Stock> stocks=generate_stock_list();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2000,1,1);
        Date from = calendar.getTime();
        Date to = new Date();

        StockToStockWithStockTrendMapper mapper = new StockToStockWithStockTrendMapper(StockTrend.Period.MONTH,  from,  to);
        try {
            String name="quant";
            MongoDatabase mongoDatabase = connect_to_mongodb(name);

            String collection_name="snowball_stock_month";
            MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);

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
                System.out.println("insert "+x.getStockNo()+" month done");
            });
        }
        catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }
    public static void update_capital_flow(){
        Calendar cal=Calendar.getInstance();
        cal.add(Calendar.DATE,0);
        long today=cal.getTime().getTime();
        

        String name="quant";
        MongoDatabase mongoDatabase = connect_to_mongodb(name);

        String collection_name="snowball_stock_capital_daily";
        MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);

        CommissionIndustryCollector collector = new CommissionIndustryCollector();
        IndustryToStocksMapper mapper = new IndustryToStocksMapper();
        StockToCapitalFlowEntryMapper mapper1 = new StockToCapitalFlowEntryMapper();
        Document object=new Document();
        collector.get()
                .parallelStream()
                .map(mapper)
                .flatMap(Collection::stream)
                .map(mapper1)
                .forEach(x-> {
                    object.put("industry",x.getKey().getIndustry().getIndustryName());
                    object.put("code",x.getKey().getStockNo());
                    object.put("capitalInflow",x.getValue().getCapitalInflow());
                    object.put("largeQuantity",x.getValue().getLargeQuantity());
                    object.put("midQuantity",x.getValue().getMidQuantity());
                    object.put("smallQuantity",x.getValue().getSmallQuantity());
                    object.put("largeQuantBuy",x.getValue().getLargeQuantity());
                    object.put("largeQuantSell",x.getValue().getLargeQuantSell());
                    object.put("largeQuantDealProp",x.getValue().getLargeQuantDealProp());
                    object.put("fiveDayInflow",x.getValue().getFiveDayInflow());
                    object.put("time",timeToStamp(today));
                    conn.insertOne( object);
                    object.clear();
                    System.out.println("insert "+x.getKey().getStockNo()+" capital done");
                });
    }
    public static List<Stock> get_stock_list(){
        List<Stock> stocks = new ArrayList<>();
        MongoDatabase mongoDatabase = connect_to_mongodb("quant");
        MongoCollection<Document> collection = connect_to_collection("ts-stock_basic",mongoDatabase);
        FindIterable<Document> res = collection.find();
        for(Document x:res){
            String[] tmp = x.getString("ts_code").split("\\.");
            stocks.add(new Stock(x.getString("name"),tmp[1]+tmp[0]));

        }
        return stocks;
    }

    public static void get_stock_majorbiz(){
        List<Stock> stocks = get_stock_list();
        Document object=new Document();
        StockToStockWithCompanyInfoMapper mapper = new StockToStockWithCompanyInfoMapper();
        String name="quant";
        MongoDatabase mongoDatabase = connect_to_mongodb(name);
        String collection_name="snowball_stock_majorbiz";
        MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);
        stocks.stream().map(mapper).filter(Objects::nonNull).forEach(
            x->{
                object.put("code", x.getStockNo());
                object.put("name", x.getStockName());
                object.put("orgtype", x.getCompanyInfo().getOrgtype());
                object.put("founddate", x.getCompanyInfo().getFounddate());
                object.put("bizscope", x.getCompanyInfo().getBizscope());
                object.put("majorbiz", x.getCompanyInfo().getMajorbiz());
                conn.insertOne(object);
                object.clear();
                System.out.println("insert " + x.getStockNo() + " majorbiz done");
            }
        );
    }

    public static void main(String args[]) {
        update_day();
        update_capital_flow();
//        update_week();
//        update_month();
//        String collection_name="snowball_stock_capital_daily";
//        delet_day(collection_name,timeToStamp());
//        get_stock_majorbiz();
    }
}