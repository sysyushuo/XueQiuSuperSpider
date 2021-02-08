package kirk.quant;
/**
 * @author kirk
 * @email sysmenghuan@163.com
 * @date 2021/1/27 15:58
 */


import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.decaywood.collector.CommissionIndustryCollector;
import org.decaywood.entity.trend.StockTrend;
import org.decaywood.mapper.industryFirst.IndustryToStocksMapper;
import org.decaywood.mapper.stockFirst.StockToCapitalFlowEntryMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithAttributeMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithCompanyInfoMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithStockTrendMapper;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.decaywood.entity.Stock;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class update_mongodb {
    public static  MongoDatabase connect_to_mongodb(String name){
        MongoClient mongoClient = new MongoClient();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(name);
        return mongoDatabase;
    }

    public static MongoCollection<Document> connect_to_collection(String collection_name,MongoDatabase mongoDatabase){
        return mongoDatabase.getCollection(collection_name);
    }
    protected static void delet_day(String collection_name,String time){
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


    protected static void update_day(){
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

            res.filter(Objects::nonNull).forEach(x->{
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

    protected static void update_week(){
        //last update 20210125
        List<Stock> stocks=generate_stock_list();
        Calendar calendar = Calendar.getInstance();
        calendar.set(2021,Calendar.JANUARY,25);
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

    protected static void update_month(){
        //last update 20210203
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
    protected static void update_capital_flow(){
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
                .collect(Collectors.toSet())
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
    protected static List<Stock> get_stock_list(){
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

    public static List<Stock> get_stock_between_list(double price_upper,double price_lower){
        Calendar cal=Calendar.getInstance();
        cal.add(Calendar.DATE,-1);
        long today=cal.getTime().getTime();
        HashMap stock_name_code=new HashMap();
        get_stock_list().forEach(x->{stock_name_code.put(x.getStockNo(),x.getStockName());});
        List<Stock> stocks = new ArrayList<>();
        MongoDatabase mongoDatabase = connect_to_mongodb("quant");
        MongoCollection<Document> collection = connect_to_collection("snowball_stock_daily",mongoDatabase);
        Bson bsonFilter = Filters.eq("time", String.valueOf(timeToStamp(today)));
        FindIterable<Document> res = collection.find(bsonFilter);
        for(Document x:res){
            if((Double.parseDouble(x.get("close").toString())<=price_upper) && (Double.parseDouble(x.get("close").toString())>=price_lower)){
                stocks.add(new Stock((String) stock_name_code.get(x.getString("code")),x.getString("code")));
            }


        }
        return stocks;
    }

    protected static void get_stock_majorbiz(){
        List<Stock> stocks = get_stock_list();

        StockToStockWithCompanyInfoMapper mapper = new StockToStockWithCompanyInfoMapper();
        String name="quant";
        MongoDatabase mongoDatabase = connect_to_mongodb(name);
        String collection_name="snowball_stock_majorbiz";
        MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);
        stocks.stream().map(mapper).filter(Objects::nonNull).forEach(
            x->{
                Document object=new Document();
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
    public static void update_stock_attribute()  {

        String name="quant";
        MongoDatabase mongoDatabase = connect_to_mongodb(name);
        String collection_name="snowball_stock_attribute";
        MongoCollection<Document> conn = connect_to_collection(collection_name,mongoDatabase);
        StockToStockWithAttributeMapper attributeMapper = new StockToStockWithAttributeMapper();
        List<Stock> stocks=generate_stock_list();
        Document object=new Document();

        stocks.stream().map(attributeMapper).filter(Objects::nonNull).forEach(
                x->{
                    object.put("stockName",x.getStockName());
                    object.put("stockNo",x.getStockNo());
                    object.put("currency_unit",x.getCurrency_unit());
                    object.put("current",x.getCurrent());
                    object.put("volume",x.getVolume());
                    object.put("percentage",x.getPercentage());
                    object.put("change",x.getChange());
                    object.put("open",x.getOpen());
                    object.put("high",x.getHigh());
                    object.put("low",x.getLow());
                    object.put("amplitude",x.getAmplitude());
                    object.put("fall_stop",x.getFall_stop());
                    object.put("rise_stop",x.getRise_stop());
                    object.put("close",x.getClose());
                    object.put("last_close",x.getLast_close());
                    object.put("high52Week",x.getHigh52Week());
                    object.put("low52Week",x.getLow52week());
                    object.put("marketCapital",x.getMarketCapital());
                    object.put("float_market_capital",x.getFloat_market_capital());
                    object.put("float_shares",x.getFloat_shares());
                    object.put("totalShares",x.getTotalShares());
                    object.put("eps",x.getEps());
                    object.put("net_assets",x.getNet_assets());
                    object.put("pe_ttm",x.getPe_ttm());
                    object.put("pe_lyr",x.getPe_lyr());
                    object.put("dividend",x.getDividend());
                    object.put("psr",x.getPsr());
                    object.put("turnover_rate",x.getTurnover_rate());
                    object.put("amount",x.getAmount());
                    object.put("time",timeToStamp(x.getTime().getTime()));
                    conn.insertOne(object);
                    object.clear();
                    System.out.println("insert " + x.getStockNo() + " attribute done");                }
        );
    }
    public static void main(String[] args) {
//        long current=System.currentTimeMillis();//当前时间毫秒数
//        long zero=current/(1000*3600*24)*(1000*3600*24)-TimeZone.getDefault().getRawOffset();
//        String collection_name="snowball_stock_daily";
//        delet_day(collection_name,String.valueOf(zero));
        System.out.println("start to update day ");
        update_day();
        System.out.println("start ot update capital flow");
        update_capital_flow();
        update_stock_attribute();

//        update_week();
//        update_month();
//        String collection_name="snowball_stock_capital_daily";
//        delet_day(collection_name,"1612195200000");
//        get_stock_majorbiz();
//        get_stock_between_list(20.0,10.0).forEach(x->{System.out.println(x.getStockName());});
    }
}