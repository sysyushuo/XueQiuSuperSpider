import mapperTest.TestCaseGenerator;
import org.decaywood.collector.*;
import org.decaywood.entity.*;
import org.decaywood.entity.trend.StockTrend;
import org.decaywood.filter.PageKeyFilter;
import org.decaywood.mapper.comment.CommentSetMapper;
import org.decaywood.mapper.cubeFirst.CubeToCubeWithLastBalancingMapper;
import org.decaywood.mapper.cubeFirst.CubeToCubeWithTrendMapper;
import org.decaywood.mapper.dateFirst.DateToLongHuBangStockMapper;
import org.decaywood.mapper.industryFirst.IndustryToStocksMapper;
import org.decaywood.mapper.stockFirst.StockToLongHuBangMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithAttributeMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithCompanyInfoMapper;
import org.decaywood.mapper.stockFirst.StockToStockWithStockTrendMapper;
import org.decaywood.utils.MathUtils;
import org.junit.Test;

import java.net.URL;
import java.rmi.RemoteException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import kirk.quant.update_mongodb;

/**
 * @author: decaywood
 * @date: 2015/11/24 14:06
 */
public class StreamTest {


    //一阳穿三线个股
    @Test
    public void yiyinsanyang() {
//        List<Stock> stocks = TestCaseGenerator.generateStocks();
        List<Stock> stocks = update_mongodb.get_stock_between_list(25,20);
        StockToStockWithAttributeMapper attributeMapper = new StockToStockWithAttributeMapper();
        StockToStockWithStockTrendMapper trendMapper = new StockToStockWithStockTrendMapper();

        Predicate<Entry<String, Stock>> predicate = x -> {

            if(x.getValue().getStockTrend().getHistory().isEmpty()) return false;
            List<StockTrend.TrendBlock> history = x.getValue().getStockTrend().getHistory();
            StockTrend.TrendBlock block = history.get(history.size() - 1);
            double close = Double.parseDouble(block.getClose());
            double open = Double.parseDouble(block.getOpen());
            double ma5 = Double.parseDouble(block.getMa5());
            double ma10 = Double.parseDouble(block.getMa10());
            double ma30 = Double.parseDouble(block.getMa30());

            double max = Math.max(close, open);
            double min = Math.min(close, open);

            return close > open && max >= MathUtils.max(ma5, ma10, ma30) && min <= MathUtils.min(ma5, ma10, ma30);

        };

        stocks.parallelStream()
                .map(x -> new Entry<>(x.getStockName(), attributeMapper.andThen(trendMapper).apply(x)))
                .filter(predicate)
                .map(Entry::getKey)
                .forEach(System.out::println);

    }

    //按关键字过滤页面
    @Test
    public void findNewsUcareAbout() {
        List<URL> news = new HuShenNewsRefCollector(HuShenNewsRefCollector.Topic.TOTAL, 2).get();
        List<URL> res = news.parallelStream().filter(new PageKeyFilter("吉翔股份", false)).collect(Collectors.toList());

        List<URL> regexRes = news.parallelStream().filter(new PageKeyFilter("吉翔股份", true)).collect(Collectors.toList());
        for (URL re : regexRes) {
            System.out.println("Regex : " + re);
        }
        for (URL re : res) {
            System.out.println("nonRegex : " + re);
        }
    }


    //创业板股票大V统计 （耗时过长）
/*    @Test
    public void getMarketStockFundTrend() {
        MarketQuotationsRankCollector collector = new MarketQuotationsRankCollector(
                MarketQuotationsRankCollector.StockType.GROWTH_ENTERPRISE_BOARD,
                MarketQuotationsRankCollector.ORDER_BY_VOLUME, 500);
        StockToVIPFollowerCountEntryMapper mapper1 = new StockToVIPFollowerCountEntryMapper(3000, 300);//搜集每个股票的粉丝
        UserInfoToDBAcceptor acceptor = new UserInfoToDBAcceptor();//写入数据库
        collector.get()
                .parallelStream() //并行流
                .map(mapper1)
                .forEach(acceptor);//结果写入数据库
    }*/


    //统计股票5000粉以上大V个数，并以行业分类股票 （耗时过长）
 /*   @Test
    public void getStocksWithVipFollowersCount() {
        CommissionIndustryCollector collector = new CommissionIndustryCollector();//搜集所有行业
        IndustryToStocksMapper mapper = new IndustryToStocksMapper();//搜集每个行业所有股票
        StockToVIPFollowerCountEntryMapper mapper1 = new StockToVIPFollowerCountEntryMapper(5000, 300);//搜集每个股票的粉丝
        UserInfoToDBAcceptor acceptor = new UserInfoToDBAcceptor();//写入数据库

        List<Entry<Stock, Integer>> res = collector.get()
                .parallelStream() //并行流
                .map(mapper)
                .flatMap(Collection::stream)
                .map(mapper1)
                .peek(acceptor)
                .collect(Collectors.toList());
        for (Entry<Stock, Integer> re : res) {
            System.out.println(re.getKey().getStockName() + " -> 5000粉丝以上大V个数  " + re.getValue());
        }
    }*/

    //最赚钱组合最新持仓以及收益走势、大盘走势
    @Test
    public void MostProfitableCubeDetail() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, Calendar.OCTOBER, 20);
        Date from = calendar.getTime();
        calendar.set(2021, Calendar.FEBRUARY, 3);
        Date to = calendar.getTime();
        MostProfitableCubeCollector cubeCollector = new MostProfitableCubeCollector( MostProfitableCubeCollector.Market.CN,
                MostProfitableCubeCollector.ORDER_BY.DAILY);
        CubeToCubeWithLastBalancingMapper mapper = new CubeToCubeWithLastBalancingMapper();
        CubeToCubeWithTrendMapper mapper1 = new CubeToCubeWithTrendMapper(from, to);
        List<Cube> cubes = cubeCollector.get().parallelStream().map(mapper.andThen(mapper1)).collect(Collectors.toList());
        for (Cube cube : cubes) {
            System.out.print(cube.getSymbol()+":"+cube.getName() + " 日收益: " + cube.getDaily_gain()+"\n");
//            System.out.println(" 最新持仓 " + cube.getRebalancing().getHistory().get(1).toString());
        }
    }


    //获取热股榜股票信息
    @Test
    public void HotRankStockDetail() {
        StockScopeHotRankCollector collector = new StockScopeHotRankCollector(StockScopeHotRankCollector.Scope.SH_SZ_WITHIN_24_HOUR);
        StockToStockWithAttributeMapper mapper1 = new StockToStockWithAttributeMapper();
        StockToStockWithStockTrendMapper mapper2 = new StockToStockWithStockTrendMapper();
        StockToStockWithCompanyInfoMapper mapper3 = new StockToStockWithCompanyInfoMapper();
        List<Stock> stocks = collector.get().parallelStream().map(mapper1.andThen(mapper2)).filter(Objects::nonNull).collect(Collectors.toList())
                .stream().map(mapper3).filter(Objects::nonNull).collect(Collectors.toList());
        for (Stock stock : stocks) {
            System.out.print(stock.getStockName() + " -> ");
            System.out.print(stock.getAmplitude() + " " + stock.getOpen() + " " + stock.getHigh() + " and so on...");
            System.out.println(" trend size: " + stock.getStockTrend().getHistory().size());
            System.out.println(" majorbiz: " + stock.getCompanyInfo().getMajorbiz());
        }
    }


    //获得某个行业所有股票的详细信息和历史走势 比如畜牧业
    @Test
    public void IndustryStockDetail() {

        CommissionIndustryCollector collector = new CommissionIndustryCollector();
        IndustryToStocksMapper mapper = new IndustryToStocksMapper();
        StockToStockWithAttributeMapper mapper1 = new StockToStockWithAttributeMapper();
        StockToStockWithStockTrendMapper mapper2 = new StockToStockWithStockTrendMapper();
        Map<Industry, List<Stock>> res = collector.get()
                .parallelStream()
                .filter(x -> x.getIndustryName().equals("稀有金属"))
                .map(mapper)
                .flatMap(Collection::stream)
                .map(mapper1.andThen(mapper2))
                .collect(Collectors.groupingBy(Stock::getIndustry));

        for (Map.Entry<Industry, List<Stock>> entry : res.entrySet()) {
            for (Stock stock : entry.getValue()) {
                System.out.print(entry.getKey().getIndustryName() + " -> " + stock.getStockName() + " -> ");
                System.out.print(stock.getAmount() + " " + stock.getChange() + " " + stock.getDividend() + " and so on...");
                System.out.println(" trend size: " + stock.getStockTrend().getHistory().size());
            }
        }

    }


    //按行业分类获取所有股票
    @Test
    public void IndustryStockInfo() {

        CommissionIndustryCollector collector = new CommissionIndustryCollector();
        IndustryToStocksMapper mapper = new IndustryToStocksMapper();
        Map<Industry, List<Stock>> res = collector.get()
                .parallelStream()
                .map(mapper)
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(Stock::getIndustry));

        for (Map.Entry<Industry, List<Stock>> entry : res.entrySet()) {
            for (Stock stock : entry.getValue()) {
                System.out.println(entry.getKey().getIndustryName() + " -> " + stock.getStockName());
            }
        }

    }



    //游资追踪
    @Test
    public void LongHuBangTracking() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2020, Calendar.JANUARY, 1);
        Date from = calendar.getTime();
        calendar.set(2021, Calendar.FEBRUARY, 20);
        Date to = calendar.getTime();
        DateRangeCollector collector = new DateRangeCollector(from, to);
        DateToLongHuBangStockMapper mapper = new DateToLongHuBangStockMapper();
        StockToLongHuBangMapper mapper1 = new StockToLongHuBangMapper();
        List<LongHuBangInfo> s = collector.get()
                .parallelStream()
                .map(mapper)
                .flatMap(List::stream).map(mapper1)
                .filter(x -> x.bizsunitInBuyList("路", true))
                .sorted(Comparator.comparing(LongHuBangInfo::getDate))
                .collect(Collectors.toList());
        for (LongHuBangInfo info : s) {
            System.out.println(info.getDate() + " -> " + info.getStock().getStockName());
        }

    }

    // 某只股票下的热帖过滤出大V评论
    @Test
    public void CommentReduce() {
        List<PostInfo> sh603399 = new StockCommentCollector("SH603399", StockCommentCollector.SortType.alpha, 1, 10)
                .get()
                .stream()
                .map(new CommentSetMapper<>())
                .collect(Collectors.toList());
        for (PostInfo postInfo : sh603399) {
            for (Comment comment : postInfo.getComments()) {
                if (Integer.parseInt(comment.getUser().getFollowers_count()) > 10000) {
                    System.out.println(comment.getText());
                }
            }
        }
    }

    // 某只票帖子里大V参与的讨论
    @Test
    public void NewComment() {
        for (int i = 0; i < 10; i++) {
            List<PostInfo> sh688180 = new StockCommentCollector("SH688180", StockCommentCollector.SortType.time, i+1, 10)
                    .get()
                    .stream()
                    .map(new CommentSetMapper<>())
                    .collect(Collectors.toList());
            for (PostInfo postInfo : sh688180) {
                for (Comment comment : postInfo.getComments()) {
                    int followerCnt = Integer.parseInt(comment.getUser().getFollowers_count());
                    if (followerCnt > 10000) {
                        System.out.println(comment.getUser().getScreen_name() + "  " + followerCnt + "  " + comment.getText());
                    }
                }
            }
        }
    }

    @Test
    public void UserComment() {
        List<PostInfo> collect = new UserCommentCollector("1598715146", 1, 2, 20).get()
                .stream().map(new CommentSetMapper<>()).collect(Collectors.toList());
        for (PostInfo postInfo : collect) {
            for (Comment comment : postInfo.getComments()) {
                if (comment.getUser_id().equalsIgnoreCase("1598715146")) {
                    System.out.println(comment.getText());
                }
            }
        }
    }

}
