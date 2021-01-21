package org.decaywood.mapper.stockFirst;

import com.fasterxml.jackson.databind.JsonNode;
import org.decaywood.entity.Entry;
import org.decaywood.entity.Stock;
import org.decaywood.mapper.AbstractMapper;
import org.decaywood.timeWaitingStrategy.TimeWaitingStrategy;
import org.decaywood.utils.EmptyObject;
import org.decaywood.utils.RequestParaBuilder;
import org.decaywood.utils.URLMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;
import java.rmi.RemoteException;

/**
 * @author: decaywood
 * @date: 2015/11/30 16:39
 */

/**
 * 股票 -> 股票+雪球大V数量 映射器
 * 速度很慢，慎用
 */
public class StockToVIPFollowerCountEntryMapper extends AbstractMapper <Stock, Entry<Stock, Integer>> {

//    private static final String REQUEST_PREFIX = URLMapper.MAIN_PAGE + "/S/";
//    private static final String REQUEST_SUFFIX = "/follows?page=";


    private int VIPFriendsCountShreshold;
    private int latestK_NewFollowers;

    public StockToVIPFollowerCountEntryMapper() {
        this(10000, 5);
    }

    public StockToVIPFollowerCountEntryMapper(int VIPFriendsCountShreshold, int latestK_NewFollowers) {
        this(null, VIPFriendsCountShreshold, latestK_NewFollowers);
    }


    /**
     *
     * @param strategy 超时等待策略（null则设置为默认等待策略）
     * @param VIPFriendsCountShreshold 是否为大V的粉丝阈值（超过这个阈值视为大V）
     * @param latestK_NewFollowers 只将最近K个新增用户纳入计算范围
     */
    public StockToVIPFollowerCountEntryMapper(TimeWaitingStrategy strategy,
                                              int VIPFriendsCountShreshold,
                                              int latestK_NewFollowers) {
        super(strategy);
        if(VIPFriendsCountShreshold < 0 || latestK_NewFollowers < 0) throw new IllegalArgumentException();
        this.VIPFriendsCountShreshold = VIPFriendsCountShreshold;
        this.latestK_NewFollowers = latestK_NewFollowers;
    }

    @Override
    public Entry<Stock, Integer> mapLogic(Stock stock) throws Exception {

        if(stock == null || stock == EmptyObject.emptyStock)
            return new Entry<>(EmptyObject.emptyStock, 0);

        String stockNo = stock.getStockNo();

        int count = 0;

        String target = URLMapper.FOLLOWER_JSON.toString();
        RequestParaBuilder builder = new RequestParaBuilder(target)
                .addParameter("symbol",stockNo)
                .addParameter("page",1)
                .addParameter("size",20)
                .addParameter("anonymous_filter",true);
        URL url = new URL(builder.build());
        String content_tmp = request(url);
        JsonNode node = mapper.readTree(content_tmp).get("data").get("followers");

        for (int i = 0; i < node.size()-1; i++) {
            String target_in = URLMapper.FRIEND_FOLLOWER_JSON.toString();
            RequestParaBuilder builder_in = new RequestParaBuilder(target_in)
                    .addParameter("uid",node.get(i).toString())
                    .addParameter("pageNo",1);
            URL url_in = new URL(builder_in.build());
            String content;
            while (true) {
                try {

                    content = request(url_in);
                    break;

                } catch (Exception e) {
                    System.out.println("Mapper: Network busy Retrying");
                }
            }

//            JsonNode node_in = mapper.readTree(content).get("count");
//
//            if(node_in.size() == 0) break;
//
//            for (JsonNode jsonNode : node) {
//                int followersCount = jsonNode.get("count").asInt();
//                if(followersCount > VIPFriendsCountShreshold) count++;
//            }
            Integer node_in = mapper.readTree(content).get("count").asInt();
            if(node_in> VIPFriendsCountShreshold) count++;
        }

        return new Entry<>(stock, count);
    }


    private JsonNode parseHtmlToJsonNode(String content) throws IOException {

        Document doc = Jsoup.parse(content);
        String indexer1 = "follows=";
        String indexer2 = ";seajs.use";
        StringBuilder builder = new StringBuilder(
                doc.getElementsByTag("script")
                .get(15)
                .dataNodes()
                .get(0)
                .attr("data"));
        int index = builder.indexOf(indexer1);
        builder.delete(0, index + indexer1.length());
        index = builder.indexOf(indexer2);
        builder.delete(index, builder.length());
        return mapper.readTree(builder.toString());

    }
}
