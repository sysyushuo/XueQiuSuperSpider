package org.decaywood.mapper.industryFirst;

import com.fasterxml.jackson.databind.JsonNode;
import org.decaywood.entity.Industry;
import org.decaywood.entity.Stock;
import org.decaywood.mapper.AbstractMapper;
import org.decaywood.timeWaitingStrategy.TimeWaitingStrategy;
import org.decaywood.utils.EmptyObject;
import org.decaywood.utils.RequestParaBuilder;
import org.decaywood.utils.URLMapper;

import java.net.URL;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: decaywood
 * @date: 2015/11/23 14:04
 */

/**
 * 行业 -> 该行业股票 映射器
 */
public class IndustryToStocksMapper extends AbstractMapper<Industry, List<Stock>> {

    /**
     * @param strategy 超时等待策略（null则设置为默认等待策略）
     */
    public IndustryToStocksMapper(TimeWaitingStrategy strategy) {
        super(strategy);
    }

    public IndustryToStocksMapper() {
        this(null);
    }

    @Override
    public List<Stock> mapLogic(Industry industry) throws Exception {

        if(industry == null || industry == EmptyObject.emptyIndustry) return new ArrayList<>();

        String target = URLMapper.INDUSTRY_JSON.toString();
        RequestParaBuilder builder = new RequestParaBuilder(target);
        builder.addParameter("page", 1)
                .addParameter("size", 500)
                .addParameter("order", "desc")
                .addParameter("order_by", "percent")
                .addParameter("exchange", "CN")
                .addParameter("market", "CN");
        String info = industry.getIndustryInfo();
        if (info.startsWith("#")) info = info.substring(1);
        for (String s : info.split("&")) {
            String[] keyAndVal = s.split("=");
            if ("level2code".equalsIgnoreCase(keyAndVal[0])) {
                builder.addParameter("ind_code", keyAndVal[1]);
            }
        }
        builder.addParameter("_", System.currentTimeMillis());
        URL url = new URL(builder.build());

        String json = request(url);
        JsonNode jsonNode = mapper.readTree(json);

        return parserJson(jsonNode,industry);

    }


    private List<Stock> parserJson(JsonNode node,Industry industry) {

        List<Stock> stocks = new ArrayList<>();

        JsonNode data = node.get("data");
        for (JsonNode jsonNode : data.get("list")) {
            Stock stock = new Stock(jsonNode.get("name").asText(), jsonNode.get("symbol").asText());
            stock.setIndustry(industry);
            stocks.add(stock);
        }
        return stocks;

    }


}
