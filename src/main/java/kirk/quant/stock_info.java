package kirk.quant;

import com.fasterxml.jackson.databind.JsonNode;
import org.decaywood.entity.LongHuBangInfo;
import org.decaywood.entity.Stock;
import org.decaywood.mapper.stockFirst.StockToLongHuBangMapper;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class stock_info {
    public static List<LongHuBangInfo> get_longhu_info(Stock stock) throws Exception {
        StockToLongHuBangMapper mapper = new StockToLongHuBangMapper();
        JsonNode date_list = mapper.get_list(stock);
        Calendar calendar = Calendar.getInstance();
        List<LongHuBangInfo> longhu_info =  new ArrayList<>();
        date_list.forEach(tmp->{
            calendar.set(Integer.parseInt(tmp.asText().substring(0,4)),Integer.parseInt(tmp.asText().substring(4,6))-1,Integer.parseInt(tmp.asText().substring(6,8)));
            stock.setStockQueryDate(calendar.getTime());
            LongHuBangInfo res = null;
            try {
                res = mapper.mapLogic(stock);
            } catch (Exception e) {
                e.printStackTrace();
            }
            longhu_info.add(res);
        });

        return longhu_info;
    }

    public static  void main(String[] args) throws Exception {
        Stock stock=new Stock("吉翔股份","SH603399");

        List<LongHuBangInfo> res = get_longhu_info(stock);
        res.forEach(x->{
            x.getTopBuyList().forEach(y-> {
                System.out.println(y.getBizsunitname()+" buy "+x.getStock().getStockName()+y.getBuyamt()+ " at "+y.getTradedate());
                });
            x.getTopSaleList().forEach(y-> {
                System.out.println(y.getBizsunitname()+" sell "+x.getStock().getStockName()+y.getSaleamt()+ " at "+y.getTradedate());
            });
        });
    }
}
