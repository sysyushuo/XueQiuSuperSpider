package kirk.quant;

import org.decaywood.entity.LongHuBangInfo;
import org.decaywood.entity.Stock;
import org.decaywood.mapper.stockFirst.StockToLongHuBangMapper;

import java.util.Calendar;
import java.util.Date;

public class stock_info {
    public static LongHuBangInfo get_longhu_info(Stock stock) throws Exception {
        StockToLongHuBangMapper mapper = new StockToLongHuBangMapper();
        LongHuBangInfo res = mapper.mapLogic(stock);
        return res;
    }

    public static  void main(String[] args) throws Exception {
        Stock stock=new Stock("吉翔股份","SH603399");
        Calendar calendar = Calendar.getInstance();
        calendar.set(2020,2,25);

        stock.setStockQueryDate(calendar.getTime());
        LongHuBangInfo res = get_longhu_info(stock);
        if (res.getTopBuyList().isEmpty()) {
            System.out.println("res is empty");

        }else {
            System.out.println(res.getTopBuyList());
        }

    }
}
