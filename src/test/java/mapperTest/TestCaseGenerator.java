package mapperTest;

import org.decaywood.entity.Cube;
import org.decaywood.entity.Industry;
import org.decaywood.entity.Stock;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: decaywood
 * @date: 2015/11/25 19:04
 */
public class TestCaseGenerator {

    public static List<Industry> generateIndustries() {
        List<Industry> industries = new ArrayList<>();
        industries.add(new Industry(" 白色家电","exchange=CN&plate=1_2_32&firstName=1&secondName=1_2&level2code=S3301"));
        industries.add(new Industry("半导体","#exchange=CN&plate=1_2_23&firstName=1&secondName=1_2&level2code=S2701"));
        return industries;
    }


    public static List<Stock> generateStocks() {
        List<Stock> stocks = new ArrayList<>();
        stocks.add(new Stock("银之杰", "SZ300085"));
        stocks.add(new Stock("利德曼","SZ300289"));
        stocks.add(new Stock("国元证券","SZ000728"));
        return stocks;
    }

    public static List<Cube> generateCube() {
        List<Cube> cubes = new ArrayList<>();
        cubes.add(new Cube("xxx", "xxx", "ZH128412")); //沪深组合
        cubes.add(new Cube("xxx", "xxx", "ZH102164")); //港股组合
        cubes.add(new Cube("xxx", "xxx", "ZH739627")); //美股组合
        return cubes;
    }

}
