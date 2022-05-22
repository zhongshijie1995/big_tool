package com.zsj.sql;

import joinery.DataFrame;
import lombok.Data;
import org.apache.ibatis.jdbc.SqlRunner;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataCompare {

    public static final String DATA_COMPARE_YAML = "DataCompare.yaml";
    public static final String COMPARE_SQL = "select {0} from {1}@{2} where {3} minus select {4} from {5} where {6}";
    public static final String SELECT_SQL = "select {0} from {1}{2} where {3}";
    public static final String COMPARE_COUNT = "select count(*) as count from ({0})\n";

    public static void main(String[] args) {
        DataCompare dataCompare = new DataCompare();
        dataCompare.execCompare(dataCompare.getCompareSQLMap(dataCompare.getConfig(null)));
    }

    public Config getConfig(String filePath) {
        InputStream inputStream;
        filePath = null == filePath ? DATA_COMPARE_YAML : filePath;
        inputStream = this.getClass().getClassLoader().getResourceAsStream(filePath);
        if (null == inputStream) return null;
        Yaml yaml = new Yaml(new Constructor(Config.class));
        return yaml.load(inputStream);
    }

    public String genCompareSQL(Config.Tab tab, Config.DB db) {
        return MessageFormat.format(
                COMPARE_SQL,
                tab.getColNames(),
                tab.getTabName(),
                db.getDbLinkName(),
                tab.getDiffCols() == null ? "1=1" : tab.getDiffCols(),
                tab.getColNames(),
                tab.getTabName(),
                tab.getDiffCols() == null ? "1=1" : tab.getDiffCols()
        );
    }

    public String genCountSQL(String sql) {
        return MessageFormat.format(COMPARE_COUNT, sql);
    }

    public Map<String, Map<Config.DB, String>> getCompareSQLMap(Config config) {
        Map<String, Map<Config.DB, String>> compareSQLMap = new HashMap<>();

        Config.DB dbA = config.getDbs().getA();
        Config.DB dbB = config.getDbs().getB();

        for (Config.Tab tab : config.getTabs()) {
            Map<Config.DB, String> querySqlMap = new HashMap<>();

            querySqlMap.put(dbA, genCompareSQL(tab, dbB));
            querySqlMap.put(dbB, genCompareSQL(tab, dbA));

            compareSQLMap.put(tab.getTabName(), querySqlMap);
        }

        return compareSQLMap;
    }

    public void execCompare(Map<String, Map<Config.DB, String>> compareSQLMap) {
        DataFrame<Object> df = new DataFrame<>("表名", "数据主体", "独有数据条数", "详情SQL");
        compareSQLMap.forEach((tab, queryMap) -> {
            queryMap.forEach((db, sql) -> {
                String countCompareSQL = genCountSQL(sql);
                int diffCount = queryOne(db, countCompareSQL);
                df.append(Arrays.asList(tab, db.getName(), diffCount, sql));
            });
        });
        try {
            String now = new SimpleDateFormat("yyMMddHHmm").format(new Date(System.currentTimeMillis()));
            df.writeCsv(MessageFormat.format("DataCompareResult-{0}.csv", now));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int queryOne(Config.DB db, String sql) {
        int result = 0;
        try (Connection conn = DriverManager.getConnection(db.getUrl(), db.getUsr(), db.getPwd())) {
            SqlRunner runner = new SqlRunner(conn);
            Map<String, Object> queryResult = runner.selectOne(sql);
            result = Integer.parseInt(queryResult.get("COUNT").toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Data
    public static class Config {
        private DB conn;
        private DBs dbs;
        private List<String> diff;
        private List<Tab> tabs;

        @Data
        public static class DBs {
            private DB a;
            private DB b;
        }

        @Data
        public static class DB {
            private String name;
            private String dbLinkName;
            private String url;
            private String usr;
            private String pwd;
        }

        @Data
        public static class Tab {
            private String tabName;
            private String colNames;
            private String diffCols;
        }
    }
}
