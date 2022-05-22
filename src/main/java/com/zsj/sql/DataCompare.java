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
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class DataCompare {

    public static final String DATA_COMPARE_YAML = "DataCompare.yaml";
    public static final String DATA_COMPARE_CSV = "DataCompare.csv";
    public static final String SELECT_SQL = "select {0} from {1}{2} where {3}";
    public static final String MINUS_SQL = "({0}) minus ({1})";
    public static final String COMPARE_COUNT = "select count(*) as count from ({0})";

    private Connection connection;

    public static void main(String[] args) {
        DataCompare dataCompare = new DataCompare();
        Config config = dataCompare.getConfig(null);

        List<Config.Tab> tabs = new ArrayList<>();
        DataFrame<Object> df = dataCompare.getDataCompareCsv(null);
        for (int i = 0; i < df.length(); i++) {
            Config.Tab tab = new Config.Tab();
            tab.setTabName(df.get(i, 0).toString());
            tab.setColNames(df.get(i, 1).toString());
            tab.setDiffCols(df.get(i, 2) != null ? df.get(i, 2).toString() : null);
            tabs.add(tab);
        }
        config.setTabs(tabs);

        dataCompare.execCompare(dataCompare.getCompareSQLMap(config), config);
    }

    public Config getConfig(String filePath) {
        InputStream inputStream;
        filePath = null == filePath ? DATA_COMPARE_YAML : filePath;
        inputStream = this.getClass().getClassLoader().getResourceAsStream(filePath);
        if (null == inputStream) return null;
        Yaml yaml = new Yaml(new Constructor(Config.class));
        return yaml.load(inputStream);
    }

    public DataFrame<Object> getDataCompareCsv(String filePath) {
        InputStream inputStream;
        filePath = null == filePath ? DATA_COMPARE_CSV : filePath;
        inputStream = this.getClass().getClassLoader().getResourceAsStream(filePath);
        if (null == inputStream) return null;
        DataFrame<Object> df = null;
        try {
            df = DataFrame.readCsv(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return df;
    }

    public Map<String, String> genCompareSQL(Config.Tab tab, Config.DB a, Config.DB b) {
        Map<String, String> selectSqlMap = new HashMap<>();

        String selectSqlA = MessageFormat.format(SELECT_SQL,
                tab.getColNames(),
                tab.getTabName(),
                a.getDbLinkName() != null ? a.getDbLinkName() : "",
                tab.getDiffCols() != null ? tab.getDiffCols() : "1=1"
        );
        String selectSqlB = MessageFormat.format(SELECT_SQL,
                tab.getColNames(),
                tab.getTabName(),
                b.getDbLinkName() != null ? b.getDbLinkName() : "",
                tab.getDiffCols() != null ? tab.getDiffCols() : "1=1"
        );
        selectSqlMap.put(
                a.getName() + "-" + b.getName(),
                MessageFormat.format(MINUS_SQL, selectSqlA, selectSqlB)
        );
        selectSqlMap.put(
                b.getName() + "-" + a.getName(),
                MessageFormat.format(MINUS_SQL, selectSqlB, selectSqlA)
        );
        return selectSqlMap;
    }

    public String genCountSQL(String sql) {
        return MessageFormat.format(COMPARE_COUNT, sql);
    }

    public Map<String, Map<String, String>> getCompareSQLMap(Config config) {
        Map<String, Map<String, String>> compareSqlMap = new HashMap<>();
        for (Config.Tab tab : config.getTabs()) {
            compareSqlMap.put(tab.getTabName(), genCompareSQL(tab, config.getA(), config.getB()));
        }
        return compareSqlMap;
    }

    public void execCompare(Map<String, Map<String, String>> compareSQLMap, Config config) {
        init_conn(config);
        DataFrame<Object> df = new DataFrame<>("表名", "数据情况", "独有数据条数", "详情SQL");
        compareSQLMap.forEach((tab, queryMap) -> {
            Logger.getLogger("").info("正在处理：" + tab);
            queryMap.forEach((type, sql) -> {
                df.append(Arrays.asList(tab, type, queryOne(genCountSQL(sql)), sql));
            });
        });
        close_conn();
        try {
            String now = new SimpleDateFormat("yyMMddHHmm").format(new Date(System.currentTimeMillis()));
            df.writeCsv(MessageFormat.format("DataCompareResult-{0}.csv", now));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void init_conn(Config config) {
        Config.DB db = config.getConn();
        try {
            connection = DriverManager.getConnection(db.getUrl(), db.getUsr(), db.getPwd());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close_conn() {
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private int queryOne(String sql) {
        int result = 0;
        SqlRunner runner = new SqlRunner(connection);
        Map<String, Object> queryResult = null;
        try {
            queryResult = runner.selectOne(sql);
        } catch (SQLException e) {
            result = -1;
        }
        result = Integer.parseInt(queryResult.get("COUNT").toString());
        return result;
    }

    @Data
    public static class Config {
        private DB conn;
        private DB a;
        private DB b;
        private List<Tab> tabs;

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
