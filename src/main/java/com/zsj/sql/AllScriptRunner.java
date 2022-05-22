package com.zsj.sql;

import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 运行多个脚本的调用器
 * <p>
 * run.conf说明
 * -------------------------------
 * 目标路径
 * 错误时停止（y）
 * 无需确认继续（y)
 * 数据库连接JDBC-URL
 * 数据库用户
 * 数据密码
 * 【0】第0个.sh脚本需要执行(y)
 * 【0】第0个.sh脚本的运行参数
 * 【1】第1个.sh脚本需要执行(y)
 * 【1】第1个.sh脚本的运行参数
 * ...
 * -------------------------------
 *
 * @author zhongshijie
 * @create 2022/3/2 21:19
 */
public class AllScriptRunner {

    /* 预定义的常量 */
    private static final String YES = "y";
    private static final String NO = "n";
    private static final String TAG_DATETIME = "yyyy-MM-dd HH:mm:ss";
    private static final String TAG_SQL_FILE = ".sql";
    private static final String TAG_SH_FILE = ".sh";
    private static final String TAG_PROC_SQL_FILE = "proc.sql";
    private static final String TAG_MULTI = "multi";
    private static final String TAG_LINE = "-----------";
    private static final String TAG_RUN_CONF = "run.conf";
    private static final String TXT_INPUT = "请输入 [%s]: ";
    private static final String TXT_INPUT_WRONG = "输入错误，请重新输入: ";
    private static final String TXT_GET_FILES = "获取 [%s] [%s] 文件来自 [%s]";
    private static final String TXT_SOME_EXP = "这里有异常: [%s][%s]";
    private static final String TXT_COMMENT = "-- 此处有多行注释被替换(from AllScriptRunner)";
    private static final String TXT_TO = ">";
    private static final String TXT_START = "%s" + TXT_TO + "START";
    private static final String TXT_END = "%s" + TXT_TO + "END";
    private static final String TXT_FILE_LOST = "文件不存在了";
    private static final String TXT_SURE_TO_RUN = "确定你的选择 %s ('y'或'n'): ";
    private static final String TXT_MULTI_START = TAG_LINE + TAG_MULTI + TAG_LINE;
    private static final String TXT_MULTI_END = TAG_LINE + TAG_LINE + TAG_LINE;
    private static final String TXT_PARAMS = "参数";
    private static final String TXT_STOP_BY = "由于运行错误在此处停下-[%s]!";
    private static final String TXT_DONT_STOP = "我们没有停止（因为你设置不停止）即便应该";
    private static final String TXT_STOP_ON_ERR = "错误时停止";
    private static final String TXT_NEED_AUTO = "无需确认继续";
    private static final String TXT_TARGET_PATH = "目标路径";
    private static final String TXT_RENAME = "更改文件名";
    private static final String TXT_SURE_CONTINUE = "确定继续吗？（'y'或'n'）";
    private static final String TXT_URL = "数据库URL";
    private static final String TXT_USR = "用户名";
    private static final String TXT_PWD = "密码";
    private static final String TXT_RUN_FAIL = "运行失败-[%s]";
    /* 全局变量：需要替换的脚本 */
    private static final Map<String, String> NEED_REPLACE_STR = new HashMap<>() {
        {
            put("EXEC ", "call ");
            put("exec ", "call ");
        }
    };
    /* 全局变量：是否在错误时停止 */
    private static String stopOnError = NO;
    /* 全局变量：输入捕获器 */
    private static Scanner sc = new Scanner(System.in);

    /**
     * 程序入口
     *
     * @param args 运行参数
     * @throws Exception 任意异常
     */
    public static void main(String[] args) throws Exception {
        AllTask allTask = new AllTask();
        allTask.run();
    }

    /**
     * 公共交互类
     */
    static class Mutual {
        /**
         * 获取当前时间的字符串
         *
         * @return 当前时间的字符串
         */
        public static String nowStr() {
            SimpleDateFormat sdf = new SimpleDateFormat(TAG_DATETIME);
            return sdf.format(Calendar.getInstance().getTime());
        }

        /**
         * 打印日志
         *
         * @param msg   打印消息（可支持占位符）
         * @param items 打印消息占位符的打印项目
         */
        public static void log(String msg, Object... items) {
            System.out.printf("%s%s%s\n", nowStr(), TXT_TO, String.format(msg, items));
        }

        /**
         * 获取输入
         *
         * @param inputName 输入的名称
         * @return 用户的输入
         */
        public static String getInput(String inputName) {
            log(String.format(TXT_INPUT, inputName));
            return sc.nextLine();
        }
    }

    /**
     * SQL任务执行器
     */
    static class SqlTask {

        /* SQL任务执行器所需的常量 */
        private final String url;
        private final String usr;
        private final String pwd;
        private final List<String> fileNames = new ArrayList<>();

        /**
         * 初始化SQL任务
         *
         * @param path SQL文件根目录
         * @param url  数据库URL
         * @param usr  数据库用户名
         * @param pwd  数据库密码
         */
        public SqlTask(String path, String url, String usr, String pwd) {
            // 赋值数据库信息
            this.url = url;
            this.usr = usr;
            this.pwd = pwd;
            // 获取所有SQL文件
            getAllSql(path);
            // 对所有文件进行排序
            fileNames.sort(String::compareTo);
            // 替换文件中需要被替换的文本
            changeNeedReplace();
            // 打印本次执行期将执行的文件数
            Mutual.log(String.format(TXT_GET_FILES, fileNames.size(), TAG_SQL_FILE, path));
        }

        /**
         * 获取所有SQL文件到常量中
         *
         * @param path 本次任务检索SQL文件的目录
         */
        private void getAllSql(String path) {
            // 列出目录下的所有文件
            File file = new File(path);
            File[] fs = file.listFiles();
            // 目录下没有文件，直接退出
            if (fs == null) return;
            // 遍历目录下的所有文件
            Arrays.stream(fs).forEach(f -> {
                String fPath = f.getPath();
                // 若为文件夹，递归本函数
                if (f.isDirectory()) getAllSql(fPath);
                // 若为SQL文件，则加入任务文件列表
                if (f.isFile() && fPath.toLowerCase().endsWith(TAG_SQL_FILE)) fileNames.add(f.getAbsolutePath());
            });
        }

        /**
         * 将已读取的文件进行SQLPlus语法替换为标准SQL语法
         */
        private void changeNeedReplace() {
            for (String fileName : fileNames) {
                // 跳过以存储过程符分割的文件无需替换
                if (fileName.toLowerCase().endsWith(TAG_PROC_SQL_FILE)) continue;
                // 应替换字符的替换
                NEED_REPLACE_STR.forEach((k, v) -> replaceStr(new File(fileName), k, v));
            }
        }

        /**
         * 替换给定文件中的指定文本
         *
         * @param file   文件
         * @param oldStr 旧文本
         * @param newStr 新文本
         */
        private void replaceStr(File file, String oldStr, String newStr) {
            boolean isOK = true;
            try {
                // 创建对目标文件读取流
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                // 存储对目标文件读取的内容
                String targetStr;
                StringBuilder tmpStr = new StringBuilder();
                while ((targetStr = br.readLine()) != null) {
                    tmpStr.append(targetStr);
                    tmpStr.append("\n");
                }
                targetStr = tmpStr.toString();
                // 创建临时文件
                File tmpF = new File("tmpFile");
                if (!tmpF.exists()) {
                    isOK = tmpF.createNewFile();
                }
                // 创建对临时文件输出流，并追加
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpF, true)));
                // 若目标文件包含了指定字段，则替换
                if (targetStr.contains(oldStr)) {
                    targetStr = targetStr.replace(oldStr, newStr);
                }
                // 若目标文件包含了注释，则去掉
                if (targetStr.contains("/*")) {
                    String patStr = "/\\*[\\w\\W]*?\\*/|//.*";
                    Pattern pattern = Pattern.compile(patStr);
                    Matcher matcher = pattern.matcher(targetStr);
                    targetStr = matcher.replaceAll(TXT_COMMENT);
                }
                // 写入临时文件
                bw.write(targetStr);
                // 关闭流
                br.close();
                bw.close();
                //删除源文件，重命名新文件
                String filePath = file.getPath();
                isOK = file.delete();
                isOK = tmpF.renameTo(new File(filePath));
            } catch (Exception e) {
                Mutual.log(TXT_SOME_EXP, file.getPath(), e.getMessage());
            }
            if (!isOK) {
                Mutual.log(TXT_SOME_EXP, file.getPath(), TXT_RENAME);
            }
        }

        /**
         * 检查脚本是否有报错
         *
         * @param file 报错日志文件
         * @return 是否无报错
         */
        private boolean isNoError(File file) {
            if (file.length() > 0) {
                if (stopOnError.equals(YES)) {
                    return false;
                }
                Mutual.log(TXT_DONT_STOP + TXT_STOP_BY, file.getPath());
            }
            return true;
        }

        /**
         * 单个执行SQL脚本
         *
         * @param fileName 文件名
         * @throws Exception 抛出任意异常
         */
        private void singleExec(String fileName) throws Exception {
            Mutual.log(TXT_START, fileName);
            // 若文件不存在，抛出异常阻止继续
            File file = new File(fileName);
            if (!file.exists()) {
                throw new Exception(TXT_FILE_LOST);
            }
            // 准备日志文件名
            String parentPath = file.getParent();
            String baseName = file.getName().replaceAll("[.][^.]+$", "");
            String errorLog = String.format("%s_err.log", baseName);
            String runLog = String.format("%s_run.log", baseName);
            // 运行脚本
            try (Connection conn = DriverManager.getConnection(this.url, this.usr, this.pwd)) {
                ScriptRunner runner = new ScriptRunner(conn);
                if (fileName.toLowerCase().contains(TAG_PROC_SQL_FILE)) {
                    runner.setDelimiter("/");
                }
                runner.setStopOnError(false);
                runner.setAutoCommit(true);
                runner.setLogWriter(new PrintWriter(new File(parentPath, runLog)));
                runner.setErrorLogWriter(new PrintWriter(new File(parentPath, errorLog)));
                runner.runScript(new FileReader(file));
                Mutual.log(TXT_END, fileName);
            } catch (Exception e) {
                Mutual.log(TXT_SOME_EXP, fileName, e.getMessage());
            }
            if (!isNoError(new File(parentPath, errorLog))) {
                throw new Exception(String.format(TXT_STOP_BY, fileName));
            }
        }

        /**
         * 并发执行SQL脚本
         *
         * @param fileNames 文件名列表
         */
        private void runMulti(List<String> fileNames) throws Exception {
            Mutual.log(TXT_MULTI_START);
            // 为每个SQL创建线程并执行
            Vector<Thread> threadVector = new Vector<>();
            List<String> errorFileName = new ArrayList<>();
            for (String fileName : fileNames) {
                Thread thread = new Thread(() -> {
                    try {
                        singleExec(fileName);
                    } catch (Exception e) {
                        errorFileName.add(fileName);
                    }
                });
                threadVector.add(thread);
                thread.start();
            }
            // 等待所有线程的结束
            for (Thread thread : threadVector) {
                try {
                    thread.join();
                } catch (Exception e) {
                    Mutual.log(e.getMessage());
                }
            }
            Mutual.log(TXT_MULTI_END);
            if (errorFileName.size() != 0) {
                throw new Exception(String.format(TXT_STOP_BY, String.join(",", errorFileName)));
            }
        }

        /**
         * 运行脚本
         */
        public void run() throws Exception {
            List<String> multiParts = new ArrayList<>();
            for (String fileName : fileNames) {
                // 添加并发任务（甄别是否用同一部分已在排队）
                boolean isSameMultiPart = false;
                if (!multiParts.isEmpty()) {
                    isSameMultiPart = multiParts.get(0).split(TAG_MULTI)[0].equals(fileName.split(TAG_MULTI)[0]);
                }
                if (fileName.contains(TAG_MULTI) && (multiParts.isEmpty() || isSameMultiPart)) {
                    multiParts.add(fileName);
                    continue;
                }
                // 并发任务上阵
                if (!multiParts.isEmpty()) {
                    runMulti(multiParts);
                    multiParts.clear();
                }
                // 添加并发任务
                if (fileName.contains(TAG_MULTI)) {
                    multiParts.add(fileName);
                    continue;
                }
                // 单独执行任务
                singleExec(fileName);
            }
            // 并发任务上阵
            if (!multiParts.isEmpty()) {
                runMulti(multiParts);
                multiParts.clear();
            }
        }
    }

    /**
     * Shell任务执行器
     */
    static class ShellTask {

        private final List<String> fileNames = new ArrayList<>();

        public ShellTask(String path) {
            getAllSh(new File(path));
            fileNames.sort(String::compareTo);
        }

        private void getAllSh(File file) {
            File[] fs = file.listFiles();
            if (fs == null) {
                return;
            }
            for (File f : fs) {
                if (f.isDirectory()) {
                    getAllSh(f);
                }
                if (f.isFile() && f.getPath().toLowerCase().endsWith(TAG_SH_FILE)) {
                    fileNames.add(f.getAbsolutePath());
                }
            }
        }

        private String shell(String fileName, String paramsStr) {
            StringBuilder sb = new StringBuilder();
            try {
                Process ps = Runtime.getRuntime().exec(
                        String.format("sh %s %s", fileName, paramsStr),
                        null,
                        new File(new File(fileName).getParent())
                );
                BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line).append("\n");
                }
                return sb.toString();
            } catch (Exception e) {
                sb.append(e.getMessage());
            }
            return String.format(TXT_RUN_FAIL, sb);
        }

        public void run() {
            String keyIn;
            boolean keyOk = false;
            for (String fileName : fileNames) {
                do {
                    keyIn = Mutual.getInput(String.format(TXT_SURE_TO_RUN, fileName)).toLowerCase();
                    switch (keyIn) {
                        case YES:
                            keyIn = Mutual.getInput(TXT_PARAMS);
                            Mutual.log(shell(fileName, keyIn));
                            keyOk = true;
                            break;
                        case NO:
                            keyOk = true;
                            break;
                        default:
                            Mutual.log(TXT_INPUT_WRONG);
                            break;
                    }
                } while (!keyOk);
            }
        }
    }

    /**
     * 任务调度器
     */
    static class AllTask {

        private final List<String> dirs = new ArrayList<>();

        public AllTask() throws IOException {
            File conf = new File(TAG_RUN_CONF);
            if (conf.exists()) {
                sc = new Scanner(Paths.get(TAG_RUN_CONF));
            }
            String path = Mutual.getInput(TXT_TARGET_PATH);
            for (File file : Objects.requireNonNull(new File(path).listFiles())) {
                if (file.isDirectory()) {
                    dirs.add(file.getAbsolutePath());
                }
            }
            dirs.sort(String::compareTo);
        }

        public void run() throws Exception {
            stopOnError = Mutual.getInput(String.format(TXT_SURE_TO_RUN, TXT_STOP_ON_ERR)).toLowerCase();
            String url = Mutual.getInput(TXT_URL);
            String usr = Mutual.getInput(TXT_USR);
            String pwd = Mutual.getInput(TXT_PWD);
            String needAuto = Mutual.getInput(String.format(TXT_SURE_TO_RUN, TXT_NEED_AUTO)).toLowerCase();
            for (String dir : dirs) {
                boolean keyOk = false;
                Mutual.log(dir);
                do {
                    String keyIn = needAuto.equals(NO) ? Mutual.getInput(TXT_SURE_CONTINUE).toLowerCase() : YES;
                    switch (keyIn) {
                        case YES:
                            // 查看和运行Shell
                            ShellTask shellTask = new ShellTask(dir);
                            shellTask.run();
                            // 查看和运行SQL
                            SqlTask sqlTask = new SqlTask(dir, url, usr, pwd);
                            sqlTask.run();
                            keyOk = true;
                            break;
                        case NO:
                            keyOk = true;
                            break;
                        default:
                            Mutual.log(TXT_INPUT_WRONG);
                            break;
                    }
                } while (!keyOk);

            }
        }
    }

}
