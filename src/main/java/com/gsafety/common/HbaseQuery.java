package com.gsafety.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: yifeng G
 * @Date: Create in 9:38 2017/8/9 2017
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class HbaseQuery {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    private static Logger logger = LoggerFactory.getLogger(HbaseQuery.class);


    public static void main(String[] args) throws IOException {
        println("Start...");
        listTables();
        insert("test","rowkey1","cf","1");
//        createTable("test", new String[]{"one", "two"});
//        deleteTable("test");
//        getData("BRIDGE_HF_FHDD_DY000001","02293a:HF_FHDD_DY000001:39_7:9223370511632408807","M","values");
//        getData("BRIDGE_HF_FHDD_DY000001","02293a:HF_FHDD_DY000001:39_7:9223370511632408807");

        println("End...");
    }

    /**
     * 初始化链接
     */
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "10.5.4.28,10.5.4.29,10.5.4.39");
//        configuration.set("hbase.master", "hdfs://udap5:60000");
//        configuration.set("hbase.root.dir", "hdfs://udap5:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public static void close() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
                System.out.println("关闭连接......");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param tableName  表名
     * @param colsFamily 列族列表
     * @throws IOException
     */
    public static void createTable(String tableName, String[] colsFamily) throws IOException {
        init();
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            println(tableName + " exists.");
        } else {
            HTableDescriptor hTableDesc = new HTableDescriptor(tName);
            for (String col : colsFamily) {
                HColumnDescriptor hColumnDesc = new HColumnDescriptor(col);
                hTableDesc.addFamily(hColumnDesc);
            }
            admin.createTable(hTableDesc);
        }
        close();
        println("成功创建hbase表.....");
    }

    /**
     * 删除表
     *
     * @param tableName 表名称
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
        } else {
            println(tableName + " not exists.");
        }
        close();
        println("成功删除hbase表......");
    }

    /**
     * 查看已有表
     *
     * @throws IOException
     */
    public static void listTables() {
        init();
        HTableDescriptor hTableDescriptors[] = null;
        try {
            hTableDescriptors = admin.listTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    /**
     * 插入单行
     *
     * @param tableName 表名称
     * @param rowKey    RowKey
     * @param colFamily 列族
     * @param col       列
     * @param value     值
     * @throws IOException
     */
    public static void insert(String tableName, String rowKey, String colFamily, String value) throws IOException {
        init();
//        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("age"), Bytes.toBytes(value));
//        table.put(put);
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
                    logger.error("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(listener);
        params.writeBufferSize(1024);
        final BufferedMutator mutator = connection.getBufferedMutator(params);
        try {
            mutator.mutate(put);
            mutator.flush();
        } finally {
            mutator.close();
            connection.close();
        }

		/*
         * 批量插入 List<Put> putList = new ArrayList<Put>(); puts.add(put); table.put(putList);
		 */
//
//        table.close();
//        close();
    }

    public static void delete(String tableName, String rowKey, String colFamily, String col) throws IOException {
        init();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            println(tableName + " not exists.");
        } else {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (colFamily != null) {
                del.addFamily(Bytes.toBytes(colFamily));
            }
            if (colFamily != null && col != null) {
                del.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
            }
            /*
             * 批量删除 List<Delete> deleteList = new ArrayList<Delete>(); deleteList.add(delete); table.delete(deleteList);
			 */
            table.delete(del);
            table.close();
        }
        close();
    }

    /**
     * 根据RowKey获取数据
     *
     * @param tableName 表名称
     * @param rowKey    RowKey名称
     * @param colFamily 列族名称
     * @param col       列名称
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        if (colFamily != null && col != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }

    /**
     * 根据RowKey获取信息
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey) throws IOException {
        getData(tableName, rowKey, null, null);
    }

    /**
     * 格式化输出
     *
     * @param result
     */
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            println("RowName: " + new String(CellUtil.cloneRow(cell)) + " ");
            println("Timetamp: " + cell.getTimestamp() + " ");
            println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
            println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
            println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
            println("/n");
        }
    }

    /**
     * 打印
     *
     * @param obj 打印对象
     */
    private static void println(Object obj) {
        System.out.println(obj);
    }


    /**
     * 通过scan的rowkey来查询hbase数据
     * @throws IOException
     */
    public static void getScanByRowkey(String  tableName,String startRowkey,String endRowkey) throws IOException {
        init();
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setMaxVersions();
        //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
        scan.setBatch(1000);
        scan.setStartRow(startRowkey.getBytes());
        scan.setStopRow(endRowkey.getBytes());
        //scan.setTimeStamp(NumberUtils.toLong("1370336286283"));
        //scan.setTimeRange(NumberUtils.toLong("1370336286283"), NumberUtils.toLong("1370336337163"));
        //scan.setStartRow(Bytes.toBytes("quanzhou"));
        //scan.setStopRow(Bytes.toBytes("xiamen"));
        //scan.addFamily(Bytes.toBytes("info"));
        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));

        //查询列镞为info，列id值为1的记录
        //方法一(单个查询)
        // Filter filter = new SingleColumnValueFilter(
        //         Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1"));
        // scan.setFilter(filter);
        //方法二(组合查询)
        //FilterList filterList=new FilterList();
        //Filter filter = new SingleColumnValueFilter(
        //    Bytes.toBytes("info"), Bytes.toBytes("id"), CompareOp.EQUAL, Bytes.toBytes("1"));
        //filterList.addFilter(filter);
        //scan.setFilter(filterList);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (KeyValue kv : r.raw()) {
                System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
                        Bytes.toString(kv.getRow()),
                        Bytes.toString(kv.getFamily()),
                        Bytes.toString(kv.getQualifier()),
                        Bytes.toString(kv.getValue()),
                        kv.getTimestamp()));
            }
            System.out.println("\n\t\t==========无限录入==========\n");
        }
        rs.close();
    }

}
