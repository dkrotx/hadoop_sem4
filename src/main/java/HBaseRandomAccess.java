/*
 * 1. setup your hbase client properly (make hbase shell work from console)
 *    setup ZooKeeper addr in hbase-site.xml
 * 2. Launch via:
 *    $ CLASSPATH="`hbase classpath`:out/artifacts/HBaseExamples/HBaseExamples.jar" java HBaseRandomAccess scan 2>/dev/null
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class HBaseRandomAccess {
    private String table_name = "YOURNAME_table";

    void DemoPut() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(table_name));
        Put put = new Put(Bytes.toBytes("microsoft.com"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("owner"), Bytes.toBytes("Bill Gates"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("last_updated"), Bytes.toBytes(1478039132000L));
        table.put(put);


        table.close();
        connection.close();
    }

    void DemoGet() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(table_name));
        Get get = new Get(Bytes.toBytes("microsoft.com"));
        Result res = table.get(get);
        for(Cell cell: res.listCells()) {
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.printf("Qualifier: %s : Value: %s\n", qualifier, value);
        }

        /*CellScanner scanner = res.cellScanner();
        while (scanner.advance()) {
            Cell cell = scanner.current();
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.printf("++ Qualifier: %s : Value: %s\n", qualifier, value);
        }*/

        table.close();
        connection.close();
    }

    void PrintCell(Cell cell) {
        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
        String value = Bytes.toString(CellUtil.cloneValue(cell));
        System.out.printf("\t%s=%s\n", qualifier, value);
    }

    void PrintResult(Result res) throws UnsupportedEncodingException {
        System.out.printf("------------- ROW: %s\n", new String(res.getRow(), "UTF8"));
        for (Cell cell: res.listCells())
            PrintCell(cell);
    }

    void DemoScan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf("webpages"));
        Scan scan = new Scan().addColumn(Bytes.toBytes("htmls"), Bytes.toBytes("text"));
        ResultScanner scanner = table.getScanner(scan);

        for (Result res: scanner)
            PrintResult(res);

        table.close();
        connection.close();
    }

    public static void main(String[] args) throws IOException {
        HBaseRandomAccess demo = new HBaseRandomAccess();

        if (args.length != 1)
            usage();

        if (args[0].equals("put"))
            demo.DemoPut();
        else if (args[0].equals("get"))
            demo.DemoGet();
        else if (args[0].equals("scan"))
            demo.DemoScan();
        else
            usage();
    }

    static void usage() {
        System.err.println("wrong usage!");
        System.exit(64);
    }
}
