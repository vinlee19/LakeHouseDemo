package cn.vinlee.iceberg;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IcebergHadoopMinioDemo {
    public static final Set<String> SYSTEM_TABLES = Sets.newHashSet("files", "history", "metadata_log_entries",
            "snapshots", "entries", "manifests", "partitions");

    public static void main(String[] args) throws IOException {


        String warehousePath = "s3a://ct-hadoop-iceberg-test/warehouse/";

        try (HadoopCatalog hadoopCatalog = createHadoopCatalog(getHadoopConf(), warehousePath)) {
            // 1. list all namespaces
            List<Namespace> namespaces = listNamespaces(hadoopCatalog);
            //
            for (Namespace namespace : namespaces) {
                System.out.println(namespace);
                Map<String, String> stringStringMap = hadoopCatalog.loadNamespaceMetadata(namespace);
                stringStringMap.forEach((k, v) -> System.out.println(k + "=" + v));
                List<TableIdentifier> icebergTables = listTables(hadoopCatalog, namespace);
                for (TableIdentifier icebergTable : icebergTables) {
                    Table table = getTable(hadoopCatalog, icebergTable);
                    Snapshot snapshot = table.currentSnapshot();
                    // snapshot summary
                    Map<String, String> summary = snapshot.summary();
                    System.out.println("Table: " + table);
                    summary.forEach((k, v) -> System.out.println(k + "=" + v));
                    String manifestListLocation = snapshot.manifestListLocation();
                    System.out.println("Manifest location: " + manifestListLocation);
                    // table history
                    List<HistoryEntry> history = table.history();
                    for (HistoryEntry historyEntry : history) {
                        long snapshotId = historyEntry.snapshotId();
                        long timestampMillis = historyEntry.timestampMillis();
                    }
                    // schema
                    Map<Integer, Schema> schemas = table.schemas();
                    // schema
                    System.out.println("schemas");
                    schemas.forEach((k, v) -> System.out.println(k + "=" + v));
                    //spec
                    System.out.println("spec:");
                    table.specs().forEach((k, v) -> System.out.println(k + "=" + v));
                    //properties
                    System.out.println("properties");
                    table.properties().forEach((k, v) -> System.out.println(k + "=" + v));
                    //table location
                    String location = table.location();
                    System.out.println("Location: " + location);
                }
                System.out.println("===========namespace===========");
            }
        }
    }

    public static HadoopCatalog createHadoopCatalog(Configuration hadoopConf, String warehousePath) {
        return new HadoopCatalog(hadoopConf, warehousePath);
    }


    public static List<Namespace> listNamespaces(HadoopCatalog catalog) {
        return catalog.listNamespaces();
    }

    public static List<TableIdentifier> listTables(HadoopCatalog catalog, Namespace namespace) {
        return catalog.listTables(namespace);
    }

    public static Table getTable(HadoopCatalog catalog, TableIdentifier tableName) {
        return catalog.loadTable(tableName);
    }

    public static Map<Integer, Schema> getSchemas(Table icebergTable) {
        return icebergTable.schemas();
    }

    public static Iterable<Snapshot> getSnapshots(Table icebergTable) {
        return icebergTable.snapshots();
    }

    public static Configuration getHadoopConf() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.s3a.access.key", "minioadmin");
        hadoopConf.set("fs.s3a.secret.key", "minioadmin");
        hadoopConf.set("fs.s3a.endpoint", "http://10.16.10.6:39000");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return hadoopConf;
    }

}
