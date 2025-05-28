package cn.vinlee.iceberg;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveCatalog;

public class IcebergHiveCatalog {
  public static void main(String[] args) throws IOException {
    String warehouseLocation = "s3a://hive-iceberg-dev/hive-warehouse";
    String uri = "thrift://172.21.16.12:19083";

    HashMap<String, String> catalogProps = new HashMap<>();
    catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
    catalogProps.put(CatalogProperties.URI, uri);
    try (HiveCatalog hiveCatalog = new HiveCatalog()) {
      hiveCatalog.setConf(getHMSConf());
      hiveCatalog.initialize("hive-iceberg-dev", catalogProps);

      List<Namespace> namespaces = hiveCatalog.listNamespaces();
      for (Namespace namespace : namespaces) {
        System.out.println(namespace);
      }
    }
  }

  public static Configuration getHMSConf() {
    Configuration hmsConf = new Configuration();
    hmsConf.set("fs.s3a.access.key", "minioadmin");
    hmsConf.set("fs.s3a.secret.key", "minioadmin");
    hmsConf.set("fs.s3a.endpoint", "http://10.16.10.6:39000");
    hmsConf.set("fs.s3a.path.style.access", "true");
    hmsConf.set("fs.s3a.connection.ssl.enabled", "false");
    hmsConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    hmsConf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    return hmsConf;
  }
}
