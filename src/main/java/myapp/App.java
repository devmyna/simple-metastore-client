package myapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import javax.security.sasl.Sasl;
import java.util.HashMap;
import java.util.Map;

public class App {
    private static final String host = "";
    private static final int port = 9083;

    public static void main(String... args) {

        // System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");

        TTransport transport = new TSocket(host, port);
        TSaslClientTransport saslTransport = null;
        try {
            // nomal
            ThriftHiveMetastore.Client nclient = new ThriftHiveMetastore.Client(
                    new TBinaryProtocol(transport));

            // kerberos
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "kerberos");
            conf.set("hadoop.security.authorization", "true");
            conf.set("dfs.client.use.datanode.hostname", "true");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("user", "user.keytab");
            UserGroupInformation currentUser = UserGroupInformation.getLoginUser();
            System.out.println("user : " + currentUser);

            Map<String, String> saslProperties = new HashMap<String, String>();
            saslProperties.put(Sasl.QOP, "auth-conf,auth");
            saslProperties.put(Sasl.SERVER_AUTH, "true");

            saslTransport = new TSaslClientTransport(
                    "GSSAPI",
                    null,
                    "hive",
                    host,
                    saslProperties,
                    null,
                    transport);

            TUGIAssumingTransport ugiTransport = new TUGIAssumingTransport(saslTransport, currentUser);
            ThriftHiveMetastore.Client client = new ThriftHiveMetastore.Client(
                    new TBinaryProtocol(ugiTransport));

            try{
                ugiTransport.open();
            } catch (Exception e1){
                e1.printStackTrace();
            }

            System.out.println("saslTransport.isOpen(): " + saslTransport.isOpen());

            System.out.println(client.get_all_databases());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            saslTransport.close();
        }
    }
}
