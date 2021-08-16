package apitest.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.common.protocol.types.Field;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class MySinkFunction extends RichSinkFunction<JSONObject> implements Serializable {
    Statement stmt = null;
    Connection conn = null;
    PreparedStatement ps = null;
    String sql = null;

    String access_id = null;
    String access_key = null;
    String project_name = null;
    String endpoint = null;

    public MySinkFunction(String access_id,String access_key,String project_name,String endpoint) {
        this.access_id=access_id;
        this.access_key=access_key;
        this.project_name=project_name;
        this.endpoint=endpoint;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Properties config = new Properties();
        config.put("access_id", "LTAI5tD7qJtAspxFwt2sQL24");
        config.put("access_key", "dqVwLyjyRPzblPEqjRzF8gAk90xXVU");
        config.put("project_name", "sparrow_pro_dev");
        // config.put("charset", "...");
        conn = DriverManager.getConnection(endpoint, config);
        stmt = conn.createStatement();
        sql = "insert into sparrow_log_frontlog PARTITION (dt=?) " +
                "(backend_app_id, version, front_version, hidden_user_id, share_param, source, user, user_id, wechat_app_id, action, content, time_stamp, detail, created_time) " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void invoke(JSONObject messageJSONObject, Context context) throws Exception {
        /**
        {
            "backend_app_id": "app_1521010788",
            "hidden_user_id": "1568299839089236",
            "logs": "[{\"timestamp\":\"1628672162\",\"content\":\"802\",\"action\":\"go_to_cuthand\"}]",
            "share_param": "",
            "source": "wechat_app",
            "timestamp": "1628672164",
            "user": "13811182290",
            "user_id": "1feb5694fca24d3885b51b8b3364a1e5",
            "wechat_app_id": ""
        }
        */



        if (messageJSONObject!=null && !messageJSONObject.isEmpty()) {
            String backend_app_id=messageJSONObject.getString("backend_app_id");
            String version=messageJSONObject.getString("version");
            String front_version=messageJSONObject.getString("front_version");
            String hidden_user_id=messageJSONObject.getString("hidden_user_id");
            String share_param=messageJSONObject.getString("share_param");
            String source=messageJSONObject.getString("source");
            String user=messageJSONObject.getString("user");
            String user_id=messageJSONObject.getString("user_id");
            String wechat_app_id=messageJSONObject.getString("wechat_app_id");

            String action = messageJSONObject.getString("action");
            String content = messageJSONObject.getString("content");
            Long timestamp = messageJSONObject.getLong("timestamp");
            String detail = messageJSONObject.getString("detail");
            String createdTime = messageJSONObject.getString("created_time");
            // dt = sf.format(date);
            String  dt = messageJSONObject.getString("dt");

            // 注意，ps的每个字段都要set成新的值！
            ps.setString(1, dt);
            ps.setString(2, backend_app_id);
            ps.setString(3, version);
            ps.setString(4, front_version);
            ps.setString(5, hidden_user_id);
            ps.setString(6, share_param);
            ps.setString(7, source);
            ps.setString(8, user);
            ps.setString(9, user_id);
            ps.setString(10, wechat_app_id);
            ps.setString(11, action);
            ps.setString(12, content);
            ps.setLong(13, timestamp);
            ps.setString(14, detail);
            ps.setString(15, createdTime);

            ps.execute();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        conn.close();
        stmt.close();
        ps.close();
    }
}
