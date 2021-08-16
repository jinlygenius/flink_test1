package apitest.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;

public class OneToManyFlatmapFunction extends RichFlatMapFunction<String, JSONObject> {
    @Override
    public void flatMap(String message, Collector<JSONObject> collector) throws Exception {
        System.out.println("message:"+message);
        JSONObject result = new JSONObject();
        try {
            JSONObject jsonObject = JSON.parseObject(message);
            result.put("backend_app_id", jsonObject.getString("backend_app_id"));
            result.put("version", jsonObject.getString("version"));
            result.put("front_version", jsonObject.getString("front_version"));
            result.put("hidden_user_id", jsonObject.getString("hidden_user_id"));
            result.put("share_param", jsonObject.getString("share_param"));
            result.put("source", jsonObject.getString("source"));
            result.put("user", jsonObject.getString("user"));
            result.put("user_id", jsonObject.getString("user_id"));
            result.put("wechat_app_id", jsonObject.getString("wechat_app_id"));

            JSONArray events = jsonObject.getJSONArray("logs");
            for (int i = 0; i < events.size(); i++) {
                JSONObject event = events.getJSONObject(i);
                Long timestamp = event.getLong("timestamp") * 1000;
                String createdTime = TimeUtil.timestamp2String(timestamp);
                String dt = TimeUtil.timestamp2String(timestamp, "yyyyMMdd");
                System.out.println("timestamp:"+timestamp);
                System.out.println("createdTime:"+createdTime);
                System.out.println("dt:"+dt);

                result.put("action", event.getString("action"));
                result.put("content", event.getString("content"));
                result.put("timestamp", timestamp);
                result.put("detail", event.getString("new_content"));
                result.put("created_time", createdTime);
                result.put("dt", dt);

                collector.collect(result);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
