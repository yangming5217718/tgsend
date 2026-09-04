package com.tele.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class FetchUsdtSpringJackson {

    public static void main(String[] args) throws Exception {
        // 20251202 0点 1764604800000
        // 20251203 0点 1764691200000

        long timestart = 1764604800000L + 3600 * 24 * 6 * 1000L;
        long timeend = timestart + 3600 * 24 * 1000L;

        // 基础 URL（第一页，不带 fingerprint）
        String baseUrl = "https://api.trongrid.io/v1/accounts/TF6pAKwgbuc3WHb9kPpasbuiizoGDndWAa/transactions/trc20"
                + "?limit=200"
                + "&min_timestamp=" + timestart
                + "&max_timestamp=" + timeend
                + "&only_to=true"
                + "&contract_address=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";

        HttpClient client = HttpClient.newHttpClient();
        ObjectMapper mapper = new ObjectMapper();

        long totalRaw = 0;
        int count = 0;

        String url = baseUrl;
        int page = 1;

        while (url != null) {
            System.out.println("请求第 " + page + " 页: " + url);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                System.err.println("请求失败 HTTP 状态码: " + response.statusCode());
                System.err.println("响应内容: " + response.body());
                break;
            }

            JsonNode root = mapper.readTree(response.body());

            // data 数组
            JsonNode dataArray = root.get("data");
            if (dataArray == null || !dataArray.isArray() || dataArray.size() == 0) {
                System.out.println("data 为空，结束翻页。");
                break;
            }

            // 累加本页数据
            for (JsonNode tx : dataArray) {
                JsonNode valueNode = tx.get("value");
                if (valueNode != null && valueNode.isTextual()) {
                    try {
                        long value = Long.parseLong(valueNode.asText());
                        totalRaw += value;
                        count++;
                    } catch (NumberFormatException e) {
                        System.err.println("无法解析 value: " + valueNode.asText());
                    }
                }
            }

            // 翻下一页：优先用 meta.links.next
            JsonNode meta = root.get("meta");
            String nextUrl = null;
            if (meta != null) {
                JsonNode links = meta.get("links");
                if (links != null && links.has("next")) {
                    String tmp = links.get("next").asText();
                    if (tmp != null && !tmp.isEmpty() && !"null".equalsIgnoreCase(tmp)) {
                        nextUrl = tmp;
                    }
                }
            }

            if (nextUrl == null) {
                System.out.println("没有下一页链接，结束翻页。");
                break;
            } else {
                url = nextUrl;
                page++;
            }
        }

        double totalUsdt = totalRaw / 1_000_000.0;

        System.out.println("—— 最终统计结果（已包含所有翻页记录） ——");
        System.out.println("交易笔数: " + count);
        System.out.println("Raw 总金额: " + totalRaw);
        System.out.printf("USDT 总金额: %.6f\n", totalUsdt);
    }
}
