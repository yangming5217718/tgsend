package com.tele.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TronUsdtBalanceAtTime {

    // 要查询的地址
    private static final String ADDRESS = "TF6pAKwgbuc3WHb9kPpasbuiizoGDndWAa";
    // TRC20 USDT 合约地址
    private static final String USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
    // TronGrid API 根地址
    private static final String BASE_URL = "https://api.trongrid.io";
    // TronGrid API Key（需要替换成你自己的）
    private static final String TRON_API_KEY = "6dd02c61-f6c4-4bdd-8ef0-e30c3b690e90";

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 1. 计算“北京时间 2025-12-03 00:00:00”的 Unix 毫秒时间戳
        long targetTimestampMs = getBeijingTimestampMillis(2025, 12, 3, 0, 0, 0);
        System.out.println("Target timestamp (ms): " + targetTimestampMs);

        // 2. 累计这个时间点之前的所有 USDT 转入
        BigInteger totalIn = fetchTotalAmount(ADDRESS, true, targetTimestampMs);
        // 3. 累计这个时间点之前的所有 USDT 转出
        BigInteger totalOut = fetchTotalAmount(ADDRESS, false, targetTimestampMs);

        // 4. 余额 = 入账总额 - 出账总额（单位：最小单位，6 位小数）
        BigInteger balanceRaw = totalIn.subtract(totalOut);

        // USDT 一般是 6 位小数
        int usdtDecimals = 6;
        BigDecimal balance = new BigDecimal(balanceRaw).movePointLeft(usdtDecimals);

        System.out.println("Address: " + ADDRESS);
        System.out.println("Total In  (raw): " + totalIn.toString());
        System.out.println("Total Out (raw): " + totalOut.toString());
        System.out.println("Balance   (raw): " + balanceRaw.toString());
        System.out.println("USDT balance at 2025-12-03 00:00:00 (Beijing): " + balance.toPlainString() + " USDT");
    }

    /**
     * 把北京时间转换为 Unix 毫秒时间戳
     */
    private static long getBeijingTimestampMillis(int year, int month, int day,
                                                  int hour, int minute, int second) {
        LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, minute, second);
        // 北京时间 = Asia/Shanghai
        return ldt.atZone(ZoneId.of("Asia/Shanghai"))
                  .toInstant()
                  .toEpochMilli();
    }

    /**
     * 从 TronGrid 查询在某个时间戳之前（包含）的所有 USDT 交易金额总和。
     *
     * @param address          要查询的地址
     * @param onlyTo           true 表示只看转入 (only_to=true)，false 表示只看转出 (only_from=true)
     * @param maxTimestampMs   截止时间（毫秒）。
     * @return 指定方向的 USDT 金额总和（最小单位，比如 1 USDT = 1_000_000）
     */
    private static BigInteger fetchTotalAmount(String address, boolean onlyTo, long maxTimestampMs) throws IOException, InterruptedException {
        BigInteger sum = BigInteger.ZERO;

        // 初始 URL（limit=200，按时间倒序）
        String directionParam = onlyTo ? "only_to=true" : "only_from=true";
        String url = BASE_URL + "/v1/accounts/" + address + "/transactions/trc20"
                + "?contract_address=" + USDT_CONTRACT
                + "&limit=200"
                + "&max_timestamp=" + maxTimestampMs
                + "&only_confirmed=true"
                + "&" + directionParam;

        while (url != null) {
            System.out.println("Requesting: " + url);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("TRON-PRO-API-KEY", TRON_API_KEY)
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("HTTP error: " + response.statusCode() + " body: " + response.body());
            }

            JsonNode root = objectMapper.readTree(response.body());

            // data 数组里是交易列表
            JsonNode data = root.path("data");
            if (data.isArray()) {
                for (JsonNode tx : data) {
                    // 只处理类型为 Transfer 的记录
                    String type = tx.path("type").asText("");
                    if (!"Transfer".equals(type)) {
                        continue;
                    }

                    // token_info 里检查是不是 USDT 合约
                    JsonNode tokenInfo = tx.path("token_info");
                    String contractAddress = tokenInfo.path("address").asText("");
                    if (!USDT_CONTRACT.equals(contractAddress)) {
                        // 不是 USDT 就跳过
                        continue;
                    }

                    // 读取 value（字符串，最小单位）
                    String valueStr = tx.path("value").asText("0");
                    if (valueStr.isEmpty()) valueStr = "0";

                    BigInteger value = new BigInteger(valueStr);
                    sum = sum.add(value);
                }
            }

            // 处理分页：TronGrid 一般会在 meta.links.next 里给出下一页链接（相对路径）
            JsonNode meta = root.path("meta");
            JsonNode links = meta.path("links");
            JsonNode nextNode = links.path("next");

            if (!nextNode.isMissingNode() && !nextNode.isNull() && !nextNode.asText().isEmpty()) {
                String nextPath = nextNode.asText();
                // 有的 API 返回的是相对路径 "/v1/...."
                if (nextPath.startsWith("http")) {
                    url = nextPath;
                } else {
                    url = BASE_URL + nextPath;
                }
            } else {
                // 没有下一页了
                url = null;
            }
        }

        System.out.println((onlyTo ? "Total IN  " : "Total OUT ") + "sum (raw units) = " + sum);
        return sum;
    }
}
