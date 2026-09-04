package com.tele.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.util.Arrays;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReverseBalanceCalculator {

    private static final String ADDRESS = "TF6pAKwgbuc3WHb9kPpasbuiizoGDndWAa";
    private static final String USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
    private static final String API_BASE = "https://api.trongrid.io";
    private static final String API_KEY = "6dd02c61-f6c4-4bdd-8ef0-e30c3b690e90";

    private static final HttpClient client = HttpClient.newHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        long timestampT = 1764604800000L; // 北京时间 2025-12-02 00:00:00
        timestampT=timestampT+3600*24*7*1000L;
        BigInteger balanceNow = getCurrentBalance();
        System.out.println("当前余额 raw: " + balanceNow);

        BigInteger inAfterT = getTransfersAfterT(true, timestampT);
        BigInteger outAfterT = getTransfersAfterT(false, timestampT);

        BigInteger balanceAtT = balanceNow.subtract(inAfterT.subtract(outAfterT));

        BigDecimal balanceUSDT = new BigDecimal(balanceAtT)
                .movePointLeft(6); // USDT decimals = 6

        System.out.println("——————————");
        System.out.println("当前余额： " + balanceNow);
        System.out.println("T之后转入： " + inAfterT);
        System.out.println("T之后转出： " + outAfterT);
        System.out.println("T时刻余额(raw)： " + balanceAtT);
        System.out.println("T时刻余额 USDT： " + balanceUSDT.toPlainString());
    }

    private static BigInteger getCurrentBalance() throws Exception {

        // 1. 先把 Base58 地址转成 Hex（41 开头）
        String addressHex = base58ToHex(ADDRESS); // 例如：41A0B1C2...
        if (!addressHex.startsWith("41")) {
            throw new IllegalArgumentException("非法 Tron 地址 Hex：" + addressHex);
        }

        // 2. ABI 参数编码：balanceOf(address) 需要一个 32 字节参数，
        //    Tron 这边一般是去掉前缀 41，剩下 40 位 hex，然后左侧补 0 补足 64 位
        String addrNoPrefix = addressHex.substring(2);           // 去掉 "41"
        BigInteger addrBigInt = new BigInteger(addrNoPrefix, 16);
        String parameter = String.format("%064x", addrBigInt);   // 左补0到64位

        // 3. 组装请求体：
        //    - contract_address / owner_address 使用 Base58（因为 visible = true）
        //    - parameter 使用上面编码好的 64 位 hex
        String body = """
        {
            "contract_address": "%s",
            "function_selector": "balanceOf(address)",
            "parameter": "%s",
            "owner_address": "%s",
            "visible": true
        }
        """.formatted(USDT_CONTRACT, parameter, ADDRESS);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(API_BASE + "/wallet/triggersmartcontract"))
                .header("Content-Type", "application/json")
                .header("TRON-PRO-API-KEY", API_KEY)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        String respBody = res.body();

        // 4. 解析返回
        JsonNode root = mapper.readTree(respBody);

        // 打印一下返回，方便调试
        System.out.println("TriggerSmartContract response:");
        System.out.println(root.toPrettyString());

        // 4.1 先看看 result 是否为 true
        JsonNode resultNode = root.path("result");
        if (!resultNode.isMissingNode()) {
            JsonNode okNode = resultNode.path("result");
            if (okNode.isBoolean() && !okNode.asBoolean()) {
                String msg = root.path("message").asText("");
                throw new RuntimeException("合约调用失败，result=false, message=" + msg);
            }
        }

        // 4.2 再安全地取 constant_result[0]
        JsonNode constantResult = root.path("constant_result");
        if (!constantResult.isArray() || constantResult.size() == 0) {
            throw new RuntimeException("返回中没有 constant_result，完整返回：\n" + root.toPrettyString());
        }

        JsonNode first = constantResult.get(0);
        if (first == null || first.isNull()) {
            throw new RuntimeException("constant_result[0] 为空，完整返回：\n" + root.toPrettyString());
        }

        String hex = first.asText();
        if (hex == null || hex.isEmpty()) {
            throw new RuntimeException("constant_result[0] 字符串为空，完整返回：\n" + root.toPrettyString());
        }

        // 5. 最终余额（raw, 未除以 10^6）
        return new BigInteger(hex, 16);
    }

    private static BigInteger getTransfersAfterT(boolean onlyTo, long minTimestamp) throws Exception {

        BigInteger sum = BigInteger.ZERO;

        String direct = onlyTo ? "only_to=true" : "only_from=true";

        String url = API_BASE + "/v1/accounts/" + ADDRESS + "/transactions/trc20"
                + "?limit=200"
                + "&" + direct
                + "&only_confirmed=true"
                + "&min_timestamp=" + minTimestamp
                + "&contract_address=" + USDT_CONTRACT;

        while (url != null) {

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("TRON-PRO-API-KEY", API_KEY)
                    .GET()
                    .build();

            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

            // 建议加个简单的状态码检查，方便排错
            if (res.statusCode() != 200) {
                System.err.println("HTTP " + res.statusCode() + " body: " + res.body());
                break;
            }

            JsonNode root = mapper.readTree(res.body());

            JsonNode data = root.path("data");
            if (data.isArray()) {
                for (JsonNode tx : data) {
                    String v = tx.path("value").asText("0");
                    if (!"0".equals(v)) {
                        sum = sum.add(new BigInteger(v));
                    }
                }
            }

            JsonNode next = root.path("meta").path("links").path("next");
            if (next.isMissingNode() || next.isNull() || next.asText().isEmpty()) {
                url = null;
            } else {
                // ✅ 直接用返回的完整 URL
                url = next.asText();
            }
        }

        return sum;
    }

    

    /**
     * 将 TRON 地址（Base58Check）解码为 Hex 字符串（不带 0x）
     * 返回内容包含开头的 41 前缀（Tron 地址格式）
     */
    public static String base58ToHex(String base58) {

        byte[] decoded = decodeBase58(base58);

        if (decoded.length != 25) {
            throw new IllegalArgumentException("Base58Check decoded length wrong: " + decoded.length);
        }

        byte[] data = Arrays.copyOfRange(decoded, 0, 21);  // 前 21 字节（含0x41）
        byte[] checksum = Arrays.copyOfRange(decoded, 21, 25);

        // 校验 SHA256(SHA256(data)) 的前 4 字节
        byte[] hash = sha256(sha256(data));
        byte[] calcChecksum = Arrays.copyOfRange(hash, 0, 4);

        if (!Arrays.equals(checksum, calcChecksum)) {
            throw new IllegalArgumentException("Base58Check checksum mismatch");
        }

        return bytesToHex(data);
    }

    // ---------------- Base58 解码 ----------------
    private static final String ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    private static final BigInteger BASE = BigInteger.valueOf(58);
    private static byte[] decodeBase58(String input) {
        BigInteger num = BigInteger.ZERO;

        for (char c : input.toCharArray()) {
            int index = ALPHABET.indexOf(c);
            if (index < 0) {
                throw new IllegalArgumentException("Invalid Base58 character: " + c);
            }
            num = num.multiply(BASE).add(BigInteger.valueOf(index));
        }

        // 转为 byte 数组
        byte[] bytes = num.toByteArray();

        // Base58 前导 '1' → 前导 0x00
        int leadingZeros = 0;
        for (char c : input.toCharArray()) {
            if (c == '1') leadingZeros++;
            else break;
        }

        byte[] result = new byte[leadingZeros + bytes.length];
        System.arraycopy(bytes, 0, result, leadingZeros, bytes.length);

        // 去掉 BigInteger 生成的前导 0
        if (result.length > 1 && result[0] == 0) {
            return Arrays.copyOfRange(result, 1, result.length);
        }

        return result;
    }

    // ---------------- Utils ----------------

    private static byte[] sha256(byte[] input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02X", b));
        return sb.toString();
    }
    
}
