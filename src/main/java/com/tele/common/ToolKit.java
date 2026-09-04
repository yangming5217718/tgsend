package com.tele.common;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;


public class ToolKit {
	
    private static final int ConnectTimeout = 1000 * 10;
    private static final int ReadTimeout = 1000 * 30;
	
	
	static final char HEX_DIGITS[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	// RSA支付公钥、RSA代付公钥、RSA私钥,请登录http://cloudcore.yincheng12.com商户平台>商户管理>获取秘钥，中获取
	
	
	
	static HostnameVerifier hv = new HostnameVerifier() {
		public boolean verify(String urlHostName, SSLSession session) {
			System.out.println("Warning: URL Host: " + urlHostName + " vs. "
					+ session.getPeerHost());
			return true;
		}
	};

	private static void trustAllHttpsCertificates() throws Exception {
		javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[1];
		javax.net.ssl.TrustManager tm = new miTM();
		trustAllCerts[0] = tm;
		javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext
				.getInstance("SSL");
		sc.init(null, trustAllCerts, null);
		javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(sc
				.getSocketFactory());
	}

	static class miTM implements javax.net.ssl.TrustManager,
			javax.net.ssl.X509TrustManager {
		public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			return null;
		}

		public boolean isServerTrusted(
				java.security.cert.X509Certificate[] certs) {
			return true;
		}

		public boolean isClientTrusted(
				java.security.cert.X509Certificate[] certs) {
			return true;
		}

		public void checkServerTrusted(
				java.security.cert.X509Certificate[] certs, String authType)
				throws java.security.cert.CertificateException {
			return;
		}

		public void checkClientTrusted(
				java.security.cert.X509Certificate[] certs, String authType)
				throws java.security.cert.CertificateException {
			return;
		}
	}

	
	
	// RSA非对称密钥算法
	public static final String KEY_ALGORITHM = "RSA";

	public final static String CHARSET = "UTF-8";
	
	public static final String SIGNATURE_ALGORITHM_SHA1WITHRSA = "SHA1WithRSA";//SHA1WithRSA 或者 MD5withRSA

	
	
	/**
	 * 提交请求
	 */
	public static String requestSsl88Https(String url, String params) {
		try {
			
//			Utils.sysOut("请求报文:" + params);
			URL urlObj = new URL(url);
			ToolKit.trustAllHttpsCertificates();
			HttpsURLConnection.setDefaultHostnameVerifier(hv);
			HttpsURLConnection conn = (HttpsURLConnection) urlObj.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setConnectTimeout(ConnectTimeout);
			conn.setReadTimeout(ReadTimeout);
			conn.setRequestProperty("Charset", CHARSET);
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
			conn.setRequestProperty("Content-Length", String.valueOf(params.length()));
			conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36");
			OutputStream outStream = conn.getOutputStream();
			outStream.write(params.toString().getBytes(CHARSET));
			outStream.flush();
			outStream.close();
			return getResponseBodyAsString(conn.getInputStream());
		} catch (Exception e) {
        	//Utils.sysOut(e.getMessage()+"==surl==111==55="+url);
			if(!e.getMessage().contains("Read")) {
				e.printStackTrace();
			}
			return "充值错误:"+e.getMessage()+"==地址："+url;
		}
	}
	
	/**
	 * 提交请求
	 */
	public static String requestSsl88Utf8(String url, String params) {
		try {
			
//			Utils.sysOut("请求报文:" + params);
			URL urlObj = new URL(url);
			ToolKit.trustAllHttpsCertificates();
			HttpsURLConnection.setDefaultHostnameVerifier(hv);
			HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.setUseCaches(false);
			conn.setConnectTimeout(ConnectTimeout);
			conn.setReadTimeout(ReadTimeout);
			conn.setRequestProperty("Charset", CHARSET);
			conn.setRequestProperty("Content-Type", "application/json;charset=utf-8");//不同
			conn.setRequestProperty("Content-Length", String.valueOf(params.length()));
			conn.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");//不同
			OutputStream outStream = conn.getOutputStream();
			outStream.write(params.toString().getBytes(CHARSET));
			outStream.flush();
			outStream.close();
			return getResponseBodyAsString(conn.getInputStream());
		} catch (Exception e) {
        	//Utils.sysOut(e.getMessage()+"==surl==111==66="+url);
			if(!e.getMessage().contains("Read")) {
				e.printStackTrace();
			}
			return "充值错误:"+e.getMessage()+"==地址："+url;
		}
	}
	/**
	 * 获取响应报文
	 */
	private static String getResponseBodyAsString(InputStream in) {
		try {
			BufferedInputStream buf = new BufferedInputStream(in);
			byte[] buffer = new byte[1024];
			StringBuffer data = new StringBuffer();
			int readDataLen;
			while ((readDataLen = buf.read(buffer)) != -1) {
				data.append(new String(buffer, 0, readDataLen, CHARSET));
			}
//			Utils.sysOut("响应报文=" + data);
			return data.toString();
		} catch (Exception e) {
			if(!e.getMessage().contains("Read")) {
				e.printStackTrace();
			}
			return "充值错误:"+e.getMessage();
		}
	}


}