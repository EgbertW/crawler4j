package edu.uci.ics.crawler4j.fetcher;

import java.io.IOException;
import java.net.Socket;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

public class SniSSLConnectionSocketFactory extends SSLConnectionSocketFactory
{
    public static final String ENABLE_SNI = "__enable_sni__";

    /*
     * Implement any constructor you need for your particular application -
     * SSLConnectionSocketFactory has many variants
     */
    public SniSSLConnectionSocketFactory(final SSLContext sslContext, final HostnameVerifier verifier) {
        super(sslContext, verifier);
    }

    @Override
    public Socket createLayeredSocket(
            final Socket socket,
            final String target,
            final int port,
            final HttpContext context) throws IOException {
        Boolean enableSniValue = (Boolean) context.getAttribute(ENABLE_SNI);
        boolean enableSni = enableSniValue == null || enableSniValue;
        return super.createLayeredSocket(socket, enableSni ? target : "", port, context);
    }
}
