package me.ele.jarch.athena.netty.debug;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import me.ele.jarch.athena.netty.AthenaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Route to proper dynamic portal.
 * <p>
 * 1. return OK if url = "/ping" for the heartbeat request
 * <p>
 * 2. return a json containing a list of portals of the dynamic debug portals
 * <p>
 * 3. return a json if url = "/tailError/"+appid for the request getting the latest 30 error log
 *
 * @author donxuu
 * @since 8/19/15.
 */
public class FixedHttpServerInboundHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger =
        LoggerFactory.getLogger(FixedHttpServerInboundHandler.class);

    private String[] getFiles() throws IOException {
        File tmpDir = new File("/tmp");
        File[] files = tmpDir.listFiles();
        if (files == null) {
            throw new IOException("no file in /tmp");
        }

        Map<String, File> fileMap = new HashMap<>();
        for (File file : files) {
            String fileName = file.getName();
            if (!fileName.startsWith("dal_debug_server") || !fileName.contains("-")) {
                continue;
            }
            String debugFilePrefix = fileName.split("-")[0];
            File fileInMap = fileMap.getOrDefault(debugFilePrefix, null);
            if (fileInMap != null) {
                long fileInMapTime = AthenaUtils.getFileCreationTime(fileInMap.toPath());
                long thisFileTime = AthenaUtils.getFileCreationTime(file.toPath());
                if (thisFileTime > fileInMapTime) {
                    fileMap.put(debugFilePrefix, file);
                }
            } else {
                fileMap.put(debugFilePrefix, file);
            }
        }

        List<String> fileNames = new ArrayList<>(fileMap.size());
        for (Map.Entry<String, File> entry : fileMap.entrySet()) {
            fileNames.add(entry.getValue().getName());
        }

        return fileNames.toArray(new String[fileNames.size()]);
    }

    private FullHttpResponse buildResponse() throws IOException {
        String[] files = getFiles();
        StringBuilder builder = new StringBuilder("[\r\n");
        Arrays.stream(files).flatMap(file -> {
            try (BufferedReader br = new BufferedReader(
                new FileReader(Paths.get("/tmp", file).toFile()))) {
                // Here we must convert the intermediate methods, br.lines() to a terminal operations
                // since the br will be closed out of this "try" block immediately.
                return br.lines().limit(10).collect(Collectors.toList()).stream();
            } catch (Exception e) {
                logger.error(Objects.toString(e));
            }
            return Stream.empty();
        }).forEach(line -> {
            if (!line.matches("^<a.*>.*</a><p></p>,$"))
                builder.append(line).append(",\r\n");
        });
        builder.append("]");
        String context = builder.deleteCharAt(builder.lastIndexOf(",")).toString();

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
            Unpooled.wrappedBuffer(context.getBytes("UTF-8")));

        response.headers().set(CONTENT_TYPE, "application/json");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        return response;
    }

    /**
     * get the lasted 30 error log
     *
     * @param appid
     * @return
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    private FullHttpResponse buildResponseForErrorLog(String appid)
        throws UnsupportedEncodingException, IOException {
        String errPath = "/data/log/" + appid + "/error.log";
        File file = new File(errPath);

        StringBuilder builder = new StringBuilder();
        builder.append("[");
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            long length = randomAccessFile.length();
            int count = 0;
            if (length != 0) {
                long pos = length - 1;
                while (pos > 0) {
                    pos--;
                    randomAccessFile.seek(pos);
                    if (randomAccessFile.readByte() == '\n') {
                        String line = randomAccessFile.readLine();
                        builder.append(" \" " + line + " \", ");
                        count++;
                        if (count == 30) // just read the latest 30 lines
                            break;
                    }
                }
                if (pos == 0) {
                    randomAccessFile.seek(0);
                    builder.append(" \" " + randomAccessFile.readLine() + " \", ");
                }
            }
        }

        if (builder.lastIndexOf(",") == -1) {
            builder.append("]");
        } else {
            builder.deleteCharAt(builder.lastIndexOf(",")).append("]");
        }

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
            Unpooled.wrappedBuffer(builder.toString().getBytes("UTF-8")));

        response.headers().set(CONTENT_TYPE, "application/json");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        return response;
    }

    /**
     * build a response whose content is the parameter msg
     *
     * @param msg
     * @return
     * @throws UnsupportedEncodingException
     */
    private FullHttpResponse buildResponseWithMsg(String msg) throws UnsupportedEncodingException {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK,
            Unpooled.wrappedBuffer(msg.getBytes("UTF-8")));

        response.headers().set(CONTENT_TYPE, "application/json");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        return response;
    }

    /**
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            if ("/ping".equalsIgnoreCase(request.getUri())) {
                HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
                response.headers().set(CONNECTION, "Keep-alive");
                ctx.writeAndFlush(response);
                return;
            }

            String uri = request.getUri();
            if (uri != null && uri.startsWith("/tailError/")) {
                String error = null;
                try {
                    String appid = uri.substring("/tailError/".length());
                    if (appid != null && !"".equals(appid)) {
                        ctx.writeAndFlush(buildResponseForErrorLog(appid));
                    } else {
                        ctx.writeAndFlush(buildResponseWithMsg(
                            "^^^!! Error!You didn't have the right parameter:appid!"));
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    error = e.toString();
                    ctx.writeAndFlush(buildResponseWithMsg("^^^!! JAVA Exception: " + error));
                }
                return;
            }

            ctx.writeAndFlush(buildResponse());
        }
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
        logger.error(cause.getMessage(), cause);
        ctx.close();
    }
}
