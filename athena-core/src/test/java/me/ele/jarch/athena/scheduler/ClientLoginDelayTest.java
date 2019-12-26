package me.ele.jarch.athena.scheduler;

import me.ele.jarch.athena.allinone.AllInOneMonitor;
import me.ele.jarch.athena.allinone.DBConfType;
import me.ele.jarch.athena.allinone.DBConnectionInfo;
import me.ele.jarch.athena.constant.Constants;
import me.ele.jarch.athena.exception.QuitException;
import me.ele.jarch.athena.server.async.AsyncClient;
import me.ele.jarch.athena.sql.ResultSet;
import org.apache.commons.io.Charsets;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClientLoginDelayTest {

    private static int loginCntPerDb = 10;
    private static volatile AtomicLong timeout = new AtomicLong(5000);
    private static String path = "dal_connection.cfg";
    private final AtomicInteger authOKCnt = new AtomicInteger(0);
    private static volatile long timeoutVal = 5100;
    private static volatile int duration = -1;

    private static volatile AtomicLong loginTimeoutCnt = new AtomicLong(0);
    private static volatile AtomicLong loginOKCnt = new AtomicLong(0);
    private static volatile AtomicLong loginFailedCnt = new AtomicLong(0);
    private static volatile AtomicLong totalTaskCnt = new AtomicLong(0);
    private static volatile boolean isTerminated = false;

    private ThreadPoolExecutor executor =
        new ThreadPoolExecutor(20, 500, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000),
            new ThreadPoolExecutor.CallerRunsPolicy());
    private static ScheduledThreadPoolExecutor sche =
        new ScheduledThreadPoolExecutor(50, new ThreadPoolExecutor.CallerRunsPolicy());
    private static final CountDownLatch DUMMY = new CountDownLatch(0);
    private volatile CountDownLatch loginTimeoutLatch = DUMMY;
    private volatile CountDownLatch authOKLatch = DUMMY;

    private static boolean isTimeout = true;
    private static boolean isLoadTest = false;


    public static void main(String[] args) throws IOException {
        parseArgs(args);

        Map<DBConfType, List<DBConnectionInfo>> dbInfoMaps = parseDBInfosFromFile(path);

        long startTime = System.nanoTime();
        List<DBConnectionInfo> dbInfos =
            dbInfoMaps.values().stream().flatMap(List::stream).collect(Collectors.toList());
        if (isLoadTest) {
            runLoadTest(dbInfos);
        } else {
            runFunctionTest(dbInfos);
        }

        System.out.println("total time used: " + (System.nanoTime() - startTime) / 1000000000.0);
        System.out.println(String.format(
            "total tasks count: %d, succeed login counts: %d, failed login counts: %d, mock timeout login counts: %d",
            totalTaskCnt.get(), loginOKCnt.get(), loginFailedCnt.get(), loginTimeoutCnt.get()));

        System.exit(0);

    }

    private static void runFunctionTest(List<DBConnectionInfo> dbInfos) {
        long individualTime = System.nanoTime();
        totalTaskCnt.getAndAdd(loginCntPerDb * dbInfos.size());

        ClientLoginDelayTest clientLoginDelayTest = new ClientLoginDelayTest();
        clientLoginDelayTest.buildNonTimeoutTasks(dbInfos);
        if (isTimeout) {
            clientLoginDelayTest.buildTimeoutTasks(dbInfos);
        }
        System.out.println(" test time used for one round: "
            + (System.nanoTime() - individualTime) / 1000000000.0);

        clientLoginDelayTest.executor.shutdown();
    }

    private static void runLoadTest(List<DBConnectionInfo> dbInfos) {
        long individualTime = System.nanoTime();
        totalTaskCnt.getAndAdd(loginCntPerDb * dbInfos.size());
        if (!isOnetimeTest()) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.schedule(() -> {
                terminateLoadTest();
                System.out.println("terminate load test");
            }, duration, TimeUnit.SECONDS);
        }
        while (!isTerminated) {
            ClientLoginDelayTest clientLoginDelayTest = new ClientLoginDelayTest();
            if (isTimeout) {
                clientLoginDelayTest.buildTimeoutTasks(dbInfos);
            } else {
                clientLoginDelayTest.buildNonTimeoutTasks(dbInfos);
            }
            System.out.println(" test time used for one round in load Test: "
                + (System.nanoTime() - individualTime) / 1000000000.0);
            System.out.println(String.format(
                "after this round, the total tasks count: %d, succeed login counts: %d, failed login counts: %d, mock timeout login counts: %d",
                totalTaskCnt.get(), loginOKCnt.get(), loginFailedCnt.get(), loginTimeoutCnt.get()));

            clientLoginDelayTest.executor.shutdown();
            if (isOnetimeTest()) {
                isTerminated = (true);
            }
        }

    }

    private static void terminateLoadTest() {
        isTerminated = true;
    }

    private static boolean isOnetimeTest() {
        return duration < 0;
    }

    private static void parseArgs(String[] args) {
        if (args.length > 0) {
            timeoutVal = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            loginCntPerDb = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            path = args[2];
        }
        if (args.length > 3) {
            isTimeout = Boolean.parseBoolean(args[3]);
        }
        if (args.length > 4) {
            isLoadTest = Boolean.parseBoolean(args[4]);
        }
        if (args.length > 5) {
            duration = Integer.parseInt(args[5]);
        }

    }

    private static Map<DBConfType, List<DBConnectionInfo>> parseDBInfosFromFile(String path)
        throws IOException {
        Map<DBConfType, List<DBConnectionInfo>> result = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path), Charsets.UTF_8)) {
            reader.lines().forEach(line -> {
                line = line.trim();
                DBConfType confType = DBConfType.getType(line);
                if (confType == DBConfType.NONE) {
                    return;
                }
                DBConnectionInfo info = DBConnectionInfo.parseDBConnectionString(line);
                if (info == null) {
                    return;
                }
                String decodedPd = AllInOneMonitor.getInstance().dec(info.getPword());
                if (decodedPd == null) {
                    decodedPd = Constants.DAL_CONFIG_ERR_PWD;
                }
                info.setPword(decodedPd);
                List<DBConnectionInfo> dbInfos =
                    result.computeIfAbsent(confType, (key) -> new ArrayList<>());
                dbInfos.add(info);
            });
        }
        return result;
    }

    private static long getTasksAwaitTime(int taskCnt) {
        return (timeoutVal * taskCnt / 500 + 100000);
    }

    private void buildTimeoutTasks(List<DBConnectionInfo> dbInfos) {
        timeout = new AtomicLong(timeoutVal);
        int taskCnt = loginCntPerDb * dbInfos.size();
        this.loginTimeoutLatch = new CountDownLatch(taskCnt);
        this.buildTestTasks(dbInfos, loginCntPerDb);

        try {
            boolean isDone =
                this.loginTimeoutLatch.await(getTasksAwaitTime(taskCnt), TimeUnit.MILLISECONDS);
            if (!isDone) {
                System.out.println(String.format(
                    "timeout waiting all timeout tasks to be done, left task count: %d in %d time",
                    this.loginTimeoutLatch.getCount(), getTasksAwaitTime(taskCnt)));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(String
            .format("test timeout cases done! service authOKCnt count: %d, total test count is: %d",
                this.authOKCnt.get(), loginCntPerDb * dbInfos.size()));
    }

    private void buildNonTimeoutTasks(List<DBConnectionInfo> dbInfos) {
        int taskCnt = loginCntPerDb * dbInfos.size();
        timeout = new AtomicLong(0);
        this.authOKLatch = new CountDownLatch(taskCnt);
        this.authOKCnt.set(0);//reset count for second round
        this.buildTestTasks(dbInfos, loginCntPerDb);

        try {
            boolean isDone = this.authOKLatch.await(30, TimeUnit.SECONDS);
            if (!isDone) {
                System.out.println(String.format(
                    "timeout waiting all non-timeout tasks to be done, left task count: %d ",
                    this.authOKLatch.getCount()));
            }
            System.out.println(String.format(
                "test normal cases done! service authOKCnt count: %d, total test count is: %d",
                this.authOKCnt.get(), taskCnt));
        } catch (InterruptedException e) {
            System.out.println(
                "timeout waiting all tasks to be done, left task count:" + this.authOKCnt
                    + " authOKLatch left: " + this.authOKLatch.getCount());
            e.printStackTrace();
        }
    }

    private void buildTestTasks(List<DBConnectionInfo> dbInfos, int taskCnt) {
        IntStream.range(0, taskCnt)
            .forEach(i -> dbInfos.forEach(dbInfo -> this.executor.execute(() -> {
                ClientDelayedLogin clientDelayedLogin = new ClientDelayedLogin(dbInfo);
                clientDelayedLogin.setClientLoginDelayTest(this);
                clientDelayedLogin.doAsyncExecute();
            })));
    }

    /**
     * Mock mysql login
     */
    static class ClientDelayedLogin extends AsyncClient {

        private volatile boolean isAuthorized = false;

        private void setClientLoginDelayTest(ClientLoginDelayTest clientLoginDelayTest) {
            this.clientLoginDelayTest = clientLoginDelayTest;
        }

        ClientLoginDelayTest clientLoginDelayTest;

        private ClientDelayedLogin(DBConnectionInfo dbConnInfo) {
            super(dbConnInfo);
        }

        @Override protected String getQuery() {
            return "SELECT @@global.read_only";
        }

        @Override protected void handleResultSet(ResultSet resultSet) {
        }

        @Override protected void handleTimeout() {
            doQuit("timeout, close the channel! " + this.getDbConnInfo().getQualifiedDbId());
        }

        @Override protected synchronized void handleChannelInactive() {
            if (isTimeout && !isAuthorized) {
                isAuthorized = true;
                loginTimeoutCnt.getAndIncrement();
                clientLoginDelayTest.loginTimeoutLatch.countDown();
                System.out.println("loginTimeoutLatch latch countdown!, counts left: "
                    + clientLoginDelayTest.loginTimeoutLatch.getCount());
            }
            if (!isAuthorized && clientLoginDelayTest.authOKLatch.getCount() > 0) {
                isAuthorized = true;
                clientLoginDelayTest.authOKLatch.countDown();
                loginFailedCnt.getAndIncrement();
                System.out.println(
                    "authenok latch countdown in handle channelInactive!, counts left: "
                        + clientLoginDelayTest.authOKLatch.getCount());
                System.out.println("Channel ID is: " + serverChannel.localAddress());
            }

        }

        @Override public synchronized void doQuit(String reason) {
            //countdown for bootstrap client connection failure
            if (!isAuthorized) {
                if (clientLoginDelayTest.authOKLatch.getCount() > 0) {
                    isAuthorized = true;
                    loginFailedCnt.getAndIncrement();
                    clientLoginDelayTest.authOKLatch.countDown();
                    System.out.println("Disconnecting unauthorized session, authOKLatch left:  "
                        + clientLoginDelayTest.authOKLatch.getCount());
                }
                if (clientLoginDelayTest.loginTimeoutLatch.getCount() > 0) {
                    isAuthorized = true;
                    loginFailedCnt.getAndIncrement();
                    clientLoginDelayTest.loginTimeoutLatch.countDown();
                    System.out.println(
                        "Disconnecting unauthorized session, loginTimeoutLatch left:  "
                            + clientLoginDelayTest.loginTimeoutLatch.getCount());
                }
            }
            super.doQuit(reason);
        }

        @Override protected void doHandshakeAndAuth() throws QuitException {
            if (isTimeout && timeout.get() > 0) {
                sche.schedule(() -> {
                    try {
                        super.doHandshakeAndAuth();
                    } catch (QuitException e) {
                        e.printStackTrace();
                    }
                }, timeout.get(), TimeUnit.MILLISECONDS);
            } else {
                super.doHandshakeAndAuth();
            }
        }

        @Override protected void doAuthResult() throws QuitException {
            clientLoginDelayTest.authOKCnt.getAndIncrement();
            isAuthorized = true;
            loginOKCnt.getAndIncrement();
            if (clientLoginDelayTest.authOKLatch.getCount() > 0) {
                clientLoginDelayTest.authOKLatch.countDown();
                System.out.println(
                    "authenok latch countdown!, counts left: " + clientLoginDelayTest.authOKLatch
                        .getCount());
            }
            super.doAuthResult();
        }


    }
}
