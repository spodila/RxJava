package rx.schedulers;

import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.Subscriptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ReroutingEventLoopScheduler extends Scheduler {
    @Override
    public Worker createWorker() {
        return WorkerQueues.INSTANCE.createWorkerQueue();
    }

    // ToDo REMOVE THIS
    public void printStats() {
        for(CoreThreadWorker worker: CoreThreadWorkers.INSTANCE.CORE_THREAD_WORKERS) {
            System.out.printf("%d: avgLatency=%10.2f  count=%d\n", worker.myIndex, worker.latencyCounts.getAverage(), worker.getCount());
        }
        System.out.println("#Reroutes=" + CoreThreadWorkers.INSTANCE.numReroutes);
    }

    protected static class CoreThreadWorkers {
        final AtomicLong counter = new AtomicLong(0L);
        private final AtomicLong numReroutes = new AtomicLong(0L);
        protected final CoreThreadWorker[] CORE_THREAD_WORKERS;
        protected final static int NUM_CORES = Runtime.getRuntime().availableProcessors();
        protected static final CoreThreadWorkers INSTANCE = new CoreThreadWorkers();
        private int n=1;
        private double[] latencies = new double[NUM_CORES];
        private AtomicBoolean canReroute = new AtomicBoolean(true);
        private int rerouter=-1;
        private long lastReroutingAt=0;
        private final long ReroutingDelayIntervalMillis = 20;

        private CoreThreadWorkers() {
            CORE_THREAD_WORKERS = new CoreThreadWorker[NUM_CORES];
            for(int i=0; i<NUM_CORES; i++) {
                CORE_THREAD_WORKERS[i] = new CoreThreadWorker(i);
            }
        }

        private CoreThreadWorker getNextWorker() {
            return getNextWorkerToRouteTo();
        }

        protected CoreThreadWorker getNextWorkerToRouteTo() {
            return CORE_THREAD_WORKERS[(int)(counter.getAndIncrement() % NUM_CORES)];
        }

        protected int getRerouteDestination(int from, double[] latencyValues) {
            int dest = -1;
            int min=-1;
            double minVal=0.0;
            for(int i=0; i<latencyValues.length; i++) {
                if(i==from)
                    continue;
                if(latencyValues[i] == 0)
                    return i;
                if(minVal>latencyValues[i]) {
                    minVal = latencyValues[i];
                    min = i;
                }
            }
            return min;
        }

        int shouldReroute(int theWorker) {
            if(!canReroute.compareAndSet(true, false))
                return -1;
            if(rerouter >= 0)
                return -1;
            if(lastReroutingAt > (System.currentTimeMillis()-ReroutingDelayIntervalMillis))
                return -1;
            try {
                for(int i=0; i<NUM_CORES; i++) {
                    latencies[i] = CORE_THREAD_WORKERS[i].latencyCounts.getAverage();
                }
                int dest = getRerouteDestination(theWorker, latencies);
                if(dest>=0)
                    rerouter = theWorker;
                return dest;
            }
            finally {
                canReroute.set(true);
            }
        }

        void endReroutring(int theWorker) {
            if(rerouter == theWorker) {
                lastReroutingAt = System.currentTimeMillis();
                rerouter = -1;
                numReroutes.incrementAndGet();
            }
        }

    }

    protected static class WorkerQueueToReRoute {
        long qIndex;

    }

    private static class AverageOfN {
        private final int MAX_LATENCY_COUNTS=10;
        private List<Long> list = new ArrayList<Long>(MAX_LATENCY_COUNTS+1);
        private double sum=0;
        void add(long val) {
            list.add(val);
            if(list.size()>=MAX_LATENCY_COUNTS) {
                long v = list.remove(0);
                sum -= v;
            }
            sum += val;
        }
        double getAverage() {
            if(list.isEmpty())
                return 0.0;
            return sum/list.size();
        }
    }

    protected static class CoreThreadWorker {
        private final int myIndex;
        private final ScheduledExecutorService executor;
        private volatile int counter = 0;
        private AverageOfN latencyCounts = new AverageOfN();
        //private List<Long> latencyCounts = new ArrayList<Long>(MAX_LATENCY_COUNTS+1);
        private Map<Long, AverageOfN> queueCountsHistory = new HashMap<Long, AverageOfN>();
        private Map<Long, Integer> queueCounts = new HashMap<Long, Integer>();
        private long latencyEvalInterval=40; // ToDo what's a good number?
        private static final double EPS=0.02;

        CoreThreadWorker(final int index) {
            this.myIndex = index;
            executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "RxScheduledExecutorPool-" + index);
                    t.setDaemon(true);
                    return t;
                }
            });
            latencyEvalWrapper();
        }

        private void latencyEvalWrapper() {
            executor.schedule(new Runnable() {
                @Override
                public void run() {
                    scheduleLatencyEval(System.currentTimeMillis());
                }
            }, latencyEvalInterval, TimeUnit.MILLISECONDS);
        }

        private void endRerouting() {
            CoreThreadWorkers.INSTANCE.endReroutring(myIndex);
        }

        private void scheduleLatencyEval(final long start) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    latencyCounts.add(System.currentTimeMillis() - start);
                    if(queueCounts.size()>1 || queueCountsHistory.size()>1) {
                        int q=0;
                        long maxQ=-1;
                        double max=0.0;
                        for(Map.Entry<Long, AverageOfN> entry: queueCountsHistory.entrySet()) {
                            Integer qCount = queueCounts.remove(entry.getKey());
                            entry.getValue().add((qCount == null)? 0L : qCount);
                        }
                        for(Map.Entry<Long, Integer> entry: queueCounts.entrySet()) {
                            AverageOfN aon = new AverageOfN();
                            aon.add(entry.getValue());
                            queueCountsHistory.put(entry.getKey(), aon);
                        }
                        for(Map.Entry<Long, AverageOfN> entry: queueCountsHistory.entrySet()) {
                            AverageOfN qAv = entry.getValue();
                            if(qAv.getAverage()>EPS) {
                                q++;
                                if(qAv.getAverage()>max) {
                                    max = qAv.getAverage();
                                    maxQ = entry.getKey();
                                }
                            }
                        }
                        if(q>1) {
                            int dest = CoreThreadWorkers.INSTANCE.shouldReroute(myIndex);
                            if(dest>=0) {
                                if(dest==myIndex)
                                    endRerouting();
                                else {
                                    // reroute maxQ
                                    final long reroutedQ = maxQ;
                                    WorkerQueues.INSTANCE.getWorkerQueue(reroutedQ).rerouteTo(dest, new Action0() {
                                        @Override
                                        public void call() {
                                            removeQ(reroutedQ);
                                            endRerouting();
                                        }
                                    });
                                }
                            }
                        }
                    }
                    latencyEvalWrapper();
                }
            });
        }

        long getCount() {
            return counter;
        }

        Future submit(final long workerQNum, final Runnable runnable) {
            return executor.submit(new Runnable() {
                @Override
                public void run() {
                    counter++;
                    Integer qCount = queueCounts.get(workerQNum);
                    if(qCount == null)
                        queueCounts.put(workerQNum, new Integer(1));
                    else
                        queueCounts.put(workerQNum, qCount + 1);
                    runnable.run();
                }
            });
        }

        Future schedule(final long workerQNum, final Runnable runnable, long delayTime, TimeUnit unit) {
            return executor.schedule(new Runnable() {
                @Override
                public void run() {
                    counter++;
                    Integer qCount = queueCounts.get(workerQNum);
                    if(qCount == null)
                        queueCounts.put(workerQNum, new Integer(1));
                    else
                        queueCounts.put(workerQNum, qCount + 1);
                    runnable.run();
                }}, delayTime, unit);
        }

        private void removeQ(long qNumber) {
            queueCounts.remove(qNumber);
            queueCountsHistory.remove(qNumber);
        }

        void remove(final long workerQNum) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    removeQ(workerQNum);
                }
            });
        }
    }

    private static class WorkerQueues {
        static final WorkerQueues INSTANCE = new WorkerQueues();
        private final ConcurrentMap<Long, WorkerQueue> workerQMap;
        final AtomicLong queueCounter;

        private WorkerQueues() {
            workerQMap = new ConcurrentHashMap<Long, WorkerQueue>();
            queueCounter = new AtomicLong();
        }

        WorkerQueue createWorkerQueue() {
            long idx = queueCounter.getAndIncrement();
            WorkerQueue workerQueue = new WorkerQueue(idx, CoreThreadWorkers.INSTANCE.getNextWorker());
            workerQMap.put(idx, workerQueue);
            return workerQueue;
        }

        WorkerQueue getWorkerQueue(long idx) {
            return workerQMap.get(idx);
        }

        void removeWorker(long idx) {
            workerQMap.remove(idx);
        }
    }

    // ToDo: confirm that Worker.schedule(...) methods are called sequentially
    private static class WorkerQueue extends Scheduler.Worker {
        private final long myIndex;
        private volatile CoreThreadWorker worker;
        private final CompositeSubscription innerSubscription = new CompositeSubscription();
        private volatile CoreThreadWorker transferDest=null;
        private volatile Action0 onTransferComplete=null;
        private final AtomicBoolean transferCompletionReady = new AtomicBoolean(false);

        WorkerQueue(long myIndex, CoreThreadWorker worker) {
            this.myIndex = myIndex;
            this.worker = worker;
        }

        void rerouteTo(int dest, Action0 onComplete) {
            onTransferComplete = onComplete;
            transferDest = CoreThreadWorkers.INSTANCE.CORE_THREAD_WORKERS[dest];
        }

        private void awaitReroute() {
            transferCompletionReady.set(false);
            worker.submit(myIndex, new Runnable() {
                @Override
                public void run() {
                    synchronized (transferCompletionReady) {
                        transferCompletionReady.set(true);
                        transferCompletionReady.notify();
                    }
                }
            });
            while (!transferCompletionReady.get()) {
                synchronized (transferCompletionReady) {
                    try {
                        transferCompletionReady.wait(10);
                    } catch (InterruptedException e) {}
                }
            }
        }

        private boolean rerouteIfNeeded() {
            if(transferDest == null)
                return false;
            awaitReroute();
            worker = transferDest;
            transferDest = null;
            onTransferComplete.call();
            return true;
        }

        @Override
        public Subscription schedule(final Action0 action) {
            rerouteIfNeeded();
            final AtomicReference<Subscription> sf = new AtomicReference<Subscription>();
            Subscription s = Subscriptions.from(worker.submit(myIndex, new Runnable() {
                @Override
                public void run() {
                    try {
                        if(innerSubscription.isUnsubscribed())
                            return;
                        action.call();
                    }
                    finally {
                        Subscription s = sf.get();
                        if (s != null) {
                            innerSubscription.remove(s);
                        }
                    }
                }
            }));
            sf.set(s);
            innerSubscription.add(s);
            return s;
        }

        @Override
        public Subscription schedule(final Action0 action, long delayTime, TimeUnit unit) {
//            if(rerouteIfNeeded(action)) // need to handle delayed action
//                return SomeUsefulSubscription;
            final AtomicReference<Subscription> sf = new AtomicReference<Subscription>();
            Subscription s = Subscriptions.from(worker.schedule(myIndex, new Runnable() {
                @Override
                public void run() {
                    try {
                        if (innerSubscription.isUnsubscribed())
                            return;
                        action.call();
                    } finally {
                        Subscription s = sf.get();
                        if (s != null) {
                            innerSubscription.remove(s);
                        }
                    }
                }
            }, delayTime, unit));
            sf.set(s);
            innerSubscription.add(s);
            return s;
        }

        @Override
        public void unsubscribe() {
            innerSubscription.unsubscribe();
            WorkerQueues.INSTANCE.removeWorker(myIndex);
            worker.remove(myIndex);
        }

        @Override
        public boolean isUnsubscribed() {
            return innerSubscription.isUnsubscribed();
        }
    }
}
