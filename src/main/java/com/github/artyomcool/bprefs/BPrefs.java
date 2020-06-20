package com.github.artyomcool.bprefs;

import android.content.SharedPreferences;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BPrefs implements SharedPreferences {

    private static final ExecutorService DEFAULT_EXECUTOR = Executors.newCachedThreadPool();
    private static final Object TOMBSTONE = new Object();

    private final ExecutorService executor;
    private final File file;
    private final File backup;

    // intentionally not volatile
    private Future<?> read;

    private final Set<OnSharedPreferenceChangeListener> listeners = Collections.newSetFromMap(new WeakHashMap<>());
    private final AtomicInteger diskWriteInProcess = new AtomicInteger();

    private final ArrayList<Object> dataCopy = new ArrayList<>();

    private final ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

    private volatile Future<Boolean> submitted = null;
    private volatile ConcurrentHashMap<String, Object> patch = null;

    public BPrefs(File file) {
        this(file, DEFAULT_EXECUTOR, false);
    }

    public BPrefs(File file, ExecutorService executor, boolean initInThisThread) {
        this.file = file;
        this.backup = new File(file.getParent(), file.getName() + ".bak");
        this.executor = executor;
        if (initInThisThread) {
            @SuppressWarnings("unused")
            boolean success = readFromDisk();
            // TODO some reports?
        } else {
            //noinspection Convert2Lambda
            read = executor.submit(new Runnable() {
                @Override
                public void run() {
                    @SuppressWarnings("unused")
                    boolean success = readFromDisk();
                    // TODO some reports?
                }
            });
        }
    }

    @Override
    public Map<String, ?> getAll() {
        synchronized (map) {
            // TODO non-copy option
            return new HashMap<>(map);
        }
    }

    @Override
    public String getString(String key, String defValue) {
        return getObject(key, defValue);
    }

    @Override
    public Set<String> getStringSet(String key, Set<String> defValues) {
        return getObject(key, defValues);
    }

    @Override
    public int getInt(String key, int defValue) {
        return getObject(key, defValue);
    }

    // TODO reduce memory footprint by storing primitives separately
    @Override
    public long getLong(String key, long defValue) {
        return getObject(key, defValue);
    }

    @Override
    public float getFloat(String key, float defValue) {
        return getObject(key, defValue);
    }

    @Override
    public boolean getBoolean(String key, boolean defValue) {
        return getObject(key, defValue);
    }

    @Override
    public boolean contains(String key) {
        return getObject(key, null) != null;
    }

    private boolean readFromDisk() {
        if (backup.exists()) {
            if (!file.delete()) {
                if (file.exists()) {
                    return false;
                }
            }
            if (!backup.renameTo(file)) {
                return false;
            }
        }

        try {
            try (DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                int count = stream.readInt();
                Map<String, Object> map = new ConcurrentHashMap<>();    // TODO pool?
                for (int i = 0; i < count; i++) {
                    String key = stream.readUTF();
                    map.put(key, readValue(stream));
                }
                this.map.putAll(map);
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private Object readValue(DataInputStream stream) throws IOException {
        byte type = stream.readByte();
        switch (type) {
            case 0:
                return Boolean.FALSE;
            case 1:
                return Boolean.TRUE;
            case 2:
                return stream.readUTF();
            case 3:
                return stream.readInt();
            case 4:
                return stream.readLong();
            case 5:
                return stream.readFloat();
            case 6:
                int size = stream.readInt();
                Set<String> set = new HashSet<>();
                for (int j = 0; j < size; j++) {
                    set.add(stream.readUTF());
                }
                return set;
            default:
                throw new IOException("Unknown type exception: " + type);
        }
    }

    private boolean writeToDisk() {
        synchronized (dataCopy) {
            dataCopy.ensureCapacity(map.size() * 2 + 16);
            synchronized (map) {
                for (Map.Entry<String, Object> e : map.entrySet()) {
                    dataCopy.add(e.getKey());
                    dataCopy.add(e.getValue());
                }
            }

            if (!backup.delete()) {
                if (backup.exists()) {
                    return false;
                }
            }

            if (!file.renameTo(backup)) {
                return false;
            }

            try {
                if (!file.createNewFile()) {
                    return false;
                }

                // TODO reuse buffer
                try (DataOutputStream stream =
                             new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
                    writeData(stream);
                }

                return backup.delete();
            } catch (IOException e) {
                return false;
            }
        }
    }

    private void writeData(DataOutputStream stream) throws IOException {
        int count = dataCopy.size() / 2;
        stream.writeInt(count);
        for (int i = 0; i < count; i++) {
            String key = (String) dataCopy.get(i * 2);
            Object value = dataCopy.get(i * 2 + 1);

            stream.writeUTF(key);
            if (value instanceof Boolean) {
                stream.writeByte((Boolean) value ? 1 : 0);
            } else if (value instanceof String) {
                stream.writeByte(2);
                stream.writeUTF((String) value);
            } else if (value instanceof Integer) {
                stream.writeByte(3);    // TODO shortcuts
                stream.writeInt((Integer) value);
            } else if (value instanceof Long) {
                stream.writeByte(4);
                stream.writeLong((Long) value);
            } else if (value instanceof Float) {
                stream.writeByte(5);
                stream.writeFloat((Float) value);
            } else if (value instanceof Set) {
                @SuppressWarnings("unchecked")
                Set<String> set = (Set<String>) value;
                int size = set.size();

                stream.writeByte(6);
                stream.writeInt(size);
                for (String element : set) {
                    stream.writeUTF(element);
                }
            }
        }
    }

    private boolean awaitWriteToDisk() {
        try {
            Future<Boolean> submitted = this.submitted;
            if (submitted == null) {
                // something wrong here - there should be submitted future
                return false;
            }
            return submitted.get();
        } catch (Exception e) {
            return false;
        }
    }

    @SuppressWarnings("Convert2Lambda")
    private void submitWriteToDiskTask() {
        submitted = executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                for (;;) {
                    int started = diskWriteInProcess.get();
                    boolean success = writeToDisk();
                    if (diskWriteInProcess.compareAndSet(started, 0)) {
                        return success;
                    }
                }
            }
        });
    }

    @Override
    public Editor edit() {
        return new SharedPreferences.Editor() {

            // TODO pool
            private final AtomicReference<ConcurrentHashMap<String, Object>> transaction =
                    new AtomicReference<>(new ConcurrentHashMap<>());

            @Override
            public Editor putString(String key, String value) {
                return put(key, value);
            }

            @Override
            public Editor putStringSet(String key, Set<String> values) {
                return put(key, new HashSet<>(values));
            }

            @Override
            public Editor putInt(String key, int value) {
                return put(key, value);
            }

            @Override
            public Editor putLong(String key, long value) {
                return put(key, value);
            }

            @Override
            public Editor putFloat(String key, float value) {
                return put(key, value);
            }

            @Override
            public Editor putBoolean(String key, boolean value) {
                return put(key, value);
            }

            @Override
            public Editor remove(String key) {
                return put(key, null);
            }

            @Override
            public Editor clear() {
                ConcurrentHashMap<String, Object> map = transaction.get();
                if (map instanceof ClearMarkerMap) {
                    map.clear();
                } else {
                    // TODO pool
                    transaction.set(new ClearMarkerMap<>());
                }
                return this;
            }

            @Override
            public boolean commit() {
                if (!commitToMemory()) {
                    return true;
                }

                int writesInProcess = diskWriteInProcess.getAndIncrement();
                if (writesInProcess != 0) {
                    return awaitWriteToDisk();
                }

                int started = diskWriteInProcess.get();
                boolean success = writeToDisk();
                if (!diskWriteInProcess.compareAndSet(started, 0)) {
                    submitWriteToDiskTask();
                }
                return success;
            }

            @Override
            public void apply() {
                if (!commitToMemory()) {
                    return;
                }
                int writesInProcess = diskWriteInProcess.getAndIncrement();
                if (writesInProcess == 0) {
                    submitWriteToDiskTask();
                }
            }

            private Editor put(String key, Object value) {
                if (value == null) {
                    value = TOMBSTONE;
                }

                ConcurrentHashMap<String, Object> transaction = this.transaction.get();
                transaction.put(key, value);

                return this;
            }

            @SuppressWarnings("BooleanMethodIsAlwaysInverted")
            private boolean commitToMemory() {
                synchronized (map) {
                    ConcurrentHashMap<String, Object> transaction = this.transaction.get();

                    patch = transaction;
                    if (transaction instanceof ClearMarkerMap) {
                        map.clear();
                    }
                    for (Map.Entry<String, ?> e : transaction.entrySet()) {
                        if (e.getValue() == TOMBSTONE) {
                            map.remove(e.getKey());
                        } else {
                            map.put(e.getKey(), e.getValue());
                        }
                    }
                    patch = null;
                    if (transaction instanceof ClearMarkerMap) {
                        this.transaction.set(new ConcurrentHashMap<>()); // TODO pool
                    } else {
                        transaction.clear();
                    }
                    return true;
                }
            }
        };
    }

    @Override
    public void registerOnSharedPreferenceChangeListener(OnSharedPreferenceChangeListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    @Override
    public void unregisterOnSharedPreferenceChangeListener(OnSharedPreferenceChangeListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getObject(String key, T def) {
        Future<?> read = this.read;
        if (read != null) {
            try {
                read.get();
                this.read = null;
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }
        ConcurrentHashMap<String, ?> patch = this.patch;
        if (patch != null) {
            Object result = patch.get(key);
            if (result != null) {
                return result == TOMBSTONE ? def : (T) result;
            }

            if (patch instanceof ClearMarkerMap) {
                return def;
            }
        }
        Object result = map.get(key);
        return result == null ? def : (T) result;
    }

    private static class ClearMarkerMap<K, V> extends ConcurrentHashMap<K, V> {}

}
