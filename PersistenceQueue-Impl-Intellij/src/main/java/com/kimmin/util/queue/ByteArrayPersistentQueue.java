package com.kimmin.util.queue;

/**
 * Created by min.jin on 2016/2/19.
 */

import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;


import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;


public class ByteArrayPersistentQueue implements PersistentQueue<byte[]> {

    private static final int PAGE_SIZE_MB = 128;
    private static final int PAGE_SIZE = 1024 * 1024 * PAGE_SIZE_MB; // 128 MB Page Size
    private static final int OBJECT_SIZE_LIMIT = 1024 * 1024 * 32; // 32 MB Object Size limit

    private static final int FILE_CLEAN_INTERVAL_SECONDS = 20;

    private RandomAccessFile readDataFile; //random access file for data
    private RandomAccessFile writeDataFile; //random access file for data

    private FileChannel readDataChannel; // channel for read data
    private FileChannel writeDataChannel; // channel for write data

    private MappedByteBuffer readMbb; // buffer used to read;
    private MappedByteBuffer writeMbb; // buffer used to write

    private File currentReadingFile;
    private File currentWritingFile;

    private boolean backlogAvailable = false; // If the persistent file has backlog already

    private final  int headerLength = 5;
    private final  byte READ = (byte) 0;
    private final  byte NOT_READ = (byte) 1;
    private final  byte MM_EOF = (byte) 2;
    private final  byte RAW_BYTES = (byte) 3;

    // 1 byte for status of the message, 4 bytes length of the payload
    private final ByteBuffer header = ByteBuffer.allocate(headerLength);
    private  final  int endingLength = 5;

    private long readFileIndex = 0; // read index file
    private long writeFileIndex = 0; // write index file

    private static final  int SIZE_OF_INT = 4;

    // remove old used files
    private Timer fileCleanTimer;

    private long maxFileSize;

    private static final String DATA_FILE_SUFFIX = ".dat";


    private String fileNamePrefix; // persistence file name
    private File queueDir; // queue directory

    public ByteArrayPersistentQueue(String dir,String qName,int maxFileSize) throws IOException {
        if (dir == null || dir.trim().length() == 0) {
            throw new IllegalArgumentException("dir is empty");
        }
        if (qName == null || qName.trim().length() == 0) {
            throw new IllegalArgumentException("name is empty");
        }
        String qDir = dir;
        if (!qDir.endsWith("/")) {
            qDir += File.separator;
        }
        qDir += qName;
        queueDir = new File(qDir);
        if (!queueDir.exists()) {
            queueDir.mkdirs();
        }
        this.fileNamePrefix = queueDir.getPath();
        if (!this.fileNamePrefix.endsWith("/")) {
            this.fileNamePrefix += File.separator;
        }
        this.fileNamePrefix += "data";
        if (maxFileSize < 0 || maxFileSize % PAGE_SIZE_MB != 0) {
            throw new IllegalArgumentException("max file size must be positive and a multiple of "
                    + PAGE_SIZE_MB + " MB");
        }
        this.maxFileSize = maxFileSize;
        this.init();
    }

    private void init() throws IOException {
        initFiles();
        this.initReadBuffer();
        this.initWriteBuffer();

        //fileCleanTimer = new Thread(new FileCleanTask()).start();
        fileCleanTimer=new Timer();
        fileCleanTimer.schedule(new FileCleanTask(),1000,FILE_CLEAN_INTERVAL_SECONDS*1000);
    }

    private TreeMap<Long, File> files = new TreeMap<Long, File>();

    private BlockingQueue<File> toDeleteFiles = new LinkedBlockingQueue<File>();

    private void initFiles() {
        File[] queueFiles = queueDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(DATA_FILE_SUFFIX);
            }
        });
        if (queueFiles != null) {
            for (File queueFile : queueFiles) {
                String fileName = queueFile.getName();
                int beginIndex = fileName.lastIndexOf('-') + 1;
                int endIndex = fileName.lastIndexOf(DATA_FILE_SUFFIX);
                String sIndex = fileName.substring(beginIndex, endIndex);
                long index = Long.parseLong(sIndex);
                files.put(index, queueFile);
            }
        }
        //  write to latest file.
        if (!files.isEmpty()) {
            writeFileIndex = files.lastKey();
            files.remove(writeFileIndex);
        }
        // read to second latest file.
        if (!files.isEmpty()) {
            readFileIndex = files.lastKey();
            files.remove(readFileIndex);
        } else {
            readFileIndex = writeFileIndex;
        }
    }


    private void initReadBuffer() throws IOException {
        currentReadingFile = new File(this.fileNamePrefix + "-" + readFileIndex + DATA_FILE_SUFFIX);

        readDataFile = new RandomAccessFile(currentReadingFile, "rw");
        readDataChannel = readDataFile.getChannel();
        readMbb = readDataChannel.map(READ_WRITE, 0, PAGE_SIZE); // create the read buffer with readPosition 0 initially
        int position = readMbb.position();
        byte active = readMbb.get(); // first byte to see whether the message is already read or not
        int length = readMbb.getInt(); // next four bytes to see the data length

        while (active == READ && length > 0) { // message is non active means, its read, so skipping it
            readMbb.position(position + headerLength + length); // skipping the read bytes
            position = readMbb.position();
            active = readMbb.get();
            length = readMbb.getInt();
        }
        if (active == NOT_READ || active == RAW_BYTES) {
            backlogAvailable = true; // the file has unconsumed message(s)
        }
        readMbb.position(position);
    }

    private void initWriteBuffer() throws IOException {
        currentWritingFile = new File(this.fileNamePrefix + "-" + writeFileIndex + DATA_FILE_SUFFIX);

        writeDataFile = new RandomAccessFile(currentWritingFile, "rw");
        writeDataChannel = writeDataFile.getChannel();

        // start the write buffer with writePosition 0 initially
        writeMbb = writeDataChannel.map(READ_WRITE, 0, PAGE_SIZE);
        int position = writeMbb.position();
        byte active = writeMbb.get();
        int length = writeMbb.getInt();
        while (length > 0) { // message is there, so skip it, keep doing until u get the end
            writeMbb.position(position + headerLength + length);
            position = writeMbb.position();
            active = writeMbb.get();
            length = writeMbb.getInt();
        }
        writeMbb.position(position);
    }

    @Override
    public byte[] consume() {
        return this.consumeFromDiskFile();
    }

    @Override
    public boolean produce(byte[] t) {
        return this.produceToDiskFile(t);
    }


    private synchronized byte[] consumeFromDiskFile() {
        try {
            int currentPosition = readMbb.position();
            byte active = readMbb.get();
            int length;
            // end of the file
            if (active == MM_EOF) {
                finishCurrentFile();
                setCurrentReadingFile();
                if (currentReadingFile == null) {
                    return null;
                }
                currentPosition = readMbb.position();
                active = readMbb.get();
                length = readMbb.getInt();
                while (active == READ && length > 0) { // message is non active means, its read, so skipping it
                    readMbb.position(currentPosition + headerLength + length); // skipping the read bytes
                    currentPosition = readMbb.position();
                    active = readMbb.get();
                    length = readMbb.getInt();
                }
            } else {
                length = readMbb.getInt();
            }

            // active is wrong or length is wrong
            if (active != RAW_BYTES || length <= 0) {
                readMbb.position(currentPosition);
                if (writeFileIndex != readFileIndex) {
                    // add by ske, 2015-10-13:
                    // to skip broken queue file:
                    setCurrentReadingFile();
                }
                return null; // the queue is empty
            }
            byte[] bytes = new byte[length];
            readMbb.get(bytes);
            readMbb.put(currentPosition, READ); // making it not active (deleted)
            if (active == NOT_READ) {
                return toObject(bytes);
            } else if (active == RAW_BYTES) {
                return bytes;
            }
        } catch (Throwable e) {
        }
        return null;
    }


    private void finishCurrentFile() {
        unMap(readMbb);
        closeResource(readDataChannel);
        closeResource(readDataFile);
        toDeleteFiles.add(currentReadingFile);
        currentReadingFile = null;
    }

    private void setCurrentReadingFile() throws IOException {
        if (readFileIndex == writeFileIndex) {
            return;
        }
        if (!files.isEmpty()) {
            readFileIndex = files.lastKey();
            files.remove(readFileIndex);
        } else {
            readFileIndex = writeFileIndex;
        }
        currentReadingFile = new File(this.fileNamePrefix + "-" + readFileIndex + DATA_FILE_SUFFIX);

        readDataFile = new RandomAccessFile(currentReadingFile, "rw");
        readDataChannel = readDataFile.getChannel();
        readMbb = readDataChannel.map(READ_WRITE, 0, PAGE_SIZE);
    }

    private synchronized boolean produceToDiskFile(byte[] bytes) {
        try {
            int length = bytes.length;
            if (length == 0) {
                return false;
            }
            if (length > OBJECT_SIZE_LIMIT) {
                return false;
            }

            //prepare the header
            prepareHeader(length);

            // check weather current buffer is enuf, otherwise we need to change the buffer
            if (writeMbb.remaining() < headerLength + length + endingLength) {
                writeMbb.put(MM_EOF); // the end
//                writeMbb.force();
                unMap(writeMbb);
                closeResource(writeDataChannel);
                closeResource(writeDataFile);
                if (writeFileIndex != readFileIndex) {
                    files.put(writeFileIndex, currentWritingFile);
                }
                writeFileIndex++;
                currentWritingFile = new File(this.fileNamePrefix + "-" + writeFileIndex + DATA_FILE_SUFFIX);
                writeDataFile = new RandomAccessFile(currentWritingFile, "rw");
                writeDataChannel = writeDataFile.getChannel();

                // start the write buffer with writePosition 0 initially
                writeMbb = writeDataChannel.map(READ_WRITE, 0, PAGE_SIZE);
            }

            writeMbb.put(header); // write header
            writeMbb.put(bytes);

            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    private void prepareHeader(int length) {
        header.clear();
        header.put(RAW_BYTES);
        header.putInt(length);
        header.flip();
    }


    private static void unMap(MappedByteBuffer buffer) {
        if (buffer == null) return;
        sun.misc.Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
        if (cleaner != null) {
            cleaner.clean();
        }
    }

    protected byte[] getBytes(byte[] o) throws IOException {
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            // TODO: improvable by using a SeDe framewrok.
            oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            oos.flush();
            return bos.toByteArray();
        } finally {
            closeResource(bos);
            closeResource(oos);
        }
    }

    protected <T> T toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            return (T) ois.readObject();
        } finally {
            closeResource(bis);
            closeResource(ois);
        }
    }

    private static void closeResource(Closeable c) {
        try {
            if (c != null) c.close();
        } catch (Exception ignore) {            /* Do Nothing */
        }
    }

    @Override
    public long getRemainingCapacity() {
        return Integer.MAX_VALUE; // fake still
    }

    @Override
    public long getTotalCapacity() {
        return maxFileSize; // fake still
    }

    @Override
    public long getUsedCapacity() {
        return getBackFileSize();
    }

    /**
     * file size in MB
     */
    public int getBackFileSize() {
//        if (writeFileIndex >= readFileIndex) {
//            return (int) ((writeFileIndex - readFileIndex + 1) * PAGE_SIZE_MB);
//        } else {
//            return (int) ((Integer.MAX_VALUE - readFileIndex + writeFileIndex + 2) * PAGE_SIZE_MB);
//        }
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return this.readFileIndex == this.writeFileIndex && readMbb.position() == writeMbb.position();
    }

    public boolean isBacklogAvailable() {
        return backlogAvailable;
    }

    public void shutdown() {
        // stop file cleaner
        fileCleanTimer.cancel();

        if (writeMbb != null) {
            writeMbb.force();
            unMap(writeMbb);
        }
        if (readMbb != null) {
            readMbb.force();
            unMap(readMbb);
        }

        closeResource(readDataChannel);
        closeResource(readDataFile);
        closeResource(writeDataChannel);
        closeResource(writeDataFile);


    }

    @Override
    public long getOverflowCount() {
        return 0;
    }


    /**
     * Periodically delete old used file which are not in current
     * read/write window.
     *
     * @author yangbo
     */
    class FileCleanTask extends TimerTask {

        public void run() {
            while (true) {
                File file = null;
                try {
                    file = toDeleteFiles.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                file.delete();
            }
        }
    }

}
