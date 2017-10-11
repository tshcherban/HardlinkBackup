//#define LOGIO

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    public static class HashSumHelper
    {
        private const string LogFile = @"C:\0\log.txt";

        public static byte[] ComputeSha1Unbuffered(string filePath)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions) 0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

            const int chunkSize = 32 * 1024 * 1024;

            var readBufferSize = chunkSize;
            readBufferSize += ((readBufferSize + 1023) & ~1023) - readBufferSize;

            var dataToProcess = new ConcurrentQueue<Buffer>();
            var reusableData = new ConcurrentQueue<Buffer>();

            using (HashAlgorithm sha = SHA1.Create())
            {
                var allowExit = false;
                var e = new ManualResetEvent(false);

                int rcount = 1, ccount = 1;

                var task = Task.Factory.StartNew(() =>
                {
                    while (true)
                    {
                        if (dataToProcess.TryDequeue(out var buffer))
                        {
                            Debug.WriteLine($"Computing... {ccount++}");
                            if (!buffer.IsLast)
                                sha.TransformBlock(buffer.Data, 0, buffer.Length, buffer.Data, 0);
                            else
                                sha.TransformFinalBlock(buffer.Data, 0, buffer.Length);

                            buffer.IsLast = false;
                            buffer.Length = 0;
                            reusableData.Enqueue(buffer);
                        }
                        else if (allowExit)
                            return;
                        e.Set();
                    }
                }, TaskCreationOptions.LongRunning);

                using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                    readBufferSize, fileOptions))
                {
                    var length = stream.Length;
                    var toRead = length;

                    var readSize = Convert.ToInt32(Math.Min(chunkSize, length));

                    while (toRead > 0)
                    {
                        if (dataToProcess.Count > 0)
                        {
                            Debug.WriteLine("Waiting...");
                            e.WaitOne();
                            e.Reset();
                        }

                        if (!reusableData.TryDequeue(out var buffer))
                        {
                            Debug.WriteLine("Allocating...");
                            var array = new byte[readSize];
                            buffer = new Buffer {Data = array};
                        }

                        Debug.WriteLine($"Reading...{rcount++}");
                        buffer.Length = stream.Read(buffer.Data, 0, readSize);
                        if (buffer.Length == 0)
                            throw new EndOfStreamException("Read beyond end of file EOF");

                        toRead -= buffer.Length;

                        buffer.IsLast = toRead == 0;

                        dataToProcess.Enqueue(buffer);
                    }

                    allowExit = true;

                    task.Wait();

                    while (reusableData.TryDequeue(out var _)) ;
                    while (dataToProcess.TryDequeue(out var _)) ;

                    reusableData = null;
                    dataToProcess = null;

                    return sha.Hash;
                }
            }
        }

        private const int BufferSizeMib = 64;
        private const int BuffersCount = 4;
        public static async Task<byte[]> CopyUnbufferedAndComputeHashAsync(string filePath, string destinationPath, Action<double> progressCallback, bool allowSimultaneousIo)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions) 0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

            const int chunkSize = BufferSizeMib * 1024 * 1024;

            var readBufferSize = chunkSize;
            readBufferSize += ((readBufferSize + 1023) & ~1023) - readBufferSize;

            using (HashAlgorithm sha = SHA1.Create())
            {
                using (var sourceStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fileOptions))
                using (var destinationStream = new FileStream(destinationPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, readBufferSize, FileOptions.WriteThrough))
                {
                    var length = sourceStream.Length;
                    var toRead = length;

                    var readSize = Convert.ToInt32(Math.Min(chunkSize, length));

                    var buffer = new LightBuffer[BuffersCount];
                    for (var i = 0; i < BuffersCount; ++i)
                        buffer[i] = new LightBuffer(readSize) {Number = -1 * i};

                    void Increment(ref int idx)
                    {
                        idx++;
                        if (idx > BuffersCount - 1)
                            idx = 0;
                    }

                    var locker = new object();

                    var sw = new Performance.Stopwatch();
                    sw.Calibrate();
                    sw.Reset();
                    sw.Start();
                    
                    var perf = new List<(double time, string value, int number)>();

#if LOGIO
                    var cnt = 0;
#endif

                    var readTask = Task.Run(async () =>
                    {
#if LOGIO
                        var readCount = 0;
#endif
                        var blockNum = 0;
                        var readIdx = 0;
                        while (toRead > 0)
                        {
                            var lightBuffer = buffer[readIdx];
                            lightBuffer.WriteDone.WaitOne();
                            lightBuffer.WriteDone.Reset();
                            lightBuffer.Number = ++blockNum;

                            if (allowSimultaneousIo)
                            {
#if LOGIO
                                Console.WriteLine($"R{++readCount} start {++cnt}");
#endif
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "RS", lightBuffer.Number));
                                lightBuffer.Length = sourceStream.Read(lightBuffer.Data, 0, readSize);
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "RE", lightBuffer.Number));
                                if (lightBuffer.Length == 0)
                                    throw null;

                                await Task.Delay(1000);
#if LOGIO
                                Console.WriteLine($"R{readCount} end {++cnt}");
#endif
                            }
                            else
                            {
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "LRS", lightBuffer.Number));
                                lock (locker)
                                {
                                    perf.Add((sw.GetSplitTimeInMicroseconds(), "LRE", lightBuffer.Number));
#if LOGIO
                                    Console.WriteLine($"R{++readCount} start {++cnt}");
#endif
                                    perf.Add((sw.GetSplitTimeInMicroseconds(), "RS", lightBuffer.Number));
                                    lightBuffer.Length = sourceStream.Read(lightBuffer.Data, 0, readSize);
                                    perf.Add((sw.GetSplitTimeInMicroseconds(), "RE", lightBuffer.Number));
                                    if (lightBuffer.Length == 0)
                                        throw null;
#if LOGIO
                                    Console.WriteLine($"R{readCount} end {++cnt}");
#endif
                                }}

                            toRead -= lightBuffer.Length;

                            lightBuffer.IsFinal = toRead == 0;
                            lightBuffer.DataReady.Set();

                            Increment(ref readIdx);
                        }
                    });

                    var writeTask = Task.Run(async () =>
                    {
                        var writeIdx = 0;
                        var run = true;
#if LOGIO
                        var writeCount = 0;
#endif
                        var writeDone = 0L;
                        while (run)
                        {
                            var lightBuffer = buffer[writeIdx];

                            perf.Add((sw.GetSplitTimeInMicroseconds(), "CWWS", lightBuffer.Number));
                            lightBuffer.DataReady.WaitOne();
                            lightBuffer.DataReady.Reset();
                            perf.Add((sw.GetSplitTimeInMicroseconds(), "CWWE", lightBuffer.Number));

                            var hashTask = Task.Factory.StartNew(() =>
                            {
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "CS", lightBuffer.Number));
                                if (lightBuffer.IsFinal)
                                {
                                    sha.TransformFinalBlock(lightBuffer.Data, 0, lightBuffer.Length);
                                    run = false;
                                }
                                else
                                    sha.TransformBlock(lightBuffer.Data, 0, lightBuffer.Length, null, 0);
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "CE", lightBuffer.Number));
                            }, TaskCreationOptions.LongRunning);

                            if (allowSimultaneousIo)
                            {
#if LOGIO
                                Console.WriteLine($"W{++writeCount} start {++cnt}");
#endif
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "WS", lightBuffer.Number));
                                destinationStream.Write(lightBuffer.Data, 0, lightBuffer.Length);
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "WE", lightBuffer.Number));
                                await Task.Delay(1000);
#if LOGIO
                                Console.WriteLine($"W{writeCount} end {++cnt}");
#endif
                            }
                            else
                            {
                                perf.Add((sw.GetSplitTimeInMicroseconds(), "LWS", lightBuffer.Number));
                                lock (locker)
                                {
                                    perf.Add((sw.GetSplitTimeInMicroseconds(), "LWE", lightBuffer.Number));
#if LOGIO
                                    Console.WriteLine($"W{++writeCount} start {++cnt}");
#endif
                                    perf.Add((sw.GetSplitTimeInMicroseconds(), "WS", lightBuffer.Number));
                                    destinationStream.Write(lightBuffer.Data, 0, lightBuffer.Length);
                                    perf.Add((sw.GetSplitTimeInMicroseconds(), "WE", lightBuffer.Number));
#if LOGIO
                                    Console.WriteLine($"W{writeCount} end {++cnt}");
#endif
                                }
                            }

                            await hashTask;

                            writeDone += lightBuffer.Length;

                            lightBuffer.WriteDone.Set();

                            progressCallback?.BeginInvoke((double) writeDone / length * 100d, ar => { }, null);

                            Increment(ref writeIdx);
                        }
                    });

                    await Task.WhenAll(readTask, writeTask);


                    perf = perf.OrderBy(i => i.time).ToList();

                    if (File.Exists(LogFile))
                        File.Delete(LogFile);

                    foreach (var i in perf)
                    File.AppendAllText(LogFile, $"{i.time:#00000.00}\t{i.value}\t{i.number}\r\n");
                    /*                    var rLevel = 0;
                                        var wLevel = 0;
                                        var cLevel = 0;
                                        foreach (var i in perf)
                                        {
                                            if (i.value == "RS")
                                            {
                                                if (rLevel == 1)
                                                    throw null;

                                                rLevel = 1;
                                            }
                                            else if (i.value == "RE")
                                            {
                                                if (rLevel == 0)
                                                    throw null;

                                                rLevel = 0;
                                            }
                                            else if (i.value == "WS")
                                            {
                                                if (wLevel == 1)
                                                    throw null;

                                                wLevel = 1;
                                            }
                                            else if (i.value == "WE")
                                            {
                                                if (wLevel == 0)
                                                    throw null;

                                                wLevel = 0;
                                            }
                                            else if (i.value == "CS")
                                            {
                                                if (cLevel == 1)
                                                    throw null;

                                                cLevel = 1;
                                            }
                                            else if (i.value == "CE")
                                            {
                                                if (cLevel == 0)
                                                    throw null;

                                                cLevel = 0;
                                            }
                                            else if (i.value == "D")
                                            {

                                            }
                                            else throw null;

                                            File.AppendAllText(LogFile, $"{i.time:#.00}\t{rLevel}\t{wLevel}\t{cLevel}\t{i.value}\r\n");
                                        }*/



                    return sha.Hash;
                }
            }
        }

        private class LightBuffer
        {
            public LightBuffer(int size)
            {
                Data = new byte[size];
            }

            public byte[] Data { get; }

            public int Length { get; set; }

            public ManualResetEvent DataReady { get; } = new ManualResetEvent(false);

            public ManualResetEvent WriteDone { get; } = new ManualResetEvent(true);

            public bool IsFinal { get; set; }

            public int Number { get; set; }
        }

        private class Buffer
        {
            public byte[] Data { get; set; }

            public int Length { get; set; }

            public bool IsLast { get; set; }
        }
    }
}

namespace Performance
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// 
    /// </summary>
    public class Stopwatch
    {
        [DllImport("kernel32.dll")]
        private static extern short QueryPerformanceCounter(ref long x);
        [DllImport("kernel32.dll")]
        private static extern short QueryPerformanceFrequency(ref long x);

        private long _startTime;
        private long _stopTime;
        private long _clockFrequency;
        private long _calibrationTime;

        public Stopwatch()
        {
            _startTime = 0;
            _stopTime = 0;
            _clockFrequency = 0;
            _calibrationTime = 0;
            Calibrate();
        }

        public void Calibrate()
        {
            QueryPerformanceFrequency(ref _clockFrequency);

            for (var i = 0; i < 1000; i++)
            {
                Start();
                Stop();
                _calibrationTime += _stopTime - _startTime;
            }

            _calibrationTime /= 1000;
        }

        public void Reset()
        {
            _startTime = 0;
            _stopTime = 0;
        }

        public void Start()
        {
            QueryPerformanceCounter(ref _startTime);
        }

        public void Stop()
        {
            QueryPerformanceCounter(ref _stopTime);
        }

        public TimeSpan GetElapsedTimeSpan()
        {
            return TimeSpan.FromMilliseconds(_GetElapsedTime_ms());
        }

        public TimeSpan GetSplitTimeSpan()
        {
            return TimeSpan.FromMilliseconds(_GetSplitTime_ms());
        }

        public double GetElapsedTimeInMicroseconds()
        {
            return (((_stopTime - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
        }

        public double GetSplitTimeInMicroseconds()
        {
            long currentCount = 0;
            QueryPerformanceCounter(ref currentCount);
            return (((currentCount - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
        }

        private double _GetSplitTime_ms()
        {
            long currentCount = 0;
            QueryPerformanceCounter(ref currentCount);
            return (((currentCount - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency) / 1000.0);
        }

        private double _GetElapsedTime_ms()
        {
            return (((_stopTime - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency) / 1000.0);
        }

    }
}
