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
        public static byte[] ComputeSha1Unbuffered(string filePath)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions)0x20000000;
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
            
                using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fileOptions))
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

        public static async Task<byte[]> CopyUnbufferedAndComputeHash(string filePath, string destinationPath, bool allowReadWrite = false)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions)0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

            const int chunkSize = 256 * 1024 * 1024;

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

                    const int count = 2;
                    var buffer = new LightBuffer[count];
                    for (var i = 0; i < count; ++i)
                        buffer[i] = new LightBuffer(readSize);

                    void Increment(ref int idx)
                    {
                        idx++;
                        if (idx > count - 1)
                            idx = 0;
                    }

                    var writeEvent = new ManualResetEvent(true);
                    var readEvent = new ManualResetEvent(true);

                    var cnt = 0;

                    var readTask = Task.Run(() =>
                    {
                        var readCount = 0;
                        var readIdx = 0;
                        while (toRead > 0)
                        {
                            var lightBuffer = buffer[readIdx];
                            lightBuffer.WriteDone.WaitOne();
                            lightBuffer.WriteDone.Reset();

                            Console.WriteLine($"R{++readCount} start {++cnt}");

                            if (!allowReadWrite)
                            {
                                writeEvent.WaitOne();
                                writeEvent.Reset();
                            }

                            lightBuffer.Length = sourceStream.Read(lightBuffer.Data, 0, readSize);
                            readEvent.Set();
                            if (lightBuffer.Length == 0)
                                throw null;

                            toRead -= lightBuffer.Length;

                            Increment(ref readIdx);

                            lightBuffer.IsFinal = toRead == 0;

                            Console.WriteLine($"R{readCount} end {++cnt}");

                            lightBuffer.DataReady.Set();
                        }
                    });

                    var writeTask = Task.Run(async () =>
                    {
                        var writeIdx = 0;
                        var run = true;
                        var writeCount = 0;
                        while (run)
                        {
                            var lightBuffer = buffer[writeIdx];

                            lightBuffer.DataReady.WaitOne();
                            lightBuffer.DataReady.Reset();

                            readEvent.WaitOne();
                            readEvent.Reset();

                            Console.WriteLine($"W{++writeCount} start {++cnt}");

                            var wrTask = destinationStream.WriteAsync(lightBuffer.Data, 0, lightBuffer.Length);
                            if (lightBuffer.IsFinal)
                            {
                                sha.TransformFinalBlock(lightBuffer.Data, 0, lightBuffer.Length);
                                run = false;
                            }
                            else
                                sha.TransformBlock(lightBuffer.Data, 0, lightBuffer.Length, null, 0);
                            
                            await wrTask;

                            Increment(ref writeIdx);

                            Console.WriteLine($"W{writeCount} end {++cnt}");

                            writeEvent.Set();
                            lightBuffer.WriteDone.Set();
                        }
                    });

                    await Task.WhenAll(readTask, writeTask);

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
        }

        private class Buffer
        {
            public byte[] Data { get; set; }

            public int Length { get; set; }

            public bool IsLast { get; set; }
        }
    }
}