using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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

        public static async Task<byte[]> CopyUnbufferedAndComputeHash(string filePath, string destinationPath)
        {
            const FileOptions fileFlagNoBuffering = (FileOptions)0x20000000;
            const FileOptions fileOptions = fileFlagNoBuffering | FileOptions.SequentialScan;

            const int chunkSize = 32 * 1024 * 1024;

            var readBufferSize = chunkSize;
            readBufferSize += ((readBufferSize + 1023) & ~1023) - readBufferSize;

            var buffers = new Queue<Buffer>();
            const int maxCount = 1;

            using (HashAlgorithm sha = SHA1.Create())
            {
                var buffer = new Buffer {Completed = true};
                
                using (var sourceFileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, readBufferSize, fileOptions))
                using (var destinationFileStream = new FileStream(destinationPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, readBufferSize, fileOptions|FileOptions.WriteThrough))
                {
                    var length = sourceFileStream.Length;
                    var toRead = length;

                    var readSize = Convert.ToInt32(Math.Min(chunkSize, length));
                    buffer.Data = new byte[readSize];

                    var task = buffer.Process(destinationFileStream, sha).ContinueWith(t =>
                    {
                        buffer.Completed = false;
                    });

                    task = Task.CompletedTask;
                    Buffer active = null;
                    while (toRead > 0)
                    {
                        if (active == null)
                        {
                            active = buffers.Dequeue();
                            if (active != null)
                            {
                                task = active.Process(destinationFileStream, sha);
                            }
                        }

                        if (buffer.Length >= maxCount)
                        {
                            await task;
                        }



                        buffer.Length = sourceFileStream.Read(buffer.Data, 0, readSize);
                        if (buffer.Length == 0)
                            throw new EndOfStreamException("Read beyond end of file EOF");

                        toRead -= buffer.Length;

                        buffer.IsLast = toRead == 0;

                        task = buffer.Process(destinationFileStream, sha);
                    }

                    return sha.Hash;
                }
            }
        }

        private class Buffer
        {
            public byte[] Data { get; set; }

            public int Length { get; set; }

            public bool IsLast { get; set; }

            public bool Completed { get; set; }

            public async Task Process(Stream destination, ICryptoTransform algorithm)
            {
                if (Completed)
                    return;

                var writeTask = destination.WriteAsync(Data, 0, Length);
                var computeTask = Task.Factory.StartNew(() =>
                {
                    if (IsLast)
                        algorithm.TransformFinalBlock(Data, 0, Length);
                    else
                        algorithm.TransformBlock(Data, 0, Length, Data, 0);
                });

                await Task.WhenAll(writeTask, computeTask);
            }
        }
    }
}