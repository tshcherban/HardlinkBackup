using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;

namespace HardLinkBackup
{
    public static class XxHash64Callback
    {
        public static readonly byte[] EmptyHash = new byte[sizeof(ulong)];

        private const int Min64 = 1024;
        private const int Div32 = 0x7FFFFFE0;

        private const ulong P1 = 11400714785074694791UL;
        private const ulong P2 = 14029467366897019727UL;
        private const ulong P3 = 1609587929392839161UL;
        private const ulong P4 = 9650029242287828579UL;
        private const ulong P5 = 2870177450012600261UL;

        public static async Task<byte[]> ComputeHash(Stream stream, int bufferSize, long length, Func<byte[], int, Task> callback)
        {
            // The buffer can't be less than 1024 bytes
            if (bufferSize < Min64)
            {
                bufferSize = Min64;
            }
            else
            {
                bufferSize &= Div32;
            }

            // Calculate the number of chunks and the remain
            var chunks = length / bufferSize;
            var remain = length % bufferSize;
            var offset = bufferSize;

            // Calculate the offset
            if (remain != 0) chunks++;
            if (remain != 0 && remain < 32) offset -= 32;

            var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

            try
            {
                return await HashCore(stream, bufferSize, chunks, offset, buffer, length, callback);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private static void ReadExact(Stream stream, byte[] buffer, int count)
        {
            var left = count;
            var read = 0;
            while (left > 0)
            {
                read += stream.Read(buffer, read, left);
                left = count - read;
            }
        }

        private static async Task<byte[]> HashCore(Stream stream, int bufferSize, long chunks, int offset, byte[] buffer, long length, Func<byte[], int, Task> callback)
        {
            // Prepare the seed vector
            var v1 = unchecked(P1 + P2);
            var v2 = P2;
            var v3 = 0ul;
            var v4 = P1;

            Task callbackTask;

            long read = 0;

            // Process chunks
            // Skip the last chunk. It will processed a little bit later
            for (var i = 2L; i <= chunks; i++)
            {
                // Change bufferSize for the last read
                if (i == chunks)
                {
                    bufferSize = offset;
                }

                // Read the next chunk
                ReadExact(stream, buffer, bufferSize);

                read += bufferSize;

                callbackTask = callback(buffer, bufferSize);

                unsafe
                {
                    fixed (byte* pData = &buffer[0])
                    {
                        var ptr = pData;
                        var end = pData + bufferSize;

                        do
                        {
                            v1 += *((ulong*) ptr) * P2;
                            v1 = (v1 << 31) | (v1 >> (64 - 31)); // rotl 31
                            v1 *= P1;
                            ptr += 8;

                            v2 += *((ulong*) ptr) * P2;
                            v2 = (v2 << 31) | (v2 >> (64 - 31)); // rotl 31
                            v2 *= P1;
                            ptr += 8;

                            v3 += *((ulong*) ptr) * P2;
                            v3 = (v3 << 31) | (v3 >> (64 - 31)); // rotl 31
                            v3 *= P1;
                            ptr += 8;

                            v4 += *((ulong*) ptr) * P2;
                            v4 = (v4 << 31) | (v4 >> (64 - 31)); // rotl 31
                            v4 *= P1;
                            ptr += 8;
                        } while (ptr < end);
                    }
                }

                await callbackTask;
            }

            // Read the last chunk
            //offset = stream.Read(buffer, 0, bufferSize);

            var toRead = length - read;
            var toReadInt = (int) toRead;

            ReadExact(stream, buffer, toReadInt);

            callbackTask = callback(buffer, toReadInt);

            ulong h64;

            unsafe
            {
                // Process the last chunk
                fixed (byte* pData = &buffer[0])
                {
                    var ptr = pData;
                    var end = pData + toReadInt;

                    if (length >= 32)
                    {
                        var limit = end - 32;

                        do
                        {
                            v1 += *((ulong*) ptr) * P2;
                            v1 = (v1 << 31) | (v1 >> (64 - 31)); // rotl 31
                            v1 *= P1;
                            ptr += 8;

                            v2 += *((ulong*) ptr) * P2;
                            v2 = (v2 << 31) | (v2 >> (64 - 31)); // rotl 31
                            v2 *= P1;
                            ptr += 8;

                            v3 += *((ulong*) ptr) * P2;
                            v3 = (v3 << 31) | (v3 >> (64 - 31)); // rotl 31
                            v3 *= P1;
                            ptr += 8;

                            v4 += *((ulong*) ptr) * P2;
                            v4 = (v4 << 31) | (v4 >> (64 - 31)); // rotl 31
                            v4 *= P1;
                            ptr += 8;
                        } while (ptr <= limit);

                        h64 = ((v1 << 1) | (v1 >> (64 - 1))) + // rotl 1
                              ((v2 << 7) | (v2 >> (64 - 7))) + // rotl 7
                              ((v3 << 12) | (v3 >> (64 - 12))) + // rotl 12
                              ((v4 << 18) | (v4 >> (64 - 18))); // rotl 18

                        // merge round
                        v1 *= P2;
                        v1 = (v1 << 31) | (v1 >> (64 - 31)); // rotl 31
                        v1 *= P1;
                        h64 ^= v1;
                        h64 = h64 * P1 + P4;

                        // merge round
                        v2 *= P2;
                        v2 = (v2 << 31) | (v2 >> (64 - 31)); // rotl 31
                        v2 *= P1;
                        h64 ^= v2;
                        h64 = h64 * P1 + P4;

                        // merge round
                        v3 *= P2;
                        v3 = (v3 << 31) | (v3 >> (64 - 31)); // rotl 31
                        v3 *= P1;
                        h64 ^= v3;
                        h64 = h64 * P1 + P4;

                        // merge round
                        v4 *= P2;
                        v4 = (v4 << 31) | (v4 >> (64 - 31)); // rotl 31
                        v4 *= P1;
                        h64 ^= v4;
                        h64 = h64 * P1 + P4;
                    }
                    else
                    {
                        h64 = P5;
                    }

                    h64 += (ulong) length;

                    // finalize
                    while (ptr <= end - 8)
                    {
                        var t1 = *((ulong*) ptr) * P2;
                        t1 = (t1 << 31) | (t1 >> (64 - 31)); // rotl 31
                        t1 *= P1;
                        h64 ^= t1;
                        h64 = ((h64 << 27) | (h64 >> (64 - 27))) * P1 + P4; // (rotl 27) * p1 + p4
                        ptr += 8;
                    }

                    if (ptr <= end - 4)
                    {
                        h64 ^= *((uint*) ptr) * P1;
                        h64 = ((h64 << 23) | (h64 >> (64 - 23))) * P2 + P3; // (rotl 27) * p2 + p3
                        ptr += 4;
                    }

                    while (ptr < end)
                    {
                        h64 ^= *((byte*) ptr) * P5;
                        h64 = ((h64 << 11) | (h64 >> (64 - 11))) * P1; // (rotl 11) * p1
                        ptr += 1;
                    }

                    // avalanche
                    h64 ^= h64 >> 33;
                    h64 *= P2;
                    h64 ^= h64 >> 29;
                    h64 *= P3;
                    h64 ^= h64 >> 32;
                }
            }

            await callbackTask;

            return BitConverter.GetBytes(h64);
        }
    }
}