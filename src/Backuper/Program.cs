using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using HardLinkBackup;

namespace Backuper
{
    class Program
    {
        static void Main(string[] args)
        {
            const string source = @"C:\0.125gb";
            const string target = @"D:\test\test";
            if (File.Exists(target))
                File.Delete(target);

            var sw = new Stopwatch();
            sw.Start();

            /*Debug.WriteLine($"Program {Thread.CurrentThread.ManagedThreadId}");

            var sha = HashSumHelper.CopyUnbufferedAndComputeHashAsync(source, target, d => {}, true).Result;
            
            var sha1 = string.Concat(sha.Select(i => i.ToString("x")));

            sw.Stop();
            Console.WriteLine($"{sha1} (done in {sw.Elapsed.TotalMilliseconds:F2} ms)");
            Console.ReadKey();
            return;*/
            try
            {
                var engine = new BackupEngine(@"F:\Src", @"F:\Dst", true);
                engine.Log += WriteLog;
                Task.Run(async () => await engine.DoBackup()).Wait();
                sw.Stop();
                Console.WriteLine($"Done in {sw.Elapsed.TotalMilliseconds:F2} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            Console.ReadKey();
        }

        private static int? _previousCategory;

        private static void WriteLog(string msg, int category)
        {
            Console.CursorLeft = 0;
            Console.Write("".PadRight(Console.BufferWidth - 1));
            Console.CursorLeft = 0;

            if (category == _previousCategory)
                Console.Write(msg);
            else
                Console.WriteLine(msg);

            _previousCategory = category;
        }
    }
}