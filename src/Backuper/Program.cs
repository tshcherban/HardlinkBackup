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
            const string source = @"E:\Work\BackupTest\test_1g";
            const string target = @"F:\test_1g_dest";
            if (File.Exists(target))
                File.Delete(target);

            var sw = new Stopwatch();
            sw.Start();
            /*
            var sha = HashSumHelper.CopyUnbufferedAndComputeHashAsync(source, target, OnProgress, true).Result;
            var sha1 = string.Concat(sha.Select(i => i.ToString("x")));

            sw.Stop();
            Console.WriteLine($"{sha1} (read in {sw.Elapsed.TotalMilliseconds:F2} ms)");*/

            try
            {
                var engine = new BackupEngine(@"D:\Photo", @"H:\Backups\Photos", true);
                engine.Log += WriteLog;
                engine.DoBackup();
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