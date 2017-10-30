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
            if (args.Length != 2)
            {
                Console.WriteLine("Wrong args. Press any key to exit");
                Console.ReadKey();
                return;
            }

            var source = args[0];
            var destination = args[1];

            if (!Directory.Exists(source))
            {
                Console.WriteLine($"Wrong args. Directory {source} does not exist. Press any key to exit");
                Console.ReadKey();
                return;
            }

            if (!Directory.Exists(destination))
            {
                Console.WriteLine($"Wrong args. Directory {destination} does not exist. Press any key to exit");
                Console.ReadKey();
                return;
            }

            Console.CursorVisible = false;
            var sw = new Stopwatch();
            sw.Start();

            try
            {
                var engine = new BackupEngine(source, destination, true);
                engine.Log += WriteLog;
                engine.LogExt += WriteLogExt;
                Task.Run(async () => await engine.DoBackup()).Wait();
                sw.Stop();
                Console.WriteLine($"Done in {sw.Elapsed.TotalMilliseconds:F2} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                Console.CursorVisible = true;
            }

            Console.ReadKey();
        }

        private static void WriteLogExt(string msg)
        {
            var left = Console.CursorLeft;

            Console.Write("".PadRight(Console.BufferWidth - 1 - left));
            Console.CursorLeft = left;

            Console.Write(msg);

            Console.CursorLeft = left;
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