using System;
using System.Diagnostics;
using System.Threading.Tasks;
using HardLinkBackup;

namespace Backuper
{
    class Program
    {
        const string source = @"D:\Photo";
        const string destination = @"H:\Hardlink backups\Photo";

        static void Main(string[] args)
        {
            BackupHardLinks();

            //BackupCustom();

            Console.WriteLine("Done. Press return to exit");

            Console.ReadLine();
        }

        private static void BackupCustom()
        {
            var e = new BackupEngine(source, destination);

            e.DoBackup().Wait();
        }

        private static Stopwatch Stopw;

        private static void BackupHardLinks()
        {
            Console.CursorVisible = false;
            var sw = Stopwatch.StartNew();

            try
            {
                var engine = new HardLinkBackupEngine(source, destination, true);
                engine.Log += WriteLog;
                engine.LogExt += WriteLogExt;

                Stopw = Stopwatch.StartNew();

                engine.DoBackup().Wait();

                sw.Stop();

                Console.WriteLine($"Done in {sw.Elapsed.TotalMilliseconds:F2} ms");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                sw.Stop();
                Console.CursorVisible = true;
            }
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

    /*if (args.Length != 2)
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
                sw.Stop();
                Console.CursorVisible = true;
            }*/
}