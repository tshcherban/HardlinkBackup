using System;
using System.Diagnostics;
using System.Threading.Tasks;
using HardLinkBackup;

namespace Backuper
{
    class Program
    {
        const string source = @"F:\src";
        const string destination = @"C:\shcherban\stest\dst1";

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

        private static void BackupHardLinks()
        {
            Console.CursorVisible = false;
            var sw = Stopwatch.StartNew();

            try
            {
                var engine = new HardLinkBackupEngine(source, destination, false);
                engine.Log += WriteLog;
                engine.LogExt += WriteLogExt;

                var stop = false;

                Task.Run(() =>
                {
                    while (!stop)
                    {
                        Task.Delay(500).Wait();

                        lock (SyncRoot)
                        {
                            Console.CursorLeft = 0;
                            Console.Write("".PadRight(Console.BufferWidth - 1));
                            Console.CursorLeft = 0;

                            if (Cat.HasValue && Log != null)
                            {
                                if (Cat == _previousCategory)
                                    Console.Write(Log);
                                else
                                    Console.WriteLine(Log);

                                _previousCategory = Cat.Value;

                                Cat = null;
                                Log = null;
                            }

                            if (LogExt != null)
                            {
                                var left = Console.CursorLeft;

                                Console.Write("".PadRight(Console.BufferWidth - 1 - left));
                                Console.CursorLeft = left;

                                Console.Write(LogExt);

                                Console.CursorLeft = left;

                                LogExt = null;
                            }
                        }
                    }
                });

                engine.DoBackup().Wait();

                sw.Stop();

                stop = true;

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

        private static string LogExt;
        private static string Log;
        private static int? Cat;

        private static void WriteLogExt(string msg)
        {
            lock (SyncRoot)
            {
                LogExt = msg;
            }
            return;
            var left = Console.CursorLeft;

            Console.Write("".PadRight(Console.BufferWidth - 1 - left));
            Console.CursorLeft = left;

            Console.Write(msg);

            Console.CursorLeft = left;
        }

        private static int? _previousCategory;

        private static object SyncRoot = new object();

        private static void WriteLog(string msg, int category)
        {
            lock (SyncRoot)
            {
                Log = msg;
                Cat = category;
                LogExt = null;
            }
            
            return;
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