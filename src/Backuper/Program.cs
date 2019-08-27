using System;
using System.Diagnostics;
using System.IO;
using HardLinkBackup;
using Renci.SshNet;

namespace Backuper
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;

            if (args == null || args.Length == 0)
            {
                Console.WriteLine("Wrong args");
                return;
            }

            Debugger.Launch();

            var source = args[0];
            if (string.IsNullOrEmpty(source))
            {
                Console.WriteLine("Source folder is not specified");
                return;
            }

            if (!Directory.Exists(source))
            {
                Console.WriteLine("Source folder does not exist");
                return;
            }

            var target = args[1];
            if (string.IsNullOrEmpty(target))
            {
                Console.WriteLine("Target folder is not specified");
                return;
            }

            if (!Directory.Exists(target))
            {
                Console.WriteLine("Target folder does not exist");
                return;
            }

            IHardLinkHelper helper;
            if (args.Length == 2)
                helper = new WinHardLinkHelper();
            else if (args.Length == 6)
            {
                var ci = new ConnectionInfo(args[3], args[4], new PasswordAuthenticationMethod(args[4], args[5]));
                _client = new SshClient(ci);
                _client.Connect();
                helper = new NetShareSshHardLinkHelper(target, args[2], _client);
            }
            else
            {
                Console.WriteLine("Wrong args");
                return;
            }

            BackupHardLinks(source, target, helper);

            _client?.Dispose();

            Console.WriteLine("Done. Press return to exit");

            Console.ReadLine();
        }

        private static void BackupHardLinks(string source, string target, IHardLinkHelper helper)
        {
            Console.CursorVisible = false;
            var sw = Stopwatch.StartNew();

            try
            {
                var engine = new HardLinkBackupEngine(source, target, true, helper);
                engine.Log += WriteLog;
                engine.LogExt += WriteLogExt;

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
        private static SshClient _client;


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