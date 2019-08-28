using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using HardLinkBackup;
using Renci.SshNet;

namespace Backuper
{
    public static class Program
    {
        private static readonly string[] SshParams = {"-sl:", "-sp:", "-sr:", "-sh:"};

        private static int? _previousCategory;
        private static SshClient _client;

        private static bool TryGetParameter(string[] args, string name, out string value)
        {
            value = null;
            value = args.FirstOrDefault(x => x.StartsWith(name))?.Replace(name, null);
            return !string.IsNullOrEmpty(value);
        }

        private static string GetParameter(string[] args, string name)
        {
            if (!TryGetParameter(args, name, out var value))
                throw new Exception("Failed to get arg " + name);

            return value;
        }

        // -s:<source>
        // -t:<target>
        // -sl:<ssh login>
        // -sp:<ssh password>
        // -sr:<ssh root dir>
        // -sh:<ssh host>
        static void Main(string[] args)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;

            if (args == null || args.Length == 0)
            {
                Console.WriteLine("Wrong args");
                return;
            }

            if (!TryGetParameter(args, "-s:", out var source))
            {
                Console.WriteLine("Source folder is not specified");
                return;
            }

            if (!Directory.Exists(source))
            {
                Console.WriteLine("Source folder does not exist");
                return;
            }

            if (!TryGetParameter(args, "-t:", out var target))
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
            var sshParams = args.Where(x => SshParams.Any(x.StartsWith)).ToList();
            if (sshParams.Count == 0)
            {
                helper = new WinHardLinkHelper();
            }
            else if (sshParams.Count == 4)
            {
                var sshLogin = GetParameter(args, "-sl");
                var sshPwd = GetParameter(args, "-sp");
                var ci = new ConnectionInfo(GetParameter(args, "-sh"), sshLogin, new PasswordAuthenticationMethod(sshLogin, sshPwd));
                _client = new SshClient(ci);
                _client.Connect();
                helper = new NetShareSshHardLinkHelper(target, GetParameter(args, "-sr"), _client);
            }
            else
            {
                Console.WriteLine("Wrong ssh args");
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