using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using HardLinkBackup;

namespace Backuper
{
    class Program
    {
        private static string _dateFormat;
        private static readonly ManualResetEvent ResetEvent = new ManualResetEvent(false);
        private static bool _run = true;
        private static int _processed;
        private static int _total;

        static void Main(string[] args)
        {
            var sw = new Stopwatch();
            sw.Start();

            var f = @"D:\Mia\Trainings\AQA\Adobe Experience Manager 6 Test automation approach QA Club with Valentyn Kvasov and Dmitry Lazarev.mp4";
            //var f = @"D:\Mia\ITM Program_ Interviewing a candidate (part 1).mp4";
            var target = @"D:\test1";
            if (File.Exists(target))
                File.Delete(target);

            var sha = HashSumHelper.CopyUnbufferedAndComputeHash(f, target, true).Result;
            var sha1 = string.Concat(sha.Select(i => i.ToString("x")));

            sw.Stop();
            Console.WriteLine($"{sha1} (read in {sw.Elapsed.TotalMilliseconds:F2} ms)");
            /*
            
            var file = @"<largefile>";
            var sha = HashSumHelper.ComputeSha256Unbuffered(file);
            var sha1 = string.Concat(sha.Select(i => i.ToString("x")));
            
            */
            //EnumerateLinks();

            //Task.Factory.StartNew(DoBackup, TaskCreationOptions.LongRunning).ContinueWith(t => ResetEvent.Set());

            //ResetEvent.WaitOne();
            //DoBackup();

            Console.ReadKey();
        }

        private static void DoBackup()
        {
            var src = @"F:\0";
            var dst = @"F:\";

            var files = Directory.EnumerateFiles(src, "*", SearchOption.AllDirectories)
                .Select(f => new FileInfoEx(f))
                .ToList();

            _dateFormat = "yyyy-MM-dd-HHmmss";

            var newBkpDate = DateTime.Now;
            var newBkpName = newBkpDate.ToString(_dateFormat, CultureInfo.InvariantCulture);
            var prevBkps = BackupInfo.DiscoverBackups(dst).ToList();
            foreach (var backupInfo in prevBkps)
            {
                backupInfo.CheckIntegrity();
            }

            var currentBkpDir = Path.Combine(dst, newBkpName);
            var currentBkp = new BackupInfo
            {
                AbsolutePath = currentBkpDir,
                DateTime = newBkpDate,
                Objects = new List<BackupFileInfo>(files.Count)
            };

            var prevBackupFiles = prevBkps
                .SelectMany(b => b.Objects.Select(fll => new {file = fll, backup = b}))
                .ToList();
            var tasks = new List<Task>();
            var copiedCount = 0;
            var linkedCount = 0;

            foreach (var f in files)
            {
                var newFile = f.FileName.Replace(src, currentBkpDir);
                var newFileRelativeName = newFile.Replace(currentBkpDir, string.Empty);

                var newDir = Path.GetDirectoryName(newFile);
                if (!Directory.Exists(newDir))
                    Directory.CreateDirectory(newDir);

                var fileFromPrevBackup =
                    prevBackupFiles
                        .FirstOrDefault(oldFile =>
                            oldFile.file.Length == f.FileInfo.Length &&
                            oldFile.file.Hash == f.FastHashStr);

                string existingFile;
                if (fileFromPrevBackup != null)
                    existingFile = fileFromPrevBackup.backup.AbsolutePath + fileFromPrevBackup.file.Path;
                else
                {
                    existingFile = currentBkp.Objects
                        .FirstOrDefault(copied =>
                            copied.Length == f.FileInfo.Length &&
                            copied.Hash == f.FastHashStr)?.Path;
                    if (existingFile != null)
                        existingFile = currentBkp.AbsolutePath + existingFile;
                }

                if (existingFile != null)
                {
                    if (!HardLinkHelper.CreateHardLink(newFile, existingFile, IntPtr.Zero))
                        throw new InvalidOperationException("Hardlink failed");

                    linkedCount++;
                    //Console.WriteLine($"Hardlinked\r\n{newFile}\r\nto\r\n{existingFile}");
                }
                else
                {
                    File.Copy(f.FileName, newFile);
                    copiedCount++;
                    tasks.Add(Task.Run(() =>
                    {
                        CreatePar(newFile, currentBkp);
                    }));

                    //Console.WriteLine($"Copied\r\n{newFile}\r\nfrom\r\n{f.FileName}");
                }

                var fi = new FileInfoEx(newFile);
                fi.FileInfo.Attributes |= FileAttributes.ReadOnly;
                currentBkp.Objects.Add(new BackupFileInfo
                {
                    Path = newFileRelativeName,
                    Hash = fi.FastHashStr,
                    Length = fi.FileInfo.Length
                });
            }

            currentBkp.WriteToDisk();
            Console.WriteLine($"Backup done. {copiedCount} files copied, {linkedCount} files linked");
            if (tasks.Count > 0)
            {
                Console.WriteLine("Waiting par...");
                Task.WaitAll(tasks.ToArray());
                Console.WriteLine("Par completed");
            }
        }

        private static void CreatePar(string file, BackupInfo currentBkp)
        {
            var parFile = currentBkp.AbsolutePath + $"\\.bkp\\par{file.Replace(currentBkp.AbsolutePath, null)}.par";
            var proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "par2j64",
                    Arguments = $"c /rr10 /rf1 \"{parFile}\" \"{file}\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                }
            };
            proc.Start();
            proc.WaitForExit();
            var outp = proc.StandardOutput.ReadToEnd();
        }

        private static void UpdateConsole(int l, int t)
        {
            _run = true;
            while (_run)
            {
                Console.SetCursorPosition(l, t);

                Console.Write($"Processed {_processed} of {_total}");
                Thread.Sleep(500);
            }
        }



        private static string text;

        
    }
}