using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Alphaleonis.Win32.Vss;
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
            if (args?.FirstOrDefault() == "VSS")
            {
                var sourceFile = @"C:\file";
                var destination = @"C:\file";

                var impl = VssUtils.LoadImplementation();
                var backup = impl.CreateVssBackupComponents();
                backup.InitializeForBackup(null);
                backup.GatherWriterMetadata();

                backup.SetContext(VssVolumeSnapshotAttributes.Persistent | VssVolumeSnapshotAttributes.NoAutoRelease);
                backup.SetBackupState(false, true, VssBackupType.Full, false);
                var snapshotSetId = backup.StartSnapshotSet();

                var volume = new FileInfo(sourceFile).Directory.Root.Name;

                var shadowCopyId = backup.AddToSnapshotSet(volume, Guid.Empty);

                backup.PrepareForBackup();

                backup.DoSnapshotSet();

/***********************************
/* At this point we have a snapshot!
/* This action should not take more then 60 second, regardless of file or disk size.
/* THe snapshot is not a backup or any copy!
/* please more information at http://technet.microsoft.com/en-us/library/ee923636.aspx
/***********************************/

// VSS step 7: Expose Snapshot
/***********************************
/* Snapshot path look like:
 * \\?\Volume{011682bf-23d7-11e2-93e7-806e6f6e6963}\
 * The build in method System.IO.File.Copy do not work with path like this,
 * Therefor, we are going to Expose the Snapshot to our application,
 * by mapping the Snapshot to new virtual volume
 * - Make sure that you are using a volume that is not already exist
 * - This is only for learning purposes. usually we will use the snapshot directly as i show in the next example in the blog
/***********************************/
                //backup.ExposeSnapshot(shadowCopyId, null, VssVolumeSnapshotAttributes.ExposedLocally, "L:");

                var root = backup.QuerySnapshots().First(x => x.SnapshotSetId == snapshotSetId && x.SnapshotId == shadowCopyId).OriginalVolumeName;

// VSS step 8: Copy Files!
/***********************************
/* Now we start to copy the files/folders/disk!
/* Execution time can depend on what we are copying
/* We can copy one element or several element.
/* As long as we are working under the same snapshot,
/* the element should be in consist state from the same point-in-time
/***********************************/
                
                var vssSource = sourceFile.Replace(volume, root);

                if (File.Exists(vssSource))
                    File.Copy(vssSource, destination + @"\" + Path.GetFileName(sourceFile) + "_copy", true);

// VSS step 9: Delete the snapshot
                backup.DeleteSnapshot(shadowCopyId, true);

                backup.Dispose();
                return;
            }

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
                var sshLogin = GetParameter(args, "-sl:");
                var sshPwd = GetParameter(args, "-sp:");
                var ci = new ConnectionInfo(GetParameter(args, "-sh:"), sshLogin, new PasswordAuthenticationMethod(sshLogin, sshPwd));
                _client = new SshClient(ci);
                _client.Connect();
                helper = new NetShareSshHardLinkHelper(target, GetParameter(args, "-sr:"), _client);
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