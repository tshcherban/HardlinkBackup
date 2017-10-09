using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace HardLinkBackup
{
    public static class HardLinkHelper
    {
        public static string[] GetHardLinksRooted(string fileName)
        {
            var links = GetHardLinks(fileName);
            var root = Path.GetPathRoot(fileName);
            return links.Select(l => $"{root.Replace("\\", null)}{l}").ToArray();
        }

        public static string[] GetHardLinks(string fileName)
        {
            var proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "fsutil",
                    Arguments = $"hardlink list \"{fileName }\"",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                }
            };
            proc.Start();
            var str = proc.StandardOutput.ReadToEnd();
            proc.WaitForExit();

            return str.Split(new[] {Environment.NewLine}, StringSplitOptions.RemoveEmptyEntries);
        }

        [DllImport("Kernel32.dll", CharSet = CharSet.Unicode)]
        public static extern bool CreateHardLink(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);
    }
}