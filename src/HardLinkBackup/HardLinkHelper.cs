using System;
using System.Diagnostics;

namespace HardLinkBackup
{
    public static class HardLinkHelper
    {
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
    }
}