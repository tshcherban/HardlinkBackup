using System;

namespace HardLinkBackup
{
    public static class PathHelpers
    {
        public static string NormalizePathWin(string path, char separator = '\\')
        {
            return path?.Replace('/', separator).Replace('\\', separator);
        }

        public static string NormalizePathUnix(string path, char separator = '/')
        {
            if (string.IsNullOrEmpty(path))
                return path;

            if ((path.StartsWith(@":\") || path.StartsWith(@":/")) && char.IsLetter(path[0]))
                throw new Exception("Unix path shouldn't start from windows drive name");

            return path.Replace('\\', separator).Replace('/', separator);
        }
    }
}