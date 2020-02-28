using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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

        public static IEnumerable<string> GetDirectoryFiles(string rootPath, string patternMatch, SearchOption searchOption)
        {
            var foundFiles = Enumerable.Empty<string>();

            if (searchOption == SearchOption.AllDirectories)
            {
                try
                {
                    var subDirs = Directory.EnumerateDirectories(rootPath);
                    foreach (string dir in subDirs)
                    {
                        foundFiles = foundFiles.Concat(GetDirectoryFiles(dir, patternMatch, searchOption)); // Add files in subdirectories recursively to the list
                    }
                }
                catch (UnauthorizedAccessException)
                {
                }
                catch (PathTooLongException)
                {
                }
            }

            try
            {
                foundFiles = foundFiles.Concat(Directory.EnumerateFiles(rootPath, patternMatch)); // Add files from the current directory
            }
            catch (UnauthorizedAccessException)
            {
            }

            return foundFiles;
        }
    }
}