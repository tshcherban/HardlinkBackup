using System.IO;
using Newtonsoft.Json;

namespace HardLinkBackup
{
    public static class JsonHelpers
    {
        public static T ReadFromFile<T>(string fileName)
        {
            using (var stream = File.OpenText(fileName))
            {
                using (var jsonTextReader = new JsonTextReader(stream))
                {
                    var s = new JsonSerializer();
                    return s.Deserialize<T>(jsonTextReader);
                }
            }
        }

        public static void WriteToFile<T>(string fileName, T value)
        {
            using (var wr = File.CreateText(fileName))
            {
                var serializer = new JsonSerializer
                {
                    Formatting = Formatting.Indented
                };
                serializer.Serialize(wr, value);
            }
        }
    }
}