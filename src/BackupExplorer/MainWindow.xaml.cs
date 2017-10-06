using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using HardLinkBackup;

namespace BackupExplorer
{
    public partial class MainWindow
    {
        private readonly List<Item> _items;

        public MainWindow()
        {
            InitializeComponent();

            var itemProvider = new ItemProvider();

            _items = itemProvider.GetItems(@"F:\");

            DataContext = _items;

            Task.Run(() =>
            {
                foreach (var i in _items.OfType<DirectoryItem>())
                    DiscoverBackups(i);
            });
        }

        private static void DiscoverBackups(DirectoryItem dirItem)
        {
            try
            {
                var bckps = BackupInfo.DiscoverBackups(dirItem.Path).ToList();
                if (bckps.Count > 0)
                {
                    dirItem.HasBackups = true;
                    foreach (var bkp in bckps)
                    {
                        dirItem.Items.OfType<DirectoryItem>().First(i => i.Path == bkp.AbsolutePath).IsBackup = true;
                    }
                }
                else
                {
                    foreach (var i in dirItem.Items.OfType<DirectoryItem>())
                        DiscoverBackups(i);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private void Control_OnMouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!(DataGrid1.SelectedItem is FileItem fi))
                return;

            var links = HardLinkHelper.GetHardLinks(fi.Path);
        }
    }

    public class DirectoryItem : Item
    {
        public List<Item> Items { get; set; }

        public bool HasBackups { get; set; }

        public bool IsBackup { get; set; }

        public DirectoryItem()
        {
            Items = new List<Item>();
        }
    }

    public class FileItem : Item
    {
    }

    public class Item
    {
        public string Name { get; set; }
        public string Path { get; set; }
    }

    public class ItemProvider
    {
        public List<Item> GetItems(string path)
        {
            var items = new List<Item>();

            var dirInfo = new DirectoryInfo(path);
            try
            {
                foreach (var directory in dirInfo.GetDirectories())
                {
                    var item = new DirectoryItem
                    {
                        Name = directory.Name,
                        Path = directory.FullName,
                        Items = GetItems(directory.FullName)
                    };

                    items.Add(item);
                }

                foreach (var file in dirInfo.GetFiles())
                {
                    var item = new FileItem
                    {
                        Name = file.Name,
                        Path = file.FullName
                    };

                    items.Add(item);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            return items;
        }
    }
}