using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media;
using OxyPlot;
using OxyPlot.Axes;
using OxyPlot.Series;

namespace WpfApp1
{
    public partial class MainWindow
    {
        public MainWindow()
        {
            InitializeComponent();

            //PlotSomeWeird();

            //PlotSomeWeird1();

            Left = 0;

            Loaded += MainWindow_Loaded;
        }

        public static long TestFunction(long seed, int count)
        {
            long result = seed;
            for (int i = 0; i < count; ++i)
            {
                result ^= i ^ seed; // Some useless bit operations
            }
            return result;
        }

        private async Task Foo()
        {
            long seed = Environment.TickCount;  // Prevents the JIT Compiler 
                                                // from optimizing Fkt calls away
            long result = 0;
            int count = 100000000;

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            while (stopwatch.ElapsedMilliseconds < 1200)
            {
                result = TestFunction(seed, count);
            }
            stopwatch.Stop();

            stopwatch.Restart();
            await Task.Delay(140);
            stopwatch.Stop();
            var el = stopwatch.Elapsed.TotalMilliseconds;
            //MessageBox.Show(el.ToString());
        }
        private void MainWindow_Loaded(object sender, System.Windows.RoutedEventArgs e)
        {
            RunJob().ContinueWith(t =>
            {
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    PlotSomeWeird1(t.Result);
                }));
            });
        }

        private class Item
        {
            public double time { get; set; }

            public int? netw { get; set; }

            public int? hash { get; set; }

            public int? file { get; set; }
        }

        private class WorkerBuffer
        {
            public int Id { get; set; }

            public byte[] Data { get; set; }

            public bool IsFinal { get; internal set; }
        }

        private async Task<IReadOnlyCollection<Item>> RunJob()
        {
            const int BuffersCount = 2;
            var producerBuffers = new AsyncQueue<WorkerBuffer>();
            var consumerBuffers = new AsyncQueue<WorkerBuffer>();

            var netwDuration = 20;
            var hashDuration = 22;
            var fileDuration = 24;

            var bfs = Enumerable.Range(1, BuffersCount).Select(_ => new WorkerBuffer()).ToList();
            producerBuffers.EnqueueRange(bfs);

            var sw = Stopwatch.StartNew();

            var items = new ConcurrentBag<Item>();

            items.Add(new Item
            {
                time = 0,
                netw = 0,
                hash = 0,
                file = 0,
            });

            var producerTask = Task.Run(async () =>
            {
                var i = 0;
                while (i < 10)
                {
                    var buffer = await producerBuffers.DequeueAsync();

                    items.Add(new Item
                    {
                        time = sw.Elapsed.TotalMilliseconds,
                        netw = 1,
                    });

                    await Task.Delay(netwDuration);

                    items.Add(new Item
                    {
                        time = sw.Elapsed.TotalMilliseconds,
                        netw = 0,
                    });

                    buffer.Id = i;
                    buffer.IsFinal = i == 9;

                    consumerBuffers.Enqueue(buffer);

                    i++;
                }
            });

            var consumersTask = Task.Run(async () =>
            {
                while (true)
                {
                    var buffer = await consumerBuffers.DequeueAsync();

                    items.Add(new Item
                    {
                        time = sw.Elapsed.TotalMilliseconds,
                        hash = 1,
                        file = 1,
                    });

                    var fileWriteTask = Task.Delay(fileDuration)
                            .ContinueWith(_ => items.Add(new Item
                            {
                                time = sw.Elapsed.TotalMilliseconds,
                                file = 0,
                            }));

                    var hashTask = Task.Delay(hashDuration)
                            .ContinueWith(_ => items.Add(new Item
                            {
                                time = sw.Elapsed.TotalMilliseconds,
                                hash = 0,
                            }));

                    await Task.WhenAll(fileWriteTask, hashTask);

                    if (buffer.IsFinal)
                    {
                        break;
                    }

                    buffer.Id = buffer.Id * -1;
                    producerBuffers.Enqueue(buffer);
                }
            });

            await Task.WhenAll(producerTask, consumersTask);

            items.Add(new Item
            {
                time = sw.Elapsed.TotalMilliseconds,
                netw = 0,
                hash = 0,
                file = 0,
            });

            return items;
        }

        private void PlotSomeWeird1(IReadOnlyCollection<Item> items)
        {
            /*var f = File.ReadAllLines(@"C:\shcherban\weird.txt")
                .Take(1000)
                .Select(line => line.Split(new[] {"\t"}, StringSplitOptions.RemoveEmptyEntries))
                .Select(i => new
                {
                    time = double.Parse(i[0], CultureInfo.InvariantCulture),
                    netw = int.Parse(i[1]),
                    hash = int.Parse(i[2]),
                    file = int.Parse(i[3]),
                })
                .ToList();
                */

            var f = items.OrderBy(x => x.time).ToList();

            var plotModel = new PlotModel();

            var networkSeries = new StairStepSeries();
            var hashSeries = new StairStepSeries();
            var fileSeries = new StairStepSeries();

            foreach (var x in f)
            {
                if (x.netw.HasValue)
                {
                    networkSeries.Points.Add(new DataPoint(x.time, x.netw.Value));
                }

                if (x.hash.HasValue)
                {
                    hashSeries.Points.Add(new DataPoint(x.time, x.hash.Value - 1));
                }

                if (x.file.HasValue)
                {
                    fileSeries.Points.Add(new DataPoint(x.time, x.file.Value - 2));
                }
            }

            plotModel.Series.Add(networkSeries);
            plotModel.Series.Add(hashSeries);
            plotModel.Series.Add(fileSeries);



            var yAxis = new LinearAxis
            {
                Position = AxisPosition.Left
            };
            plotModel.Axes.Add(yAxis);

            plotModel.ResetAllAxes();

            yAxis.AbsoluteMinimum = -3;
            yAxis.Minimum = -3;
            yAxis.AbsoluteMaximum = 2;
            yAxis.Maximum = 2;
            yAxis.IsPanEnabled = false;
            yAxis.IsZoomEnabled = false;

            Plot1.Model = plotModel;
        }

        private void PlotSomeWeird()
        {
            var f = File.ReadAllLines(@"C:\0\log.txt")
                .Select(line => line.Split(new[] { "\t" }, StringSplitOptions.RemoveEmptyEntries))
                .Select(i => new { e = i[1], v = double.Parse(i[0]), n = int.Parse(i[2]) })
                .ToList();

            var plotModel = new PlotModel();
            var yAxis = new LinearAxis
            {
                Position = AxisPosition.Left
            };
            plotModel.Axes.Add(yAxis);
            var xAxis = new LinearAxis
            {
                Position = AxisPosition.Bottom
            };
            plotModel.Axes.Add(xAxis);

            var series = new RectangleBarSeries();
            plotModel.Series.Add(series);

            RectangleBarItem cItem = null;
            RectangleBarItem rItem = null;
            RectangleBarItem wItem = null;
            RectangleBarItem cwwItem = null;
            RectangleBarItem lwItem = null;
            RectangleBarItem lrItem = null;

            //xAxis.Maximum = f.Max(i => i.v);

            var h = 1;
            var cy = 1;
            var wy = cy + h;
            var ry = wy + h;
            var cwwy = ry + h;
            var lwy = wy;
            var lry = ry;

            var cMediaColor = Colors.Yellow;
            var cColor = OxyColor.FromRgb(cMediaColor.R, cMediaColor.G, cMediaColor.B);

            var wMediaColor = Colors.Red;
            var wColor = OxyColor.FromRgb(wMediaColor.R, wMediaColor.G, wMediaColor.B);

            var rMediaColor = Colors.Green;
            var rColor = OxyColor.FromRgb(rMediaColor.R, rMediaColor.G, rMediaColor.B);

            var cwwMediaColor = Colors.DarkSeaGreen;
            var cwwColor = OxyColor.FromRgb(cwwMediaColor.R, cwwMediaColor.G, cwwMediaColor.B);

            var lwMediaColor = Colors.Aquamarine;
            var lwColor = OxyColor.FromRgb(lwMediaColor.R, lwMediaColor.G, lwMediaColor.B);

            var lrMediaColor = Colors.Coral;
            var lrColor = OxyColor.FromRgb(lrMediaColor.R, lrMediaColor.G, lrMediaColor.B);

            foreach (var i in f)
            {
                if (i.e == "CS")
                {
                    if (cItem != null)
                        throw null;

                    cItem = new RectangleBarItem(i.v, cy, 0, cy + h) { Color = cColor, Title = i.n.ToString() };
                }
                else if (i.e == "CE")
                {
                    if (cItem == null)
                        throw null;

                    cItem.X1 = i.v;
                    series.Items.Add(cItem);
                    cItem = null;
                }
                else if (i.e == "WS")
                {
                    if (wItem != null)
                        throw null;

                    wItem = new RectangleBarItem(i.v, wy, 0, wy + h) { Color = wColor, Title = i.n.ToString() };
                }
                else if (i.e == "WE")
                {
                    if (wItem == null)
                        throw null;

                    wItem.X1 = i.v;
                    series.Items.Add(wItem);
                    wItem = null;
                }
                else if (i.e == "RS")
                {
                    if (rItem != null)
                        throw null;

                    rItem = new RectangleBarItem(i.v, ry, 0, ry + h) { Color = rColor, Title = i.n.ToString() };
                }
                else if (i.e == "RE")
                {
                    if (rItem == null)
                        throw null;

                    rItem.X1 = i.v;
                    series.Items.Add(rItem);
                    rItem = null;
                }
                else if (i.e == "CWWS")
                {
                    if (cwwItem != null)
                        throw null;

                    cwwItem = new RectangleBarItem(i.v, cwwy, 0, cwwy + h) { Color = cwwColor, Title = i.n.ToString() };
                }
                else if (i.e == "CWWE")
                {
                    if (cwwItem == null)
                        throw null;

                    cwwItem.X1 = i.v;
                    series.Items.Add(cwwItem);
                    cwwItem = null;
                }
                else if (i.e == "LWS")
                {
                    if (lwItem != null)
                        throw null;

                    lwItem = new RectangleBarItem(i.v, lwy, 0, lwy + h) { Color = lwColor, Title = i.n.ToString() };
                }
                else if (i.e == "LWE")
                {
                    if (lwItem == null)
                        throw null;

                    lwItem.X1 = i.v;
                    series.Items.Add(lwItem);
                    lwItem = null;
                }
                else if (i.e == "LRS")
                {
                    if (lrItem != null)
                        throw null;

                    lrItem = new RectangleBarItem(i.v, lry, 0, lry + h) { Color = lrColor, Title = i.n.ToString() };
                }
                else if (i.e == "LRE")
                {
                    if (lrItem == null)
                        throw null;

                    lrItem.X1 = i.v;
                    series.Items.Add(lrItem);
                    lrItem = null;
                }
            }

            const double significantEventThreshold = 4;
            foreach (var i in series.Items.ToList())
            {
                if (i.X1 - i.X0 < significantEventThreshold)
                    series.Items.Remove(i);
            }

            plotModel.ResetAllAxes();

            Plot1.Model = plotModel;
        }
    }

    public class AsyncQueue<T>
    {
        private readonly SemaphoreSlim _sem;
        private readonly ConcurrentQueue<T> _que;

        public AsyncQueue()
        {
            _sem = new SemaphoreSlim(0);
            _que = new ConcurrentQueue<T>();
        }

        public void Enqueue(T item)
        {
            _que.Enqueue(item);
            _sem.Release();
        }

        public void EnqueueRange(IEnumerable<T> source)
        {
            var n = 0;
            foreach (var item in source)
            {
                _que.Enqueue(item);
                n++;
            }
            _sem.Release(n);
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            for (; ; )
            {
                await _sem.WaitAsync(cancellationToken);

                if (_que.TryDequeue(out T item))
                {
                    return item;
                }
            }
        }
    }
}