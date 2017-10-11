using System;
using System.IO;
using System.Linq;
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

            var f = File.ReadAllLines(@"F:\data.txt")
                .Select(line => line.Split(new[] {"\t"}, StringSplitOptions.RemoveEmptyEntries))
                .Select(i => new {e = i[1], v = double.Parse(i[0]), n=int.Parse(i[2])})
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
            var lwy = cwwy + h;
            var lry = lwy + h;

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

                    cItem = new RectangleBarItem(i.v, cy, 0, cy + h) {Color = cColor, Title = i.n.ToString()};
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

                    wItem = new RectangleBarItem(i.v, wy, 0, wy + h) {Color = wColor, Title = i.n.ToString() };
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

            plotModel.ResetAllAxes();

            Plot1.Model = plotModel;

            Left = 0;
        }
    }
}