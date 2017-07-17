using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using TIME.Core;
using TIME.DataTypes;

namespace FlowMatters.Source.HDF5IO.Tests
{
    [TestFixture]
    public class TestHDF5TimeSeriesIO
    {
        [Test]
        public void SaveAndLoadDaily()
        {
            double[] vals = Enumerable.Range(0, 1000).Select(i => (double) i).ToArray();
            TimeSeries ts = new TimeSeries(new DateTime(1990, 1, 1), TimeStep.Daily, vals);
            ts.name = "My Time Series";
            ts.units = Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond);

            vals = Enumerable.Range(1000, 1000).Select(i => (double) i).ToArray();
            TimeSeries ts2 = new TimeSeries(new DateTime(1990, 1, 1), TimeStep.Daily, vals);
            ts2.name = "Mine / Someone's Time Series";
            ts2.units = Unit.PredefinedUnit(CommonUnits.squareMetres);

            var writer = new HDF5TimeSeriesIO();
            writer.Use(ts);
            writer.Use(ts2);

            var fn = "_TestTimeSeriesIO.h5";
            writer.Save(fn);

            var reader = new HDF5TimeSeriesIO();
            reader.Load(fn);

            TimeSeries retrieved = (TimeSeries) reader.DataSets.First(t=>t.name=="My Time Series");
            Assert.IsTrue(ts.EqualData(retrieved));
            Assert.AreEqual(ts.name, retrieved.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond), retrieved.units);

            TimeSeries retrieved2 = (TimeSeries) reader.DataSets.First(t => t.name == "Mine / Someone's Time Series");
            Assert.IsTrue(ts2.EqualData(retrieved2));
            Assert.AreEqual(ts2.name, retrieved2.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.squareMetres), retrieved2.units);
        }
    }
}
