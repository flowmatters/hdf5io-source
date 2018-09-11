using System;
using System.Linq;
using FlowMatters.H5SS;
using NUnit.Framework;
using TIME.Core;
using TIME.DataTypes;

namespace FlowMatters.Source.HDF5IO.Tests
{
    [TestFixture]
    class TestLazyLoad
    {

        [Test]
        public void LazyLoad()
        {
            var start = new DateTime(1990, 1, 1);
            double[] vals = Enumerable.Range(0, 1000).Select(i => (double)i).ToArray();
            TimeSeries ts = new TimeSeries(start, TimeStep.Daily, vals);
            ts.name = "My Time Series";
            ts.units = Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond);

            vals = Enumerable.Range(1000, 1000).Select(i => (double)i).ToArray();
            TimeSeries ts2 = new TimeSeries(start, TimeStep.Daily, vals);
            ts2.name = "Mine / Someone's Time Series";
            ts2.units = Unit.PredefinedUnit(CommonUnits.squareMetres);

            var writer = new HDF5TimeSeriesIO();
            writer.DataType = HDF5DataType.Double;

            writer.Use(ts);
            writer.Use(ts2);

            string fn = "TestTimeSeriesIO_LazyLoad.h5";
            writer.Save(fn);

            var reader = new HDF5TimeSeriesIO(true);
            reader.Load(fn);

            TimeSeries retrieved = (TimeSeries)reader.DataSets.First(t => t.name == "My Time Series");
            Assert.IsInstanceOf<HDF5TimeSeriesState>(retrieved.state);

            HDF5TimeSeriesState state = (HDF5TimeSeriesState) retrieved.state;
            Assert.AreEqual(1000, retrieved.Count);
            Assert.AreEqual(1000, state.Count);

            Assert.IsFalse(state.Loaded);
            Assert.AreEqual(ts.name, retrieved.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond), retrieved.units);
            bool compatibleWith = ts.IsCompatibleWith(retrieved);
            if (!compatibleWith)
            {
                Assert.Fail(ts.GetCompatibilityErrorMessage());
            }
            Assert.IsTrue(compatibleWith);
            Assert.IsTrue(ts.EqualData(retrieved));
            Assert.IsTrue(state.Loaded);

            TimeSeries retrieved2 = (TimeSeries)reader.DataSets.First(t => t.name == "Mine / Someone's Time Series");
            Assert.IsInstanceOf<HDF5TimeSeriesState>(retrieved2.state);

            HDF5TimeSeriesState state2 = (HDF5TimeSeriesState)retrieved2.state;
            Assert.IsFalse(state2.Loaded);
            Assert.AreEqual(ts2.name, retrieved2.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.squareMetres), retrieved2.units);
            Assert.IsTrue(ts2.IsCompatibleWith(retrieved2));
            Assert.IsTrue(ts2.EqualData(retrieved2));
            Assert.IsTrue(state2.Loaded);

        }
    }
}
