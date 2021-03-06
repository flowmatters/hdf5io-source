﻿using System;
using System.Linq;
using FlowMatters.H5SS;
using NUnit.Framework;
using TIME.Core;
using TIME.DataTypes;
using TIME.DataTypes.IO.CsvFileIo;

namespace FlowMatters.Source.HDF5IO.Tests
{
    [TestFixture]
    public class TestHDF5TimeSeriesIO
    {
        [Test]
        public void SaveAndLoadDailyDouble()
        {
            SaveAndLoadDaily();
        }

        [Test]
        public void SaveAndLoadDailyFloat()
        {
            SaveAndLoadDaily("float",HDF5DataType.Float);
        }

        [Test]
        public void SaveAndLoadMetadata()
        {
            var start = new DateTime(1990, 1, 1);
            double[] vals = Enumerable.Range(0, 1000).Select(i => (double)i).ToArray();
            TimeSeries ts = new TimeSeries(start, TimeStep.Daily, vals);
            ts.name = "My Time Series";
            ts.units = Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond);

            var metadata = new GenericTimeSeriesMetaData();
            metadata.SetValue(GenericTimeSeriesMetaData.ElementName,"My Element");
            metadata.SetValue(GenericTimeSeriesMetaData.RunName,"My Run Name");

            ts.metadata = metadata;

            var writer = new HDF5TimeSeriesIO();
            writer.Use(ts);
            var fn = "TestTimeSeriesIO_withMetadata.h5";
            writer.Save(fn);

            var reader = new HDF5TimeSeriesIO();
            reader.Load(fn);

            TimeSeries retrieved = (TimeSeries)reader.DataSets.First(t => t.name == "My Time Series");
            Assert.IsTrue(ts.EqualData(retrieved));
            Assert.IsTrue(ts.IsCompatibleWith(retrieved));

            Assert.IsInstanceOf<GenericTimeSeriesMetaData>(retrieved.metadata);
            var retrMetadata = (GenericTimeSeriesMetaData)retrieved.metadata;
            Assert.AreEqual("My Element",metadata.GetValue<string>(GenericTimeSeriesMetaData.ElementName));
            Assert.AreEqual("My Run Name", metadata.GetValue<string>(GenericTimeSeriesMetaData.RunName));
        }

        [Test]
        public void TestSaveAndLoadIdenticalNames()
        {
            var start = new DateTime(1990, 1, 1);
            double[] vals = Enumerable.Range(0, 1000).Select(i => (double)i).ToArray();
            TimeSeries ts = new TimeSeries(start, TimeStep.Daily, vals);
            ts.name = "My / Time Series";
            ts.units = Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond);

            vals = Enumerable.Range(1000, 1000).Select(i => (double)i).ToArray();
            TimeSeries ts2 = new TimeSeries(start, TimeStep.Daily, vals);
            ts2.name = "My / Time Series";
            ts2.units = Unit.PredefinedUnit(CommonUnits.squareMetres);

            var writer = new HDF5TimeSeriesIO();

            writer.Use(ts);
            writer.Use(ts2);

            var fn = "DuplicateNames_TestTimeSeriesIO.h5";
            writer.Save(fn);

            var reader = new HDF5TimeSeriesIO();
            reader.Load(fn);

            TimeSeries retrieved = (TimeSeries)reader.DataSets.First(t => t.name == "My / Time Series");
            Assert.IsTrue(ts.EqualData(retrieved));
            Assert.IsTrue(ts.IsCompatibleWith(retrieved));
            Assert.AreEqual(ts.name, retrieved.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond), retrieved.units);

            TimeSeries retrieved2 = (TimeSeries)reader.DataSets.First(t => t.name == "My / Time Series 1");
            Assert.IsTrue(ts2.EqualData(retrieved2));
            Assert.IsTrue(ts2.IsCompatibleWith(retrieved2));
            Assert.AreEqual("My / Time Series 1", retrieved2.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.squareMetres), retrieved2.units);
        }

        private static void SaveAndLoadDaily(string fn="double",HDF5DataType precision=HDF5DataType.Double)
        {
            var start = new DateTime(1990, 1, 1);
            double[] vals = Enumerable.Range(0, 1000).Select(i => (double) i).ToArray();
            TimeSeries ts = new TimeSeries(start, TimeStep.Daily, vals);
            ts.name = "My Time Series";
            ts.units = Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond);

            vals = Enumerable.Range(1000, 1000).Select(i => (double) i).ToArray();
            TimeSeries ts2 = new TimeSeries(start, TimeStep.Daily, vals);
            ts2.name = "Mine / Someone's Time Series";
            ts2.units = Unit.PredefinedUnit(CommonUnits.squareMetres);

            var writer = new HDF5TimeSeriesIO();
            writer.DataType = precision;

            writer.Use(ts);
            writer.Use(ts2);

            fn = fn+"_TestTimeSeriesIO.h5";
            writer.Save(fn);

            var reader = new HDF5TimeSeriesIO();
            reader.Load(fn);

            TimeSeries retrieved = (TimeSeries) reader.DataSets.First(t => t.name == "My Time Series");
            Assert.IsTrue(ts.EqualData(retrieved));
            Assert.IsTrue(ts.IsCompatibleWith(retrieved));
            Assert.AreEqual(ts.name, retrieved.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond), retrieved.units);

            TimeSeries retrieved2 = (TimeSeries) reader.DataSets.First(t => t.name == "Mine / Someone's Time Series");
            Assert.IsTrue(ts2.EqualData(retrieved2));
            Assert.IsTrue(ts2.IsCompatibleWith(retrieved2));
            Assert.AreEqual(ts2.name, retrieved2.name);
            Assert.AreEqual(Unit.PredefinedUnit(CommonUnits.squareMetres), retrieved2.units);
        }
    }
}
