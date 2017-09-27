using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowMatters.H5SS;
using NUnit.Framework;
using TIME.Core;
using TIME.DataTypes;
using TIME.DataTypes.IO.CsvFileIo;

namespace FlowMatters.Source.HDF5IO.Tests
{
    public class TestStream
    {

        [Test]
        public void TestStreamToDisk()
        {
            const int BLOCKS = 10;
            const int BLOCK_SIZE = 100;
            var start = new DateTime(1990, 1, 1);
            double[] allValues = ARangeDouble(0, BLOCKS*BLOCK_SIZE);
            double[][] blocks = new double[BLOCKS][];
            for (int i = 0; i < BLOCKS; i++)
            {
                blocks[i] = ARangeDouble(i*BLOCK_SIZE, BLOCK_SIZE);
            }

            var fn = "TestTimeSeriesIO_withMetadata.h5";
            var tsName = "My Time Series";
            var units = Unit.PredefinedUnit(CommonUnits.cubicMetresPerSecond);

            HDF5File destFile = new HDF5File(fn,HDF5FileMode.WriteNew);
            var timeStep = TimeStep.Daily;
            HDF5TimeSeriesState state = 
                HDF5TimeSeriesState.CreateBufferedWrite(
                    destFile,
                    start,
                    timeStep.add(start,BLOCKS*BLOCK_SIZE-1),
                    timeStep,
                    units,
                    tsName);

            TimeSeries ts = new TimeSeries(state);

            for (int i = 0; i < allValues.Length; i++)
            {
                ts.setItem(i,allValues[i]);
            }
            state.WriteBuffer();
            //ts.name = tsName;

            //var metadata = new GenericTimeSeriesMetaData();
            //metadata.SetValue(GenericTimeSeriesMetaData.ElementName, "My Element");
            //metadata.SetValue(GenericTimeSeriesMetaData.RunName, "My Run Name");

            //ts.metadata = metadata;
            destFile.Close();

            var reader = new HDF5TimeSeriesIO();
            reader.Load(fn);

            TimeSeries retrieved = (TimeSeries) reader.DataSets.First(t => t.name == tsName);
            Assert.AreEqual(start,retrieved.Start);
            Assert.AreEqual(timeStep, retrieved.timeStep);
            Assert.AreEqual(allValues.Length,retrieved.Count());
            for (int i = 0; i < allValues.Length; i++)
            {
                Assert.AreEqual(allValues[i],retrieved[i]);
            }

            //Assert.IsInstanceOf<GenericTimeSeriesMetaData>(retrieved.metadata);
            //var retrMetadata = (GenericTimeSeriesMetaData) retrieved.metadata;
            //Assert.AreEqual("My Element", metadata.GetValue<string>(GenericTimeSeriesMetaData.ElementName));
            //Assert.AreEqual("My Run Name", metadata.GetValue<string>(GenericTimeSeriesMetaData.RunName));
        }

        private static double[] ARangeDouble(int start, int count)
        {
            return Enumerable.Range(start, count).Select(i => (double) i).ToArray();
        }
    }
}