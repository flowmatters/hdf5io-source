using System;
using System.Linq;
using FlowMatters.H5SS;
using TIME.Core;
using TIME.DataTypes;
using TIME.DataTypes.IO;
using TIME.DataTypes.IO.CsvFileIo;

namespace FlowMatters.Source.HDF5IO
{
    public class HDF5TimeSeriesIO : MultiTimeSeriesIO
    {
        public static bool DEFAULT_LAZY_LOAD=true;

        public HDF5TimeSeriesIO()
        {
            LazyLoad = DEFAULT_LAZY_LOAD;
            DataType=HDF5DataType.Double;
            Compressed = true;
        }

        public HDF5TimeSeriesIO(bool lazyLoad)
        {
            LazyLoad = lazyLoad;
        }

        public bool LazyLoad { get; set; }
        public bool Compressed { get; set; }
        public override string Description => "HDF5 Based time series storage";

        public override string Filter => ".h5";
        public HDF5DataType DataType { get; set; }

        public override bool CanSave(string filename)
        {
            throw new NotImplementedException();
        }

        public override void Load(FileReader reader)
        {
            var f = new HDF5File(reader.FileName);
            data.AddRange(f.DataSets.Select(kvp => ReadTimeSeries(kvp.Key, kvp.Value)).ToArray());
            // When to close?
            if (!LazyLoad)
                f.Close();
        }

        private TimeSeries ReadTimeSeries(string name, HDF5DataSet dataset)
        {
            TimeSeries result;
            if (LazyLoad)
            {
                result = new TimeSeries(HDF5TimeSeriesState.CreateRead(dataset));
            }
            else
            {
                DateTime startDate = new DateTime((long)dataset.Attributes[Constants.START_DATE]);
                var timeStep = TimeStep.FromName((string)dataset.Attributes[Constants.TIMESTEP]);
                double[] values = ReturnPrecision(dataset.Get());
                result = new TimeSeries(startDate, timeStep, values);
            }

            result.name = UniqueNameResolver.RestoreName(name);
            if (dataset.Attributes.Contains(Constants.UNITS))
            {
                string unitString = (string)dataset.Attributes[Constants.UNITS];
                result.units = Unit.parse(unitString);
            }

            foreach (string key in dataset.Attributes.Keys)
            {
                if (!key.StartsWith(Constants.META_PREFIX))
                    continue;

                if (!(result.metadata is GenericTimeSeriesMetaData))
                    result.metadata = new GenericTimeSeriesMetaData();

                var meta = (GenericTimeSeriesMetaData) result.metadata;
                meta.SetValue(key.Substring(Constants.META_PREFIX.Length),dataset.Attributes[key]);
            }
            return result;
        }

        public override void Save(FileWriter writer)
        {
            HDF5File dest = new HDF5File(writer.FileName,HDF5FileMode.WriteNew);
            NameResolver.Reset();
            foreach (TimeSeries ts in DataSets)
            {
                WriteTimeSeries(dest,ts);
            }

            dest.Close();
        }

        UniqueNameResolver NameResolver = new UniqueNameResolver();

        protected void WriteTimeSeries(HDF5File dest, TimeSeries ts, string path=null)
        {
            path = NameResolver.UniquePath(path??ts.name);
            
            var dataset = dest.CreateDataset(path, ConvertPrecision(ts.ToArray()),null,null,Compressed);
            dataset.Attributes.Create(Constants.UNITS, ts.units.ToString());
            dataset.Attributes.Create(Constants.START_DATE,ts.timeForItem(0).Ticks);
            dataset.Attributes.Create(Constants.TIMESTEP, ts.timeStep.Name);

            HDF5TimeSeriesMetadata.WriteMetadata(ts,dataset);
        }

        private Array ConvertPrecision(double[] origArray)
        {
            switch (DataType)
            {
                case HDF5DataType.Double:
                    return origArray;
                case HDF5DataType.Float:
                    return origArray.Select(d => (float) d).ToArray();
                default:
                    throw new NotImplementedException();
            }
        }

        internal static double[] ReturnPrecision(Array array)
        {
            var elementType = array.ElementType();
            if (elementType == typeof(double))
                return (double[])array;

            if (elementType == typeof(float))
            {
                return ((float[]) array).Select(f => (double) f).ToArray();
            }
            throw new NotImplementedException();
        }
    }

    public static class TimeSeriesExtensions
    {
        public static DateTime[] Dates(this TimeSeries ts)
        {
            return Enumerable.Range(0,ts.Count).Select(i => ts.timeForItem(i)).ToArray();
        }

        public static long[] Ticks(this TimeSeries ts)
        {
            return ts.Dates().Select(d => d.Ticks).ToArray();
        }

        public static TimeSeries FromDates(DateTime[] dates, double[] values)
        {
            return new TimeSeries(dates[0],TimeStep.FromSeconds((dates[1]-dates[0]).TotalSeconds),values);
       }
    }
}
