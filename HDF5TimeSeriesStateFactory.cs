using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using FlowMatters.H5SS;
using SourceOutputStreaming;
using TIME.Core;
using TIME.DataTypes;

namespace FlowMatters.Source.HDF5IO
{
    [FileExtension(".h5"), Description("HDF5 Files")]
    public class HDF5TimeSeriesStateFactory : ITimeSeriesStateFactory
    {
        public string Destination { get; set; }
        public StreamingOutputOverwriteOption OverwriteOption { get; set; }
        public int BufferSize { get; set; }
        private HDF5File _destFile;
        private UniqueNameResolver _nameResolver;
        private List<HDF5TimeSeriesState> states;
        private List<TimeSeries> allSeries;

        public TimeSeries NewTimeSeries(DateTime start, DateTime end, TimeStep ts, string name, Unit units)
        {
            HDF5TimeSeriesState state =
                HDF5TimeSeriesState.CreateBufferedWrite(
                    _destFile,
                    start,
                    end,
                    ts,
                    units,
                    name,
                    _nameResolver);
            states.Add(state);
            var result = new TimeSeries(state);
            result.name = name;
            result.units = units;
            allSeries.Add(result);

            return result;
        }

        public void AfterStep(DateTime step)
        {
        }

        public void AfterRun()
        {
            states.ForEach(s => s.WriteBuffer());
            allSeries.ForEach(ts => HDF5TimeSeriesMetadata.WriteMetadata(ts, ((HDF5TimeSeriesState)ts.state).DataSet));

            if (_destFile == null)
                return;

            _destFile.Close();

            var reopened = new HDF5File(Destination, HDF5FileMode.ReadOnly);
            var newDataSets = reopened.DataSets;
            states.ForEach(s => s.SwitchToReadMode(newDataSets));

            states = null;
            allSeries = null;
            _destFile = null;
        }

        public void NewRun()
        {
            if (_destFile != null)
            {
                _destFile.Close();
            }

            states = new List<HDF5TimeSeriesState>();
            allSeries = new List<TimeSeries>();

            _destFile = CreateDestinationFile();
            _nameResolver = new UniqueNameResolver();
        }

        private HDF5File CreateDestinationFile()
        {
            string fn = Destination;
            if (File.Exists(fn))
            {
                switch (OverwriteOption)
                {
                    case StreamingOutputOverwriteOption.Fail:
                        throw new IOException("Cannot initialise streaming. File exists");
                    case StreamingOutputOverwriteOption.Increment:
                        fn = IncrementFilename();
                        break;
                }
            }

            try
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                return new HDF5File(fn, HDF5FileMode.WriteNew);
            }
            catch (IOException)
            {
                // See if its related to the file still being 'open' and waiting closure in a finalizer
                // Not pretty, but we don't know when Source is done with the time series...
                GC.Collect();
                GC.WaitForPendingFinalizers();
                return new HDF5File(fn, HDF5FileMode.WriteNew);
            }
        }

        private string IncrementFilename()
        {
            string fn = Destination;
            string dir = Path.GetDirectoryName(Destination);
            string ext = Path.GetExtension(Destination);
            string baseName = Path.GetFileNameWithoutExtension(Destination);
            int n = 0;

            while (File.Exists(fn))
            {
                n++;
                fn = string.Format("{0} ({1}){2}", baseName, n, ext);
            }
            return Path.Combine(dir, fn);
        }
    }
}
