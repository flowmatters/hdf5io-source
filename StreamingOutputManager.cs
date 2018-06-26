using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FlowMatters.H5SS;
using RiverSystem;
using TIME.Core;
using TIME.DataTypes;
using TIME.ManagedExtensions;
using TIME.ScenarioManagement;
using TIME.ScenarioManagement.RunManagement;
using RiverSystemConfiguration = RiverSystem.RiverSystemConfiguration;

namespace FlowMatters.Source.HDF5IO
{
    public enum StreamingOutputOverwriteOption
    {
        Fail,
        Overwrite,
        Increment
    }

    public class StreamingOutputManager :
        IPluginInitialise<RiverSystemScenario>,
        IPluginRunStart<RiverSystemScenario>,
        IPluginRunEnd<RiverSystemScenario>,
        IPluginAfterStep<RiverSystemScenario>
    {
        public StreamingOutputManager() 
        {
            OverwriteOption = StreamingOutputOverwriteOption.Fail;
//            Configuration = config;
        }

        public RiverSystemConfiguration Configuration { get; set; }
        public string Destination { get; set; }
        public StreamingOutputOverwriteOption OverwriteOption { get; set; }
        private HDF5File _destFile;
        private UniqueNameResolver _nameResolver;
        private List<HDF5TimeSeriesState> states;
        private List<TimeSeries> allSeries;
        public int id {get; set; }

        //public TimeSeries DefaultMakeResultsTimeSeries(
        //    DateTime start, DateTime end, TimeStep ts, string name, Unit units)
        //{
        //    return new TimeSeries(start, end, ts) { units = units, name = name };
        //}

        public TimeSeries MakeStreamingTimeSeries(
            DateTime start, DateTime end, TimeStep ts, string name, Unit units)
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
            allSeries.Add(result);

            return result;
        }

        public void ScenarioInitialise(RiverSystemScenario scenario, RunningConfiguration config)
        {
            if (_destFile != null)
            {
                _destFile.Close();
            }

            states = new List<HDF5TimeSeriesState>();
            allSeries = new List<TimeSeries>();

            _destFile = CreateDestinationFile();
            _nameResolver = new UniqueNameResolver();
            scenario.RunManager.CurrentConfiguration.MakeResultTimeSeries = MakeStreamingTimeSeries;
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
            string ext = Path.GetExtension(Destination);
            string baseName = Path.GetFileNameWithoutExtension(Destination); 
            int n = 0;

            while (File.Exists(fn))
            {
                n++;
                fn = string.Format("{0} ({1}){2}", baseName, n, ext);
            }
            return fn;
        }

        public void ScenarioRunStart(RiverSystemScenario scenario)
        {

        }

        public void ScenarioAfterStep(RiverSystemScenario scenario, DateTime step)
        {
        }

        public void ScenarioRunEnd(RiverSystemScenario scenario)
        {
            scenario.RunManager.CurrentConfiguration.MakeResultTimeSeries =
                scenario.RunManager.CurrentConfiguration.DefaultMakeResultsTimeSeries;

            states.ForEach(s=>s.WriteBuffer());
            allSeries.ForEach(ts=>HDF5TimeSeriesMetadata.WriteMetadata(ts,((HDF5TimeSeriesState)ts.state).DataSet));

            if (_destFile == null)
                return;

            _destFile.Close();

            var reopened = new HDF5File(Destination,HDF5FileMode.ReadOnly);
            var newDataSets = reopened.DataSets;
            states.ForEach(s=>s.SwitchToReadMode(newDataSets));

            states = null;
            allSeries = null;
            _destFile = null;
        }

        public static StreamingOutputManager EnableStreaming(RiverSystemScenario scenario, string destinationFilename)
        {
            if (scenario.PluginDataModels.OfType<StreamingOutputManager>().Any())
            {
                return scenario.PluginDataModels.OfType<StreamingOutputManager>().First();
            }
            var streamer = new StreamingOutputManager();
            streamer.Destination = destinationFilename;
            scenario.PluginDataModels.Add(streamer);
            return streamer;
        }

        public static void DisableStreaming(RiverSystemScenario scenario)
        {
            if (!scenario.PluginDataModels.OfType<StreamingOutputManager>().Any())
            {
                return;
            }

            scenario.PluginDataModels.RemoveAll(plugin=>plugin is StreamingOutputManager);
        }
    }
}
