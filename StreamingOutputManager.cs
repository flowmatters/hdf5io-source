using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowMatters.H5SS;
using RiverSystem;
using TIME.Core;
using TIME.DataTypes;
using TIME.ScenarioManagement;
using TIME.ScenarioManagement.RunManagement;
using RiverSystemConfiguration = RiverSystem.RiverSystemConfiguration;

namespace FlowMatters.Source.HDF5IO
{
    public class StreamingOutputManager :
        IPluginInitialise<RiverSystemScenario>,
        IPluginRunStart<RiverSystemScenario>,
        IPluginRunEnd<RiverSystemScenario>,
        IPluginAfterStep<RiverSystemScenario>
    {
        public StreamingOutputManager(RiverSystemScenario scenario) 
        {
            Scenario = scenario;
//            Configuration = config;
        }

        public RiverSystemConfiguration Configuration { get; set; }
        public RiverSystemScenario Scenario { get; set; }
        public string Destination { get; set; }
        private HDF5File _destFile;
        private UniqueNameResolver _nameResolver;
        private List<HDF5TimeSeriesState> states;
        private List<TimeSeries> allSeries;
        public int id
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

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

            _destFile = new HDF5File(Destination, HDF5FileMode.WriteNew);
            _nameResolver = new UniqueNameResolver();
            scenario.RunManager.CurrentConfiguration.MakeResultTimeSeries = MakeStreamingTimeSeries;
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
            var streamer = new StreamingOutputManager(scenario);
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
