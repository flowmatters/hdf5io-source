using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using RiverSystem;
using RiverSystem.Controls.Common.RelativePathLoader;
using TIME.ManagedExtensions;
using TIME.ScenarioManagement;
using RiverSystemConfiguration = RiverSystem.RiverSystemConfiguration;

namespace FlowMatters.Source.HDF5IO
{
    /// <summary>
    /// Interaction logic for DataStreamingOptionsWindow.xaml
    /// </summary>
    public partial class DataStreamingOptionsWindow : Window, IScenarioHandler<RiverSystemScenario>
    {
        public bool StreamingEnabled { get; set; }

        public RiverSystemScenario Scenario { get; set; }

        public int TimeWindow { get; set; }
        public int PrecisionIndex { get; set; }
        public RelativePathFileSelectorViewModel RelativePathFileSelectorViewModel { get; set; }

        public DataStreamingOptionsWindow(RiverSystemScenario scenario)
        {
            Scenario = scenario;
            Config = Scenario.CurrentRiverSystemConfiguration;
            StreamingEnabled = Scenario.AllPluginDataModels.OfType<StreamingOutputManager>().Any();

            var project = scenario.Project;
            var projectIsSaved = !string.IsNullOrEmpty(project.FileName);
            RelativePathFileSelectorViewModel = new RelativePathFileSelectorViewModel(projectIsSaved, "RunResults.h5",
                currentProjectPath: projectIsSaved? project.FullFilename:"",
                filter:"HDF5 Files|*.h5",
                saveSelector:true);
            InitializeComponent();
        }

        public RiverSystemConfiguration Config { get; set; }

        private void OkButtonClick(object sender, RoutedEventArgs e)
        {
            ApplyChanges();
            Close();
        }

        private void ApplyChanges()
        {
//            MessageBox.Show("This form does nothing at the moment. Just sayin'");
            if (StreamingEnabled)
            {
                ConfigureStreaming();
            }
            else
            {
                RemoveStreaming();
            }
        }

        void ConfigureStreaming()
        {
            if (Scenario.PluginDataModels.OfType<StreamingOutputManager>().Any())
            {
                return;
            }
            var streamer = new StreamingOutputManager(Scenario);
            streamer.Destination = RelativePathFileSelectorViewModel.FullPath;
            Scenario.PluginDataModels.Add(streamer);
        }

        void RemoveStreaming()
        {
            if (!Scenario.PluginDataModels.OfType<StreamingOutputManager>().Any())
            {
                return;
            }

            Scenario.PluginDataModels.RemoveAll(plugin=>plugin is StreamingOutputManager);
        }
    }
}
