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
using TIME.ScenarioManagement;

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
            var project = scenario.Project;
            var projectIsSaved = !string.IsNullOrEmpty(project.FileName);
            RelativePathFileSelectorViewModel = new RelativePathFileSelectorViewModel(projectIsSaved, "RunResults.h5",
                currentProjectPath: projectIsSaved? project.FullFilename:"",
                filter:"HDF5 Files|*.h5",
                saveSelector:true);
            InitializeComponent();
        }

        private void OkButtonClick(object sender, RoutedEventArgs e)
        {
            ApplyChanges();
            Close();
        }

        private void ApplyChanges()
        {
            MessageBox.Show("This form does nothing at the moment. Just sayin'");
        }
    }
}
