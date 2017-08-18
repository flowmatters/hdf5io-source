using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using RiverSystem;
using RiverSystem.Controls.Attributes;
using RiverSystem.Controls.Interfaces;

namespace FlowMatters.Source.HDF5IO
{
    [MenuItem("Data Streaming...", MenuType.Advanced, 999)]
    public class DataStreamingOptionsMenuItem : IControllerExtension
    {
        private RiverSystemScenario scenario;
        public void Initialise(RiverSystemScenario scenario)
        {
            this.scenario = scenario;
        }

        public void Show(Form parent)
        {
            (new DataStreamingOptionsWindow(scenario)).ShowDialog();
        }

        public bool RequiresScenario { get { return true; } }

        public bool RequiresProject
        {
            get { return true; }
        }

        public bool RefreshSchematic { get { return false; } }
        public System.Drawing.Image Icon { get; }
    }
}
