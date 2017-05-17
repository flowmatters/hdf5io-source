using FlowMatters.Source.HDF5IO.h5ss;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TIME.DataTypes;
using TIME.DataTypes.IO;

namespace FlowMatters.Source.HDF5IO
{
    class HDF5TimeSeriesIO : MultiTimeSeriesIO
    {
        public override string Description => "HDF5 Based data storages";

        public override string Filter => ".h5";

        public override bool CanSave(string filename)
        {
            throw new NotImplementedException();
        }

        public override void Load(FileReader reader)
        {
            var f = new HDF5File(reader.FileName);

            throw new NotImplementedException();
        }

        public override void Save(FileWriter writer)
        {
//            var f = HDF.PInvoke.H5F.open
            throw new NotImplementedException();
        }
    }
}
