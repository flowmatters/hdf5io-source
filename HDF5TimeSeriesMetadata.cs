using System;
using FlowMatters.H5SS;
using TIME.DataTypes;
using TIME.DataTypes.IO.CsvFileIo;

namespace FlowMatters.Source.HDF5IO
{
    public class HDF5TimeSeriesMetadata
    {
        public HDF5TimeSeriesMetadata(HDF5TimeSeriesState state)
        {
            throw new NotImplementedException();
        }

        public static void WriteMetadata(TimeSeries ts, HDF5DataSet dataset)
        {
            if (!(ts.metadata is GenericTimeSeriesMetaData)) return;

            var meta = (GenericTimeSeriesMetaData)ts.metadata;
            foreach (var key in meta.GetKeys())
            {
                var val = meta.GetValue<object>(key);
                if (val is string || val is float || val is long || val is double || val is int)
                    dataset.Attributes.Create(Constants.META_PREFIX + key, val);
            }
        }
    }
}