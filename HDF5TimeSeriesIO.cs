using FlowMatters.Source.HDF5IO.h5ss;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TIME.Core;
using TIME.DataTypes;
using TIME.DataTypes.IO;

namespace FlowMatters.Source.HDF5IO
{
    public class HDF5TimeSeriesIO : MultiTimeSeriesIO
    {
        public override string Description => "HDF5 Based time series storage";

        public override string Filter => ".h5";

        public override bool CanSave(string filename)
        {
            throw new NotImplementedException();
        }

        public override void Load(FileReader reader)
        {
            var f = new HDF5File(reader.FileName);
            data.AddRange(f.Groups.Select(kvp => ReadTimeSeries(kvp.Key, kvp.Value)).ToArray());
        }

        private TimeSeries ReadTimeSeries(string name, HDF5Group group)
        {
            double[] values = (double[]) group.DataSets["Values"].Get();
            DateTime[] dates = ((long[]) group.DataSets["Dates"].Get()).Select(ticks => new DateTime(ticks)).ToArray();
            var result = TimeSeriesExtensions.FromDates(dates, values);
            result.name = RestoreName(name);
            if (group.Attributes.Contains("Units"))
            {
                string unitString = (string)group.Attributes["Units"];
                result.units = Unit.parse(unitString);
            }
            return result;
        }

        public override void Save(FileWriter writer)
        {
            HDF5File dest = new HDF5File(writer.FileName,HDF5FileMode.WriteNew);
            foreach (TimeSeries ts in DataSets)
            {
                WriteTimeSeries(dest,ts);
            }

            dest.Close();
        }

        protected void WriteTimeSeries(HDF5File dest, TimeSeries ts, string path=null)
        {
            path = UniquePath(dest, ts, path);
            path = H5SafeName(path);
            dest.CreateGroup(path);
            var grp = dest.Groups[path];
            grp.CreateDataset("Values",ts.ToArray());
            grp.CreateDataset("Dates",ts.Ticks());
            grp.Attributes.Create("Units",ts.units.ToString());
        }

        private string H5SafeName(string path)
        {
            return path.Replace("/", "%SLASH%");
        }

        private string RestoreName(string name)
        {
            return name.Replace("%SLASH%", "/");
        }

        private static string UniquePath(HDF5File dest, TimeSeries ts, string path)
        {
            if (path == null)
            {
                path = ts.name;
            }

            string origPath = path;
            int suffix = 1;
            while (dest.Groups.ContainsKey(path) || dest.DataSets.ContainsKey(path))
            {
                path = $"{origPath} {suffix}";
                suffix++;
            }
            return path;
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
