using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HDF.PInvoke;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FlowMatters.Source.HDF5IO.h5ss
{
    public enum HDF5FileMode
    {
        ReadWrite = (int) HDF.PInvoke.H5F.ACC_RDWR,
        ReadOnly = (int) HDF.PInvoke.H5F.ACC_RDONLY
    }

    public class HDF5File
    {
        bool closed;
        Int64 h5ID;
        private H5F.info_t _bhInfo;

        public HDF5File(string fn, HDF5FileMode mode=HDF5FileMode.ReadOnly)
        {
            h5ID = HDF.PInvoke.H5F.open(fn, (uint)mode);
            _bhInfo = new H5F.info_t();
            HDF.PInvoke.H5F.get_info(h5ID, ref _bhInfo);

        }

        public void Close()
        {
            H5F.close(h5ID);
            closed = true;
        }

        ~HDF5File()
        {
            if (!closed)
                H5F.close(h5ID);
        }

        public Dictionary<string,long> X(bool dataSets)
        {
            Dictionary<string,long> datasetNames = new Dictionary<string, long>();
            Dictionary<string,long> groupNames = new Dictionary<string, long>();
            var rootId = H5G.open(h5ID, "/");

            H5O.visit(h5ID, H5.index_t.NAME, H5.iter_order_t.INC, new H5O.iterate_t(
              delegate (long objectId, IntPtr namePtr, ref H5O.info_t info, IntPtr op_data)
              {
                  string objectName = Marshal.PtrToStringAnsi(namePtr);
                  H5O.info_t gInfo = new H5O.info_t();
                  H5O.get_info_by_name(objectId, objectName, ref gInfo);
                  
                  if (gInfo.type == H5O.type_t.DATASET)
                  {
                      datasetNames[objectName]=objectId;
                  }
                  else if (gInfo.type == H5O.type_t.GROUP)
                  {
                      groupNames[objectName] = objectId;
                  }
                  return 0;
              }), new IntPtr());

            H5G.close(rootId);

            // Print out the information that we found
            foreach (var line in datasetNames)
            {
                Debug.WriteLine(line);
            }

            if (dataSets)
                return datasetNames;
            return groupNames;
        }

        public IDictionary<string,HDF5Group> Groups
        {
            get
            {
                var nameIDs = X(false);
                var topLevel = nameIDs.Keys.Where(n => !n.Contains("/"));

                Dictionary<string, HDF5Group> result = new Dictionary<string, HDF5Group>();
                foreach( var key in topLevel)
                {
                    result[key] = new HDF5Group(key,nameIDs[key]);
                    //var nested = result.Where(kvp => kvp.Key.StartsWith(key + '/'));
                }
                return result;
            }
        }
    }

    public class HDF5Group
    {
        long _id;
        string _name;
        public string Name {
            get { return _name; }
            set
            {
                _name = value;
                // Update HDF5
            }
        }

        public HDF5Group(string name,long id)
        {
            _name = name;
            _id = id;
        }
    }
}
