using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace FlowMatters.Source.HDF5IO.h5ss.Tests
{
    public class BaseHDF5WriteTest
    {
        protected HDF5File file;
        //        string fn = @"..\..\..\hdf5io-source\TestData\Created.h5";
        protected string fn = @"Created.h5";

        [SetUp]
        public void Setup()
        {
            if (File.Exists(fn))
                File.Delete(fn);

            file = new HDF5File(fn, HDF5FileMode.ReadWrite);
            Assert.IsTrue(File.Exists(fn), String.Format("Expected {0} created", fn));
        }

        [TearDown]
        public void TearDown()
        {
            file.Close();
            File.Delete(fn);
        }

        public void ReopenForRead()
        {
            file.Close();
            file = new HDF5File(fn,HDF5FileMode.ReadOnly);
        }

        protected void TestBeforeAndAfter(Action modification, Action confirmation)
        {
            modification();
            confirmation();
            ReopenForRead();
            confirmation();
        }
    }
}
