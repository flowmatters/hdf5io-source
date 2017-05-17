using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace FlowMatters.Source.HDF5IO.h5ss.Tests
{
    [TestFixture]
    class TestRead
    {
        [Test]
        public void FindVariables()
        {
            string cwd = Directory.GetCurrentDirectory();
            string fn = "..\\..\\..\\hdf5-io-source\\h5ss\\Tests\\example.h5";
            Assert.IsTrue(File.Exists(fn),String.Format("Expected {0} accessible from {1}",fn,cwd));
            HDF5File file = new HDF5File(fn);
            IDictionary<string, HDF5Group> groups = file.Groups;

            Assert.AreEqual(4,groups.Count,String.Format("Expected 4 groups, but had {0}:{1}",groups.Count,String.Join(",",groups.Keys)));
        }
    }
}
