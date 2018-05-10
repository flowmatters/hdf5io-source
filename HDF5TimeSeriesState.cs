using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowMatters.H5SS;
using TIME.Core;
using TIME.DataTypes;
using TIME.DataTypes.TimeSeriesImplementation;

namespace FlowMatters.Source.HDF5IO
{
    enum HDF5TimeSeriesMode
    {
        ReadOnly,
        ReadWrite
    }

    public class HDF5TimeSeriesState : TimeSeriesState
    {
        public const int BUFFER_SIZE=512; // 256 entries translates to 4kb buffer @ double precision, which is one block of NTFS

        public static HDF5TimeSeriesState CreateRead(HDF5DataSet dataset)
        {
            return new HDF5TimeSeriesState(dataset);
        }

        public static HDF5TimeSeriesState CreateBufferedWrite(HDF5Container parent,
            DateTime start, DateTime end, TimeStep ts, Unit units, string name,
            UniqueNameResolver resolver=null)
        {
            resolver = resolver ?? new UniqueNameResolver();

            ulong[] shape = {(ulong) ts.numSteps(start, end)};
            name = resolver.UniquePath(name);
            HDF5DataSet ds = parent.CreateDataset(name, shape, typeof(double),chunkShape: new[]{(ulong)BUFFER_SIZE});
            ds.Attributes.Create(Constants.UNITS, units.ToString());
            ds.Attributes.Create(Constants.START_DATE, start.Ticks);
            ds.Attributes.Create(Constants.TIMESTEP, ts.Name);

            var result = new HDF5TimeSeriesState(ds);
            result._start = start;
            result._timeStep = ts;
            result.InitialiseWriteMode();
            return result;
        }

        private HDF5TimeSeriesMode _mode = HDF5TimeSeriesMode.ReadOnly;
        private double[] buffer;
        private ulong bufferLocation;
        private HDF5TimeSeriesState(HDF5DataSet dataset)
        {
            DataSet = dataset;
        }

        ~HDF5TimeSeriesState()
        {
            DataSet = null;
        }

        private void InitialiseWriteMode()
        {
            _mode = HDF5TimeSeriesMode.ReadWrite;
            buffer = new double[BUFFER_SIZE];
            bufferLocation = 0;
        }

        internal HDF5DataSet DataSet { get; set; }

        public bool Loaded { get; private set; }

        public override int itemForTime(DateTime dt)
        {
            return timeStep.itemForTime(dt) - timeStep.itemForTime(start);
        }

        public override DateTime timeForItem(int i)
        {
            return timeStep.add(start, i);
        }

        public override double item(int i)
        {
            EnsureLoaded();
            return Data[i];
        }

        private void EnsureLoaded()
        {
            if (Loaded)
                return;

            Data = HDF5TimeSeriesIO.ReturnPrecision(DataSet.Get());
            Loaded = true;
        }

        public override void setItem(int i, double v)
        {
            ulong offset = (ulong) i - bufferLocation;
            if (_mode == HDF5TimeSeriesMode.ReadOnly)
            {
                EnsureLoaded();
                Data[i] = v;
            }
            else
            {
                if (offset >= BUFFER_SIZE)
                {
                    WriteBuffer();
                    ZeroBuffer();
                    bufferLocation = BUFFER_SIZE*((ulong) i/BUFFER_SIZE);
                    offset = 0;
                }
                buffer[offset] = v;
            }
        }

        private void ZeroBuffer()
        {
            for (int i = 0; i < buffer.Length; i++)
            {
                buffer[i] = 0.0;
            }
        }

        public void WriteBuffer()
        {
            var toWrite = buffer;
            if ((bufferLocation + BUFFER_SIZE) >= (ulong)Count)
            {
                toWrite = SliceBuffer(0, (ulong)Count - bufferLocation);
            }

            DataSet.Put(toWrite,new ulong[] {bufferLocation});
            DataSet.Flush();
        }

        private double[] SliceBuffer(ulong start, ulong endOpen)
        {
            ulong size = endOpen - start;
            double[] result = new double[size];
            for (ulong i = 0; i < (ulong)result.Length; i++)
            {
                result[i] = buffer[start + i];
            }
            return result;
        }

        public void SwitchToReadMode(IDictionary<String,HDF5DataSet> datasets)
        {
            buffer = null;
            DataSet = datasets[DataSet.Name];
        }

        public double[] Data { get; private set; }

        public override TimeSeriesState Clone()
        {
            return new HDF5TimeSeriesState(DataSet); // IS THIS OK?
        }

        public override void init(DateTime startTime, int numEntries)
        {
            throw new NotImplementedException();
        }

        public override void init(DateTime startTime, DateTime endTime)
        {
            throw new NotImplementedException();
        }

        private TimeStep _timeStep;

        public override TimeStep timeStep
        {
            get
            {
                if (_timeStep == null)
                {
                    _timeStep = TimeStep.FromName((string)DataSet.Attributes[Constants.TIMESTEP]);
                }
                return _timeStep;
            }
        }

        private int _count = -1;

        public override int Count
        {
            get
            {
                if (_count < 0)
                {
                    _count = (int) DataSet.Shape[0];
                }
                return _count;
            }
        }

        private DateTime _start = DateTime.MinValue;

        public override DateTime start { get {
            if (_start==DateTime.MinValue)
            {
                _start = new DateTime((long) DataSet.Attributes[Constants.START_DATE]);
            }
            return _start;
        }
            set { _start = value; } }

        public override DateTime end
        {
            get { return timeStep.add(start, Count-1); }
            set { throw new NotSupportedException();}
        }
    }
}
