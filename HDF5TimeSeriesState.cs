using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowMatters.H5SS;
using TIME.DataTypes;
using TIME.DataTypes.TimeSeriesImplementation;

namespace FlowMatters.Source.HDF5IO
{
    class HDF5TimeSeriesState : TimeSeriesState
    {
        public HDF5TimeSeriesState(HDF5DataSet dataset)
        {
            DataSet = dataset;
        }

        ~HDF5TimeSeriesState()
        {
            DataSet = null;
        }

        private HDF5DataSet DataSet { get; set; }

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
            EnsureLoaded();
            Data[i]=v;
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
