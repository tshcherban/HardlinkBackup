using System;
using System.Runtime.InteropServices;

namespace HardLinkBackup
{
    /// <summary>
    /// 
    /// </summary>
    public class PerformanceCounter
    {
        [DllImport("kernel32.dll")]
        private static extern short QueryPerformanceCounter(ref long x);

        [DllImport("kernel32.dll")]
        private static extern short QueryPerformanceFrequency(ref long x);

        private long _startTime;
        private long _stopTime;
        private long _clockFrequency;
        private long _calibrationTime;

        public PerformanceCounter()
        {
            _startTime = 0;
            _stopTime = 0;
            _clockFrequency = 0;
            _calibrationTime = 0;
            Calibrate();
        }

        public void Calibrate()
        {
            QueryPerformanceFrequency(ref _clockFrequency);

            for (var i = 0; i < 1000; i++)
            {
                Start();
                Stop();
                _calibrationTime += _stopTime - _startTime;
            }

            _calibrationTime /= 1000;
        }

        public void Reset()
        {
            _startTime = 0;
            _stopTime = 0;
        }

        public void Start()
        {
            QueryPerformanceCounter(ref _startTime);
        }

        public void Stop()
        {
            QueryPerformanceCounter(ref _stopTime);
        }

        public TimeSpan GetElapsedTimeSpan()
        {
            return TimeSpan.FromMilliseconds(_GetElapsedTime_ms());
        }

        public TimeSpan GetSplitTimeSpan()
        {
            return TimeSpan.FromMilliseconds(_GetSplitTime_ms());
        }

        public double GetElapsedTimeInMicroseconds()
        {
            return (((_stopTime - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
        }

        public double GetSplitTimeInMicroseconds()
        {
            long currentCount = 0;
            QueryPerformanceCounter(ref currentCount);
            return (((currentCount - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency));
        }

        private double _GetSplitTime_ms()
        {
            long currentCount = 0;
            QueryPerformanceCounter(ref currentCount);
            return (((currentCount - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency) / 1000.0);
        }

        private double _GetElapsedTime_ms()
        {
            return (((_stopTime - _startTime - _calibrationTime) * 1000000.0 / _clockFrequency) / 1000.0);
        }
    }
}