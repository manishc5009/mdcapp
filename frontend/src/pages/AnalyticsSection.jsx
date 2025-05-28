import React, { useEffect, useRef } from 'react';
import Chart from 'chart.js/auto';

const AnalyticsSection = () => {
  const uploadsChartRef = useRef(null);
  const statusChartRef = useRef(null);
  const destinationChartRef = useRef(null);

  useEffect(() => {
    const uploadsCtx = uploadsChartRef.current.getContext('2d');
    const statusCtx = statusChartRef.current.getContext('2d');
    const destinationCtx = destinationChartRef.current.getContext('2d');

    const uploadsChart = new Chart(uploadsCtx, {
      type: 'bar',
      data: {
        labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        datasets: [{
          label: 'Uploads',
          data: [30, 45, 120, 40, 70, 90, 20, 80, 100, 50, 90, 30],
          backgroundColor: '#3b82f6',
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
      },
    });

    const statusChart = new Chart(statusCtx, {
      type: 'doughnut',
      data: {
        labels: ['Success', 'Failed'],
        datasets: [{
          data: [80, 20],
          backgroundColor: ['#22c55e', '#ef4444'],
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
      },
    });

    const destinationChart = new Chart(destinationCtx, {
      type: 'bar',
      data: {
        labels: ['Google', 'Meta', 'Neilsen', 'Amazon', 'Microsoft', 'Linkedin', 'Twitter', 'Youtube', 'Snapchat',' Tiktok', 'Reddit', 'Pinterest'],
        datasets: [{
          label: 'Data Volume',
          data: [120, 80, 30, 150, 220, 60, 40, 70, 650, 90, 100, 110],
          backgroundColor: '#3b82f6',
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
      },
    });

    return () => {
      uploadsChart.destroy();
      statusChart.destroy();
      destinationChart.destroy();
    };
  }, []);

  return (
    <div className="section">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        <div className="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-800">Uploads by Month</h2>
          </div>
          <div className="p-4" style={{ height: 300 }}>
            <canvas ref={uploadsChartRef} />
          </div>
        </div>
        <div className="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-800">Success vs Failed</h2>
          </div>
          <div className="p-4" style={{ height: 300 }}>
            <canvas ref={statusChartRef} />
          </div>
        </div>
      </div>
      <div className="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200">
        <div className="p-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-800">Data Volume by Resource</h2>
        </div>
        <div className="p-4" style={{ height: 350 }}>
          <canvas ref={destinationChartRef} />
        </div>
      </div>
    </div>
  );
};

export default AnalyticsSection;
