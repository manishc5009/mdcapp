import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import {
  faFileExcel, faFileUpload, faCheckCircle, faTimesCircle, faDatabase,
  faHistory, faPlusCircle, faTasks, faCog, faBolt
} from '@fortawesome/free-solid-svg-icons';
import DashboardCard from '../components/DashboardCard';
import QuickActionButton from '../components/QuickActionButton';
import RecentUploadItem from '../components/RecentUploadItem';

const apiurl = import.meta.env.VITE_API_URL;

const CACHE_KEY = 'dashboardMetricsCache';
const CACHE_TTL = 10 * 60 * 1000; // 10 minutes in milliseconds

const DashboardSection = () => {
  const [metrics, setMetrics] = useState({
    totalUploads: null,
    successfulUploads: null,
    failedUploads: null,
    dataProcessed: null,
  });
  const [recentUploads, setRecentUploads] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDashboardMetrics = async () => {
      try {
        const response = await fetch(`${apiurl}/api/dashboard/metrics`);
        if (!response.ok) {
          throw new Error('Failed to fetch dashboard metrics');
        }
        const data = await response.json();
        const cacheData = {
          timestamp: Date.now(),
          metrics: {
            totalUploads: data.totalUploads,
            successfulUploads: data.successfulUploads,
            failedUploads: data.failedUploads,
            dataProcessed: data.dataProcessed,
          },
          recentUploads: data.recentUploads || [],
        };
        localStorage.setItem(CACHE_KEY, JSON.stringify(cacheData));
        setMetrics(cacheData.metrics);
        setRecentUploads(cacheData.recentUploads);
        setLoading(false);
      } catch (err) {
        setError(err.message);
        setLoading(false);
      }
    };

    const cached = localStorage.getItem(CACHE_KEY);
    if (cached) {
      try {
        const parsed = JSON.parse(cached);
        if (Date.now() - parsed.timestamp < CACHE_TTL) {
          setMetrics(parsed.metrics);
          setRecentUploads(parsed.recentUploads);
          setLoading(false);
          return;
        }
      } catch {
        // ignore parse errors and fetch fresh data
      }
    }
    fetchDashboardMetrics();
  }, []);

  if (loading) {
    return <div>Loading dashboard metrics...</div>;
  }

  if (error) {
    return <div>Error loading dashboard metrics: {error}</div>;
  }

  return (
    <div>
      {/* Dashboard Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
        <DashboardCard icon={faFileExcel} iconColor="text-blue-500" bgColor="bg-blue-100" title="Total Uploads" value={metrics.totalUploads ?? 'N/A'} />
        <DashboardCard icon={faCheckCircle} iconColor="text-green-500" bgColor="bg-green-100" title="Successful" value={metrics.successfulUploads ?? 'N/A'} />
        <DashboardCard icon={faTimesCircle} iconColor="text-red-500" bgColor="bg-red-100" title="Failed" value={metrics.failedUploads ?? 'N/A'} />
        <DashboardCard icon={faDatabase} iconColor="text-purple-500" bgColor="bg-purple-100" title="Data Processed" value={metrics.dataProcessed ?? 'N/A'} />
      </div>

      {/* Recent Activity */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Recent Uploads */}
        <div className="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-800 flex items-center">
              <FontAwesomeIcon icon={faHistory} className="mr-2 text-blue-500" /> Recent Uploads
            </h2>
          </div>
          <div className="divide-y divide-gray-200">
            {recentUploads.length === 0 ? (
              <p className="p-4 text-gray-500">No recent uploads found.</p>
            ) : (
              recentUploads.map((upload) => (
                <RecentUploadItem
                  key={upload.id}
                  statusIcon={upload.status === 'success' ? faCheckCircle : faTimesCircle}
                  statusColor={upload.status === 'success' ? 'bg-green-100 text-green-500' : 'bg-red-100 text-red-500'}
                  fileName={upload.fileName}
                  timeAgo={upload.timeAgo}
                  size={upload.size}
                />
              ))
            )}
          </div>
        </div>

        {/* Quick Actions */}
        <div className="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-800 flex items-center">
              <FontAwesomeIcon icon={faBolt} className="mr-2 text-yellow-500" /> Quick Actions
            </h2>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-2 gap-4">
              <Link to="/upload">
                <QuickActionButton icon={faFileUpload} iconColor="text-blue-500" bgColor="bg-blue-100" label="New Upload" hoverBgColor="blue-50" />
              </Link>
              <Link to="/destination">
                <QuickActionButton icon={faPlusCircle} iconColor="text-green-500" bgColor="bg-green-100" label="New Destination" hoverBgColor="green-50" />
              </Link>
              <Link to="/jobs">
                <QuickActionButton icon={faTasks} iconColor="text-purple-500" bgColor="bg-purple-100" label="View Jobs" hoverBgColor="purple-50" />
              </Link>
              <Link to="/settings">
                <QuickActionButton icon={faCog} iconColor="text-red-500" bgColor="bg-red-100" label="Settings" hoverBgColor="red-50" />
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DashboardSection;
