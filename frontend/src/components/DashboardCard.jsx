import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

const DashboardCard = ({ icon, iconColor, bgColor, title, value }) => (
  <div className="dashboard-card bg-white rounded-lg shadow p-6 border border-gray-200 hover:transform hover:-translate-y-1 hover:shadow-lg transition">
    <div className="flex items-center">
      <div className={`p-3 rounded-full ${bgColor} mr-4`}>
        <FontAwesomeIcon icon={icon} className={`${iconColor} text-xl`} />
      </div>
      <div>
        <p className="text-sm text-gray-500">{title}</p>
        <p className="text-2xl font-bold">{value}</p>
      </div>
    </div>
  </div>
);

export default DashboardCard;