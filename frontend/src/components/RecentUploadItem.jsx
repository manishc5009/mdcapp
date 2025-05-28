import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

const RecentUploadItem = ({ statusIcon, statusColor, fileName, timeAgo, size }) => (
  <div className="flex items-center px-4 py-3">
    <span className={`flex items-center justify-center w-8 h-8 rounded-full ${statusColor} mr-3`}>
      <FontAwesomeIcon icon={statusIcon} />
    </span>
    <div className="flex-1">
      <div className="font-medium text-gray-800">{fileName}</div>
      <div className="text-xs text-gray-500">{timeAgo} • {size}</div>
    </div>
  </div>
);

export default RecentUploadItem;