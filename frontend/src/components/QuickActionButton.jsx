import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

const QuickActionButton = ({ icon, iconColor, bgColor, label, hoverBgColor }) => (
  <button
    className={`w-full flex flex-col items-center justify-center p-4 rounded-lg shadow transition bg-white hover:bg-${hoverBgColor} border border-gray-200`}
  >
    <span className={`p-2 rounded-full ${bgColor} mb-2`}>
      <FontAwesomeIcon icon={icon} className={`${iconColor} text-lg`} />
    </span>
    <span className="text-xs font-medium text-gray-700">{label}</span>
  </button>
);

export default QuickActionButton;
