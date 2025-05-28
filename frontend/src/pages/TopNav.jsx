import React from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSearch, faBell } from '@fortawesome/free-solid-svg-icons';

const TopNav = () => {
  return (
    <div className="bg-white border-b border-gray-200 p-4 flex justify-between items-center">
      <div className="flex items-center space-x-4 flex-1">
        <div className="relative w-full max-w-xs">
          <FontAwesomeIcon icon={faSearch} className="absolute left-3 top-2.5 text-gray-400" />
          <input
            type="text"
            placeholder="Search..."
            className="pl-10 pr-3 py-1.5 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 w-full"
          />
        </div>
      </div>
      <div className="flex items-center space-x-4">
        <div className="relative">
          <FontAwesomeIcon icon={faBell} className="text-gray-500 cursor-pointer" />
          <span className="absolute top-0 right-0 w-2 h-2 bg-red-500 rounded-full"></span>
        </div>
      </div>
    </div>
  );
};

export default TopNav;
