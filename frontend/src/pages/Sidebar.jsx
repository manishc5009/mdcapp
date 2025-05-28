import React, { useState } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faHome, faFileUpload, faHistory, faCog, faChartBar, faUser, faRightFromBracket, faChevronDown } from '@fortawesome/free-solid-svg-icons';
import { NavLink, useNavigate } from 'react-router-dom';
import { removeToken } from '../utils/auth';
// Import your logo image
import dashLogo from '../assets/images/logo.png';

const navItems = [
  { to: '/', icon: faHome, label: 'Dashboard', end: true },
  { to: '/upload', icon: faFileUpload, label: 'New Upload' },
  { to: '/history', icon: faHistory, label: 'Upload History' },
  { to: '/analytics', icon: faChartBar, label: 'Analytics' },
  { to: '/settings', icon: faCog, label: 'Settings' },
];

import { getUser, removeUser } from '../utils/auth';

const Sidebar = () => {
  const [showLogout, setShowLogout] = useState(false);
  const navigate = useNavigate();

  const toggleLogout = () => {
    setShowLogout(!showLogout);
  };

  const handleLogout = () => {
    removeToken();
    removeUser();
    navigate('/login');
  };

  const user = getUser();

  return (
    <div className="sidebar w-64 bg-white border-r border-gray-200 flex flex-col">
      <div className="p-4 border-b border-gray-200 flex items-center justify-start">
        <img src={dashLogo} alt="Dashboard Logo" className="h-8 w-auto" />
        <span
          className="ml-2 font-bold text-sm whitespace-nowrap"
          style={{ color: 'rgb(92, 189, 250)' }}
        >
          Marketing Data Cloud
        </span>
      </div>
      <div className="flex-1 overflow-y-auto">
        <div className="p-2">
          {navItems.map(({ to, icon, label, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                `flex items-center p-2 rounded transition-colors ${
                  isActive ? 'bg-blue-100 text-blue-700 font-semibold' : 'hover:bg-blue-50 text-gray-700'
                }`
              }
            >
              <FontAwesomeIcon icon={icon} className="mr-3" />
              {label}
            </NavLink>
          ))}
        </div>
        <div className="p-4 border-t border-gray-200 mt-auto">
          <div className="flex items-center justify-between cursor-pointer" onClick={toggleLogout}>
            <div className="flex items-center">
              <div className="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center mr-3">
                <FontAwesomeIcon icon={faUser} className="text-blue-500" />
              </div>
              <div>
                <p className="font-medium">{user?.username || 'User'}</p>
                <p className="text-xs text-gray-500">{user?.email || ''}</p>
              </div>
            </div>
            <div className={`transition-transform duration-300 ${showLogout ? 'rotate-180' : 'rotate-0'}`}>
              <FontAwesomeIcon icon={faChevronDown} className="text-gray-500" />
            </div>
          </div>
          <div
            className={`overflow-hidden rounded-md bg-gray-100 transition-all duration-700 ease-in-out ${
              showLogout ? 'max-h-20 mt-2' : 'max-h-0'
            }`}
          >
              <button
                onClick={handleLogout}
                className="w-full text-left px-4 py-2 text-sm text-red-600 hover:bg-red-100 rounded-md flex items-center"
              >
                <FontAwesomeIcon icon={faRightFromBracket} className="mr-2" />
                Logout
              </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;
