import React from 'react';
import { FiSun, FiMoon, FiX } from 'react-icons/fi';

const DashboardSettingsDrawer = ({ open, onClose, darkMode, setDarkMode }) => (
  <div
    className={`fixed top-0 right-0 h-full w-72 bg-white shadow-lg z-50 transform transition-transform duration-300 ${
      open ? 'translate-x-0' : 'translate-x-full'
    }`}
  >
    <div className="flex items-center justify-between p-4 border-b">
      <span className="font-semibold text-lg">Settings</span>
      <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
        <FiX size={24} />
      </button>
    </div>
    <div className="p-6">
      <div className="flex items-center justify-between">
        <span className="text-gray-700 font-medium">Dark Mode</span>
        <button
          onClick={() => setDarkMode((prev) => !prev)}
          className="p-2 rounded-full bg-gray-100 hover:bg-gray-200 transition-colors"
          aria-label="Toggle dark mode"
          type="button"
        >
          {darkMode ? (
            <FiSun className="text-yellow-400 w-6 h-6" />
          ) : (
            <FiMoon className="text-gray-400 w-6 h-6" />
          )}
        </button>
      </div>
    </div>
  </div>
);

export default DashboardSettingsDrawer;