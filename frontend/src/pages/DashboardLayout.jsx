import React, { useState } from 'react';
import Sidebar from './Sidebar';
import TopNav from './TopNav';
import { Outlet } from 'react-router-dom';
import { FiSettings } from 'react-icons/fi';
import DashboardSettingsDrawer from '../components/DashboardSettingsDrawer';

const DashboardLayout = ({ darkMode, setDarkMode }) => {
  const [drawerOpen, setDrawerOpen] = useState(false);

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col relative">
        <TopNav />
        {/* Settings icon fixed bottom right */}
        <button
          className="fixed bottom-6 right-6 z-40 p-2 rounded-full bg-white border shadow hover:bg-gray-100"
          onClick={() => setDrawerOpen(true)}
          aria-label="Open settings"
          type="button"
        >
          <FiSettings className="w-6 h-6 text-gray-700" />
        </button>
        <DashboardSettingsDrawer
          open={drawerOpen}
          onClose={() => setDrawerOpen(false)}
          darkMode={darkMode}
          setDarkMode={setDarkMode}
        />
        <main className="flex-1 overflow-y-auto p-6 bg-gray-50">
          <Outlet />
        </main>
      </div>
    </div>
  );
};

export default DashboardLayout;