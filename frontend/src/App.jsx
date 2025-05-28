import React, { useEffect, useState } from 'react';
import { BrowserRouter as Router, Routes, Route, useLocation } from 'react-router-dom';
import { FiSun, FiMoon } from 'react-icons/fi';
import Login from './pages/Login';
import Signup from './pages/Signup';
import ForgotPassword from './pages/ForgotPassword';
import DashboardSection from './pages/DashboardSection';
import UploadSection from './pages/UploadSection';
import AnalyticsSection from './pages/AnalyticsSection';
import HistorySection from './pages/HistorySection';
import SettingsSection from './pages/SettingsSection';
import DashboardLayout from './pages/DashboardLayout';
import ProtectedRoute from './components/ProtectedRoute';
import PublicRoute from './components/PublicRoute';
import './App.css';

function App() {
  const [darkMode, setDarkMode] = useState(() => {
    const stored = sessionStorage.getItem('darkMode');
    return stored !== null ? stored === 'true' : true;
  });

  useEffect(() => {
    if (darkMode) {
      document.body.classList.add('dark');
    } else {
      document.body.classList.remove('dark');
    }
    sessionStorage.setItem('darkMode', darkMode);
  }, [darkMode]);

  // Helper to show toggle only on auth pages
  const AuthToggle = () => {
    const location = useLocation();
    const isAuthPage = ['/login', '/signup', '/forgot-password'].includes(location.pathname);
    if (!isAuthPage) return null;
    return (
      <div className="fixed top-4 right-6 z-50">
        <button
          onClick={() => setDarkMode((prev) => !prev)}
          className="p-2 rounded-full bg-dark-lighter hover:bg-dark focus:outline-none transition-colors"
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
    );
  };

  return (
    <Router>
      <AuthToggle />
      <Routes>
        {/* Auth routes: no dashboard layout */}
        <Route element={<PublicRoute />}>
          <Route path="/login" element={<Login />} />
          <Route path="/signup" element={<Signup />} />
          <Route path="/forgot-password" element={<ForgotPassword />} />
        </Route>

        {/* Dashboard routes: use DashboardLayout */}
        <Route element={<ProtectedRoute />}>
          <Route
            path="/"
            element={<DashboardLayout darkMode={darkMode} setDarkMode={setDarkMode} />}
          >
            <Route index element={<DashboardSection />} />
            <Route path="dashboard" element={<DashboardSection />} />
            <Route path="upload" element={<UploadSection />} />
            <Route path="analytics" element={<AnalyticsSection />} />
            <Route path="history" element={<HistorySection />} />
            <Route path="settings" element={<SettingsSection />} />
          </Route>
        </Route>
      </Routes>
    </Router>
  );
}

export default App;