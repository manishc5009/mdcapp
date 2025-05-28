import React, { useState } from 'react';
import { getToken } from '../utils/auth';

const SettingsSection = () => {
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);

  const validateForm = () => {
    if (!currentPassword || !newPassword || !confirmPassword) {
      setError('All fields are required.');
      return false;
    }
    if (newPassword !== confirmPassword) {
      setError('New password and confirm password do not match.');
      return false;
    }
    if (newPassword.length < 8) {
      setError('New password must be at least 8 characters long.');
      return false;
    }
    // Add more validation as needed (e.g., complexity)
    setError('');
    return true;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!validateForm()) return;

    setLoading(true);
    setError('');
    setSuccess('');

    try {
      const apiurl = import.meta.env.VITE_API_URL;

      const token = getToken();

      const response = await fetch(`${apiurl}/api/users/change-password`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          currentPassword,
          newPassword,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to change password');
      }

      setSuccess('Password changed successfully.');
      setCurrentPassword('');
      setNewPassword('');
      setConfirmPassword('');
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="section">
      <div className="bg-white rounded-xl shadow-lg overflow-hidden mb-8 border border-gray-200">
        <div className="p-6 border-b border-gray-200">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Settings</h2>
        </div>
        <form onSubmit={handleSubmit} className="p-6 space-y-6">
          <div>
            <h3 className="text-lg leading-6 font-medium text-gray-900">Change Password</h3>
            <p className="mt-1 max-w-2xl text-sm text-gray-500">Update your account password here.</p>
          </div>
          <div className="space-y-6 sm:space-y-5">
            <div className="sm:grid sm:grid-cols-3 sm:gap-4 sm:items-start">
              <label htmlFor="current-password" className="block text-sm font-medium text-gray-700 sm:mt-px sm:pt-2">
                Current Password
              </label>
              <div className="mt-1 sm:mt-0 sm:col-span-2">
                <input
                  type="password"
                  name="current-password"
                  id="current-password"
                  autoComplete="current-password"
                  value={currentPassword}
                  onChange={(e) => setCurrentPassword(e.target.value)}
                  className="max-w-lg block w-full shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm border-gray-300 rounded-md"
                  required
                />
              </div>
            </div>
            <div className="sm:grid sm:grid-cols-3 sm:gap-4 sm:items-start">
              <label htmlFor="new-password" className="block text-sm font-medium text-gray-700 sm:mt-px sm:pt-2">
                New Password
              </label>
              <div className="mt-1 sm:mt-0 sm:col-span-2">
                <input
                  type="password"
                  name="new-password"
                  id="new-password"
                  autoComplete="new-password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  className="max-w-lg block w-full shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm border-gray-300 rounded-md"
                  required
                />
              </div>
            </div>
            <div className="sm:grid sm:grid-cols-3 sm:gap-4 sm:items-start">
              <label htmlFor="confirm-password" className="block text-sm font-medium text-gray-700 sm:mt-px sm:pt-2">
                Confirm New Password
              </label>
              <div className="mt-1 sm:mt-0 sm:col-span-2">
                <input
                  type="password"
                  name="confirm-password"
                  id="confirm-password"
                  autoComplete="new-password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="max-w-lg block w-full shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm border-gray-300 rounded-md"
                  required
                />
              </div>
            </div>
          </div>
          {error && <p className="text-red-600 text-sm">{error}</p>}
          {success && <p className="text-green-600 text-sm">{success}</p>}
          <div className="pt-5">
            <div className="flex justify-end space-x-3">
              <button
                type="reset"
                onClick={() => {
                  setCurrentPassword('');
                  setNewPassword('');
                  setConfirmPassword('');
                  setError('');
                  setSuccess('');
                }}
                className="bg-white py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={loading}
                className="ml-3 inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
              >
                {loading ? 'Saving...' : 'Save'}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
};

export default SettingsSection;
