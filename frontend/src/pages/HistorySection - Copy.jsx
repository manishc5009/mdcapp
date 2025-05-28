import React, { useState, useEffect } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSearch } from '@fortawesome/free-solid-svg-icons';

const HistorySection = () => {
  const [notebooks, setNotebooks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Load logged-in user info from localStorage on mount
  const [loggedInUser, setLoggedInUser] = useState({});

  useEffect(() => {
    const user = localStorage.getItem('mdc_auth_user');
    if (user) {
      try {
        setLoggedInUser(JSON.parse(user));
      } catch (e) {
        console.error('Failed to parse loggedInUser from localStorage', e);
        setLoggedInUser({});
      }
    }
  }, []);

  useEffect(() => {
    const fetchNotebooks = async () => {
      if (!loggedInUser.id) return;
      setLoading(true);
      setError(null);
      try {
        const apiurl = import.meta.env.VITE_API_URL;
        const response = await fetch(`${apiurl}/api/databricks/user-notebooks/${loggedInUser.id}`);
        if (!response.ok) {
          throw new Error('Failed to fetch notebooks');
        }
        const data = await response.json();
        setNotebooks(data.notebooks || []);
      } catch (err) {
        console.error('Error fetching notebooks:', err);
        setError('Failed to load notebooks');
      } finally {
        setLoading(false);
      }
    };
    fetchNotebooks();
  }, [loggedInUser]);

  return (
    <div className="section">
      <div className="bg-white rounded-xl shadow-lg overflow-hidden mb-8 border border-gray-200">
        <div className="p-6 border-b border-gray-200">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Upload History</h2>
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
            <div className="relative max-w-xs mb-4 sm:mb-0">
              <label htmlFor="hs-table-search" className="sr-only">Search</label>
              <div className="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                <FontAwesomeIcon icon={faSearch} className="text-gray-400" />
              </div>
              <input
                type="text"
                name="hs-table-search"
                id="hs-table-search"
                className="py-2 pl-10 pr-4 block w-full border border-gray-200 rounded-md text-sm focus:border-blue-500 focus:ring-blue-500"
                placeholder="Search..."
              />
            </div>
            <div className="flex space-x-2">
              <select className="py-2 px-3 pr-9 block border border-gray-200 rounded-md text-sm focus:border-blue-500 focus:ring-blue-500">
                <option>All Status</option>
                <option>Success</option>
                <option>Failed</option>
              </select>
              <select className="py-2 px-3 pr-9 block border border-gray-200 rounded-md text-sm focus:border-blue-500 focus:ring-blue-500">
                <option>Last 30 days</option>
                <option>Last week</option>
                <option>Last 24 hours</option>
              </select>
            </div>
          </div>
        </div>
        <div className="p-0">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">File Name</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Data Source</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rows</th>
                  <th scope="col" className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {loading ? (
                  <tr>
                    <td colSpan="6" className="px-6 py-4 whitespace-nowrap text-center text-gray-500">Loading...</td>
                  </tr>
                ) : error ? (
                  <tr>
                    <td colSpan="6" className="px-6 py-4 whitespace-nowrap text-center text-red-500">{error}</td>
                  </tr>
                ) : notebooks.length === 0 ? (
                  <tr>
                    <td colSpan="6" className="px-6 py-4 whitespace-nowrap text-center text-gray-500">No records found.</td>
                  </tr>
                ) : (
                  notebooks.map((notebook) => (
                    <tr key={notebook.id} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center">
                          <div className="flex-shrink-0 h-10 w-10 bg-blue-100 rounded-full flex items-center justify-center">
                            <i className="fas fa-file-excel text-blue-600"></i>
                          </div>
                          <div className="ml-4">
                            <div className="text-sm font-medium text-gray-900">{notebook.fileName}</div>
                            <div className="text-sm text-gray-500">{notebook.filesize}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{notebook.selectedOption}</td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                          notebook.status === 1 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                        }`}>
                          {notebook.status === 1 ? 'Success' : 'Failed'}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{new Date(notebook.createdAt).toLocaleDateString()}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{notebook.total_rows || 'N/A'}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <button className="text-blue-600 hover:text-blue-900 mr-3">View</button>
                        <button className="text-gray-600 hover:text-gray-900">Download</button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          <div className="bg-gray-50 px-6 py-3 flex items-center justify-between border-t border-gray-200">
            <div className="flex-1 flex justify-between sm:hidden">
              <button className="relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50">
                Previous
              </button>
              <button className="ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50">
                Next
              </button>
            </div>
            <div className="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
              <div>
                <p className="text-sm text-gray-700">
                  Showing <span className="font-medium">1</span> to <span className="font-medium">{notebooks.length}</span> of <span className="font-medium">{notebooks.length}</span> results
                </p>
              </div>
              <div>
                <nav className="relative z-0 inline-flex rounded-md shadow-sm -space-x-px" aria-label="Pagination">
                  <button className="relative inline-flex items-center px-2 py-2 rounded-l-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50">
                    <span className="sr-only">Previous</span>
                    <i className="fas fa-chevron-left"></i>
                  </button>
                  <button aria-current="page" className="z-10 bg-blue-50 border-blue-500 text-blue-600 relative inline-flex items-center px-4 py-2 border text-sm font-medium">
                    1
                  </button>
                  <button className="bg-white border-gray-300 text-gray-500 hover:bg-gray-50 relative inline-flex items-center px-4 py-2 border text-sm font-medium">
                    2
                  </button>
                  <button className="bg-white border-gray-300 text-gray-500 hover:bg-gray-50 relative inline-flex items-center px-4 py-2 border text-sm font-medium">
                    3
                  </button>
                  <button className="relative inline-flex items-center px-2 py-2 rounded-r-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50">
                    <span className="sr-only">Next</span>
                    <i className="fas fa-chevron-right"></i>
                  </button>
                </nav>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default HistorySection;
