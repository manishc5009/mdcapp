import React, { useState } from 'react';

export default function ParsedPreview({ data = [], title = "Parsed Data Preview:", subtitle }) {
  // Ensure data is always an array
  const safeData = Array.isArray(data) ? data : [];
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 20;
  const totalPages = Math.ceil(safeData.length / rowsPerPage);

  const handlePrevPage = () => setCurrentPage((prev) => Math.max(prev - 1, 1));
  const handleNextPage = () => setCurrentPage((prev) => Math.min(prev + 1, totalPages));

  const startIndex = (currentPage - 1) * rowsPerPage;
  const currentRows = safeData.slice(startIndex, startIndex + rowsPerPage);

  if (!safeData || safeData.length === 0) {
    return <div className="text-gray-500 text-center py-8">No data to preview.</div>;
  }

  return (
    <div className="mt-6 bg-white p-4 rounded-md shadow">
      <h2 className="text-lg font-semibold mb-2">{title}</h2>
      {subtitle && <h4 className="mb-4 text-gray-500">{subtitle}</h4>}
      <div className="overflow-x-auto w-full border border-gray-300 rounded-md mb-4" style={{ maxWidth: '75vw' }}>
        <table className="min-w-max divide-y divide-gray-200 text-sm">
          <thead>
            <tr>
              {Object.keys(safeData[0]).map((key) => (
                <th key={key} className="px-4 py-2 text-left font-medium text-gray-600">{key}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {currentRows.map((row, i) => (
              <tr key={startIndex + i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                {Object.values(row).map((val, idx) => (
                  <td key={idx} className="px-4 py-2 whitespace-nowrap text-gray-700">{val}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex justify-between mt-4 items-center">
        <button
          onClick={handlePrevPage}
          disabled={currentPage === 1}
          className="px-3 py-1 bg-gray-300 rounded disabled:opacity-50"
        >
          Previous
        </button>
        <span>
          Page {currentPage} of {totalPages}
        </span>
        <button
          onClick={handleNextPage}
          disabled={currentPage === totalPages}
          className="px-3 py-1 bg-gray-300 rounded disabled:opacity-50"
        >
          Next
        </button>
      </div>
    </div>
  );
}
