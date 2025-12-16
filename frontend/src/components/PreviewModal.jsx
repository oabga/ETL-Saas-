
import React, { useState, useEffect } from 'react';
import { X, Loader2 } from 'lucide-react';
import { getPreviewData } from '../utils/api';

export default function PreviewModal({ jobId, onClose }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchPreview = async () => {
      try {
        console.log('📊 Fetching preview for job:', jobId);
        const result = await getPreviewData(jobId);
        console.log('✅ Preview data:', result);
        setData(result);
      } catch (err) {
        console.error('❌ Preview error:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchPreview();
  }, [jobId]);

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-xl max-w-6xl w-full max-h-[90vh] overflow-hidden shadow-2xl">
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b bg-gray-50">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">Data Preview</h2>
            {data && (
              <p className="text-sm text-gray-600 mt-1">
                Showing <strong>{data.preview_rows}</strong> of <strong>{data.total_rows}</strong> rows
                {data.filename && ` • ${data.filename}`}
              </p>
            )}
          </div>
          <button 
            onClick={onClose} 
            className="text-gray-500 hover:text-gray-700 p-2 hover:bg-gray-200 rounded-lg transition"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6 overflow-auto max-h-[calc(90vh-120px)]">
          {loading ? (
            <div className="text-center py-12">
              <Loader2 className="w-16 h-16 text-blue-500 mx-auto mb-4 animate-spin" />
              <p className="text-gray-600 text-lg">Loading preview...</p>
            </div>
          ) : error ? (
            <div className="text-center py-12">
              <div className="bg-red-50 border border-red-200 rounded-lg p-6 max-w-md mx-auto">
                <p className="text-red-800 font-medium text-lg mb-2">⚠️ Error loading preview</p>
                <p className="text-red-600 text-sm">{error}</p>
                <button 
                  onClick={onClose}
                  className="mt-4 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700"
                >
                  Close
                </button>
              </div>
            </div>
          ) : data && data.data.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full border-collapse text-sm">
                <thead>
                  <tr className="bg-gradient-to-r from-blue-50 to-blue-100">
                    {data.columns.map((col, i) => (
                      <th 
                        key={i} 
                        className="border border-gray-300 px-4 py-3 text-left font-semibold text-gray-700 sticky top-0 bg-blue-100"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.data.map((row, i) => (
                    <tr key={i} className="hover:bg-blue-50 transition">
                      {row.map((cell, j) => (
                        <td key={j} className="border border-gray-300 px-4 py-2 text-gray-800">
                          {cell !== null && cell !== undefined ? cell : <span className="text-gray-400 italic">null</span>}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-12">
              <p className="text-gray-500">No data to preview</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
