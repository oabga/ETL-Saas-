import React, { useState } from 'react';
import { FileText, Download, Search } from 'lucide-react';
import { getDownloadUrl } from '../utils/api';

export default function JobTable({ jobs = [] }) {
  const [searchQuery, setSearchQuery] = useState('');


  const completedJobs = jobs.filter(job => job.status === 'COMPLETED');
  
  const filteredJobs = completedJobs.filter(job => {
    const matchesSearch = job.filename ? 
      job.filename.toLowerCase().includes(searchQuery.toLowerCase()) : 
      true;
    return matchesSearch;
  });


  const getParquetFilename = (csvFilename) => {
    if (!csvFilename) return 'Untitled.parquet';
    
    return csvFilename.replace(/\.csv$/i, '.parquet');
  };


  const formatUploadTime = (timestamp) => {
    if (!timestamp) return 'N/A';
    
    try {
      const date = new Date(timestamp);
      
      return date.toLocaleString('vi-VN', {
        timeZone: 'Asia/Ho_Chi_Minh',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false  
      });
    } catch (error) {
      return 'N/A';
    }
  };

  const handleDownload = async (jobId) => {
    try {
      const url = await getDownloadUrl(jobId);
      window.open(url, '_blank');
    } catch (error) {
      alert(`Failed to download: ${error.message}`);
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200">
      <div className="p-6 border-b border-gray-200">
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
          <div>
            <h2 className="text-xl font-bold text-gray-900">Completed Jobs</h2>
            <p className="text-sm text-gray-500 mt-1">
              {completedJobs.length} job(s) ready for download
            </p>
          </div>
          
          <div className="relative w-full sm:w-64">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
            <input
              type="text"
              placeholder="Search jobs..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
            />
          </div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Filename
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Upload Time
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {filteredJobs.length === 0 ? (
              <tr>
                <td colSpan="3" className="px-6 py-12 text-center text-gray-500">
                  <FileText className="w-12 h-12 text-gray-300 mx-auto mb-3" />
                  <p className="text-lg font-medium">No completed jobs yet</p>
                  <p className="text-sm mt-1">Upload a CSV file and wait for processing to complete</p>
                </td>
              </tr>
            ) : (
              filteredJobs.map(job => (
                <tr key={job.jobId} className="hover:bg-gray-50 transition">
                  <td className="px-6 py-4">
                    <div className="flex items-center">
                      <FileText className="w-5 h-5 text-gray-400 mr-3" />
                      <div>
                        <span className="font-medium text-gray-900 block">
                          {getParquetFilename(job.filename)}
                        </span>
                        {job.filename && (
                          <span className="text-xs text-gray-500">
                            Original: {job.filename}
                          </span>
                        )}
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-600">
                    {formatUploadTime(job.createdAt)}
                  </td>
                  <td className="px-6 py-4">
                    <button
                      onClick={() => handleDownload(job.jobId)}
                      className="inline-flex items-center px-4 py-2 bg-green-600 hover:bg-green-700 text-white font-medium rounded-lg transition"
                      title="Download Parquet"
                    >
                      <Download className="w-4 h-4 mr-2" />
                      Download
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}