import React, { useState, useEffect } from 'react';
import { Upload, Loader2, Download, Eye, CheckCircle } from 'lucide-react';
import { uploadFile, fetchJobs, getDownloadUrl } from '../utils/api';
import PreviewModal from './PreviewModal';

export default function UploadZone({ onUploadSuccess }) {
  const [file, setFile] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [message, setMessage] = useState('');
  const [isUploading, setIsUploading] = useState(false);
  
  const [currentJobId, setCurrentJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState(null);
  const [showPreview, setShowPreview] = useState(false);

  useEffect(() => {
    if (!currentJobId || jobStatus === 'COMPLETED') return;

    const pollInterval = setInterval(async () => {
      try {
        const jobs = await fetchJobs();
        const job = jobs.find(j => j.jobId === currentJobId);
        
        if (job) {
          console.log('📊 Job status:', job.status);
          setJobStatus(job.status);
          
          if (job.status === 'COMPLETED') {
            setMessage('✅ Processing complete! Your file is ready.');
            clearInterval(pollInterval);
            
            if (onUploadSuccess) {
              onUploadSuccess();
            }
          } else if (job.status === 'FAILED') {
            setMessage('❌ Processing failed. Please try again.');
            clearInterval(pollInterval);
          }
        }
      } catch (error) {
        console.error('Error polling job:', error);
      }
    }, 2000);

    return () => clearInterval(pollInterval);
  }, [currentJobId, jobStatus, onUploadSuccess]);

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = () => {
    setIsDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    
    const droppedFiles = e.dataTransfer.files;
    if (droppedFiles.length > 0) {
      const droppedFile = droppedFiles[0];
      if (droppedFile.name.endsWith('.csv')) {
        setFile(droppedFile);
        setMessage(`Selected: ${droppedFile.name}`);
        setCurrentJobId(null);
        setJobStatus(null);
      } else {
        setMessage('❌ Only CSV files are allowed');
      }
    }
  };

  const handleFileSelect = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      setFile(selectedFile);
      setMessage(`Selected: ${selectedFile.name}`);
      setCurrentJobId(null);
      setJobStatus(null);
    }
  };

  const handleUpload = async () => {
    if (!file) {
      setMessage('❌ Please select a CSV file');
      return;
    }

    setIsUploading(true);
    setMessage('Uploading...');
    setJobStatus(null);

    try {
      const result = await uploadFile(file);
      setMessage(`✅ Upload successful! Processing...`);
      setCurrentJobId(result.job_id);
      setJobStatus('QUEUED');
      setFile(null);
    } catch (error) {
      setMessage(`❌ Error: ${error.message}`);
      setCurrentJobId(null);
      setJobStatus(null);
    } finally {
      setIsUploading(false);
    }
  };

  const handleDownload = async () => {
    if (!currentJobId) return;
    
    try {
      const url = await getDownloadUrl(currentJobId);
      window.open(url, '_blank');
    } catch (error) {
      alert(`Failed to download: ${error.message}`);
    }
  };

  const handlePreview = () => {
    setShowPreview(true);
  };

  const handleNewUpload = () => {
    setFile(null);
    setCurrentJobId(null);
    setJobStatus(null);
    setMessage('');
  };

  return (
    <>
      <div className="bg-white rounded-xl shadow-sm p-8 mb-8 border border-gray-200">
        <h2 className="text-xl font-bold text-gray-900 mb-4">Upload CSV File</h2>
        
        {!currentJobId ? (
          <>
            <div
              onDragOver={handleDragOver}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
              className={`border-2 border-dashed rounded-xl p-12 text-center transition ${
                isDragging
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-300 hover:border-gray-400'
              }`}
            >
              <Upload className="w-16 h-16 text-gray-400 mx-auto mb-4" />
              <p className="text-lg font-medium text-gray-700 mb-2">
                Drag and drop your CSV file here
              </p>
              <p className="text-sm text-gray-500 mb-4">or</p>
              
              <label className="inline-block px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 cursor-pointer transition">
                <input
                  type="file"
                  accept=".csv"
                  onChange={handleFileSelect}
                  className="hidden"
                />
                Browse Files
              </label>
              
              <p className="text-xs text-gray-400 mt-4">
                Supported format: CSV (Max 100MB)
              </p>
            </div>

            {file && (
              <div className="mt-4 flex items-center justify-between bg-gray-50 p-4 rounded-lg">
                <p className="text-sm text-gray-700">
                  <strong>Selected:</strong> {file.name} ({(file.size / 1024).toFixed(2)} KB)
                </p>
                <button
                  onClick={handleUpload}
                  disabled={isUploading}
                  className={`px-6 py-2 rounded-lg font-medium transition ${
                    isUploading
                      ? 'bg-gray-300 text-gray-500 cursor-not-allowed'
                      : 'bg-blue-600 text-white hover:bg-blue-700'
                  }`}
                >
                  {isUploading ? 'Uploading...' : 'Upload & Process'}
                </button>
              </div>
            )}
          </>
        ) : (
          <>
            <div className="border-2 border-gray-200 rounded-xl p-8 text-center">
              {jobStatus === 'COMPLETED' ? (
                <>
                  <CheckCircle className="w-20 h-20 text-green-500 mx-auto mb-4" />
                  <h3 className="text-2xl font-bold text-gray-900 mb-2">
                    Processing Complete! 🎉
                  </h3>
                  <p className="text-gray-600 mb-6">
                    Your file has been successfully converted to Parquet format
                  </p>

                  <div className="flex gap-3 justify-center mb-4">
                    <button
                      onClick={handleDownload}
                      className="inline-flex items-center px-6 py-3 bg-green-600 hover:bg-green-700 text-white font-medium rounded-lg transition"
                    >
                      <Download className="w-5 h-5 mr-2" />
                      Download Parquet
                    </button>
                    
                    <button
                      onClick={handlePreview}
                      className="inline-flex items-center px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition"
                    >
                      <Eye className="w-5 h-5 mr-2" />
                      Preview Data
                    </button>
                  </div>

                  <button
                    onClick={handleNewUpload}
                    className="text-blue-600 hover:text-blue-700 font-medium text-sm"
                  >
                    Upload another file →
                  </button>
                </>
              ) : (
                <>
                  <Loader2 className="w-20 h-20 text-blue-500 mx-auto mb-4 animate-spin" />
                  <h3 className="text-2xl font-bold text-gray-900 mb-2">
                    {jobStatus === 'QUEUED' ? 'Queued...' : 'Processing...'}
                  </h3>
                  <p className="text-gray-600 mb-4">
                    {jobStatus === 'QUEUED' 
                      ? 'Your file is in the queue and will be processed shortly'
                      : 'Converting CSV to Parquet format. This usually takes 10-30 seconds.'
                    }
                  </p>

                  <div className="max-w-md mx-auto">
                    <div className="flex justify-between text-sm text-gray-600 mb-2">
                      <span>Upload ✓</span>
                      <span className={jobStatus === 'PROCESSING' ? 'text-blue-600 font-medium' : ''}>
                        Processing {jobStatus === 'PROCESSING' ? '⏳' : ''}
                      </span>
                      <span className="text-gray-400">Complete</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-blue-600 h-2 rounded-full transition-all duration-500"
                        style={{ width: jobStatus === 'PROCESSING' ? '66%' : '33%' }}
                      />
                    </div>
                  </div>

                  <button
                    onClick={handleNewUpload}
                    className="mt-6 text-gray-500 hover:text-gray-700 font-medium text-sm"
                  >
                    Cancel and upload new file
                  </button>
                </>
              )}
            </div>
          </>
        )}

        {message && !currentJobId && (
          <div className={`mt-4 p-4 rounded-lg ${
            message.includes('✅') ? 'bg-green-50 text-green-800' :
            message.includes('❌') ? 'bg-red-50 text-red-800' :
            'bg-blue-50 text-blue-800'
          }`}>
            {message}
          </div>
        )}
      </div>

    
      {showPreview && currentJobId && (
        <PreviewModal 
          jobId={currentJobId} 
          onClose={() => setShowPreview(false)} 
        />
      )}
    </>
  );
}
