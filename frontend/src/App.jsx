import { Authenticator } from '@aws-amplify/ui-react';
import { useState, useEffect } from 'react';
import Header from './components/Header';
import StatsCards from './components/StatsCards';
import UploadZone from './components/UploadZone';
import JobTable from './components/JobTable';
import { fetchJobs } from './utils/api';

export default function App() {
  const [jobs, setJobs] = useState([]);
  const [isLoading, setIsLoading] = useState(true);

  const loadJobs = async () => {
    try {
      const data = await fetchJobs();
      setJobs(data);
    } catch (error) {
      console.error('Error loading jobs:', error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadJobs();
    
    // Auto-refresh every 5 seconds
    const interval = setInterval(loadJobs, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleUploadSuccess = () => {
    // Refresh jobs list after successful upload
    loadJobs();
  };

  return (
    <Authenticator>
      {({ signOut, user }) => (
        <div className="min-h-screen bg-gray-50">
          <Header user={user} onSignOut={signOut} />
          
          <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            {isLoading ? (
              <div className="text-center py-12">
                <p className="text-gray-500">Loading...</p>
              </div>
            ) : (
              <>
                <StatsCards jobs={jobs} />
                <UploadZone onUploadSuccess={handleUploadSuccess} />
                <JobTable jobs={jobs} />
              </>
            )}
          </main>
        </div>
      )}
    </Authenticator>
  );
}