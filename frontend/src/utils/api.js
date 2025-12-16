import { fetchAuthSession } from 'aws-amplify/auth';

const BACKEND_URL = 'https://d27y5ff6oq18ug.cloudfront.net'; // cloudfront

export const getAuthToken = async () => {
  const session = await fetchAuthSession();
  return session.tokens?.idToken?.toString();
};

// Upload file
export const uploadFile = async (file) => {
  const token = await getAuthToken();
  if (!token) throw new Error('No authentication token');

  const formData = new FormData();
  formData.append('file', file);

  const response = await fetch(`${BACKEND_URL}/api/upload`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`
    },
    body: formData
  });

  if (!response.ok) {
    throw new Error(`Upload failed: ${response.status}`);
  }

  return response.json();
};

// Fetch jobs
export const fetchJobs = async () => {
  const token = await getAuthToken();
  if (!token) throw new Error('No authentication token');

  const response = await fetch(`${BACKEND_URL}/api/jobs`, {
    headers: {
      'Authorization': `Bearer ${token}`
    }
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch jobs: ${response.status}`);
  }

  return response.json();
};

// Download job
export const getDownloadUrl = async (jobId) => {
  const token = await getAuthToken();
  if (!token) throw new Error('No authentication token');

  const response = await fetch(`${BACKEND_URL}/api/jobs/${jobId}/download`, {
    headers: {
      'Authorization': `Bearer ${token}`
    }
  });

  if (!response.ok) {
    throw new Error(`Failed to get download URL: ${response.status}`);
  }

  const data = await response.json();
  return data.download_url;
};


export const getPreviewData = async (jobId) => {
  const token = await getAuthToken();
  if (!token) throw new Error('No authentication token');

  const response = await fetch(`${BACKEND_URL}/api/jobs/${jobId}/preview`, {
    headers: {
      'Authorization': `Bearer ${token}`
    }
  });

  if (!response.ok) {
    throw new Error(`Failed to get preview: ${response.status}`);
  }

  return response.json();
};