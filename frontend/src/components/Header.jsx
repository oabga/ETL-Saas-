import React from 'react';

export default function Header({ user, onSignOut }) {
  return (
    <header className="bg-white shadow-sm border-b border-gray-200">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">ETL SaaS Dashboard</h1>
            <p className="text-sm text-gray-500 mt-1">
              Logged in as: <strong>{user?.signInDetails?.loginId}</strong>
            </p>
          </div>
          <button
            onClick={onSignOut}
            className="px-4 py-2 bg-red-500 text-white rounded-lg hover:bg-red-600 transition"
          >
            Sign Out
          </button>
        </div>
      </div>
    </header>
  );
}