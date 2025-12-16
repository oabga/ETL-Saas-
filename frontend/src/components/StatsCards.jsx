import React from 'react';
import { Database, CheckCircle, Clock } from 'lucide-react';

export default function StatsCards({ jobs = [] }) {
  const stats = {
    total: jobs.length,
    completed: jobs.filter(j => j.status === 'COMPLETED').length,
    processing: jobs.filter(j => ['PROCESSING', 'QUEUED'].includes(j.status)).length,
  };

  const cards = [
    {
      title: 'Total Uploads',
      value: stats.total,
      icon: Database,
      bgColor: 'bg-blue-100',
      iconColor: 'text-blue-600',
      valueColor: 'text-gray-900'
    },
    {
      title: 'Ready to Download',
      value: stats.completed,
      icon: CheckCircle,
      bgColor: 'bg-green-100',
      iconColor: 'text-green-600',
      valueColor: 'text-green-600'
    },
    {
      title: 'Processing',
      value: stats.processing,
      icon: Clock,
      bgColor: 'bg-yellow-100',
      iconColor: 'text-yellow-600',
      valueColor: 'text-yellow-600'
    }
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
      {cards.map((card, index) => {
        const Icon = card.icon;
        return (
          <div key={index} className="bg-white rounded-xl shadow-sm p-6 border border-gray-200">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 font-medium">{card.title}</p>
                <p className={`text-3xl font-bold ${card.valueColor} mt-2`}>
                  {card.value}
                </p>
              </div>
              <div className={`${card.bgColor} p-3 rounded-lg`}>
                <Icon className={`w-8 h-8 ${card.iconColor}`} />
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}