
import React from 'react';

interface CardProps {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  className?: string;
}

const Card: React.FC<CardProps> = ({ title, subtitle, children, className = '' }) => {
  return (
    <div className={`bg-gray-800 border border-gray-700 rounded-xl shadow-lg ${className}`}>
      <div className="p-4 sm:p-6 border-b border-gray-700">
        <h2 className="text-xl font-bold text-gray-100">{title}</h2>
        {subtitle && <p className="mt-1 text-sm text-gray-400">{subtitle}</p>}
      </div>
      <div className="p-4 sm:p-6">
        {children}
      </div>
    </div>
  );
};

export default Card;
